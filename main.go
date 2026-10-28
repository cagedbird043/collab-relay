// collab-relay 是 oh-my-pi（omp）collab 协同的转发服务器，协议与官方 relay 兼容。
//
// relay 只按 4 字节信封头路由端到端加密的帧，看不到会话内容。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var version = "dev"

func main() {
	cfg := DefaultConfig()
	listen := flag.String("listen", "127.0.0.1:7466", "监听地址")
	hostCIDRs := flag.String("host-cidrs", "", "允许开房（role=host）的来源网段，逗号分隔；留空表示不限制")
	flag.BoolVar(&cfg.TrustForwardedFor, "trust-xff", cfg.TrustForwardedFor, "来自回环地址的请求按 X-Forwarded-For 最后一跳识别客户端 IP")
	flag.IntVar(&cfg.MaxRooms, "max-rooms", cfg.MaxRooms, "同时存在的房间上限")
	flag.IntVar(&cfg.MaxGuestsPerRoom, "max-guests", cfg.MaxGuestsPerRoom, "每个房间的访客上限")
	flag.IntVar(&cfg.MaxConnsPerIP, "max-conns-per-ip", cfg.MaxConnsPerIP, "每个来源 IP 的并发连接上限")
	showVersion := flag.Bool("version", false, "打印版本后退出")
	if err := applyEnv(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	prefixes, err := parsePrefixes(*hostCIDRs)
	if err != nil {
		log.Error("解析 host-cidrs 失败", "err", err)
		os.Exit(2)
	}
	cfg.HostPrefixes = prefixes

	relay := NewRelay(cfg, log)
	srv := &http.Server{Handler: relay, ReadHeaderTimeout: 10 * time.Second}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("监听失败", "addr", *listen, "err", err)
		os.Exit(1)
	}
	log.Info("collab-relay 启动", "version", version, "addr", ln.Addr().String(), "host_cidrs", *hostCIDRs)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigc:
		log.Info("收到退出信号，关闭所有房间", "signal", sig.String())
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("服务异常退出", "err", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	relay.Shutdown(ctx)
	_ = srv.Shutdown(ctx)
}

// envPrefix 加上参数名（大写、- 换成 _）就是对应的环境变量，例如 -host-cidrs 对应 COLLAB_RELAY_HOST_CIDRS。
const envPrefix = "COLLAB_RELAY_"

// applyEnv 用环境变量覆盖参数默认值，必须在 Parse 之前调用，命令行参数的优先级更高。
func applyEnv(fs *flag.FlagSet) error {
	var err error
	fs.VisitAll(func(f *flag.Flag) {
		if err != nil || f.Name == "version" {
			return
		}
		name := envName(f.Name)
		v, ok := os.LookupEnv(name)
		if !ok {
			return
		}
		if e := f.Value.Set(v); e != nil {
			err = fmt.Errorf("环境变量 %s=%q 无效: %w", name, v, e)
		}
	})
	return err
}

func envName(flagName string) string {
	return envPrefix + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

func parsePrefixes(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pfx, err := netip.ParsePrefix(part)
		if err != nil {
			return nil, err
		}
		out = append(out, pfx.Masked())
	}
	return out, nil
}
