# collab-relay

[oh-my-pi](https://github.com/can1357/oh-my-pi)（omp）collab 协同的转发服务器，Go 实现，协议与官方 relay 兼容，客户端无需任何改动。

relay 只看每一帧前 4 字节的 peerId 来路由，帧内容由客户端端到端加密（AES-256-GCM，密钥只在分享链接 `#` 之后），relay 看不到会话内容。

## 协议

| 项目 | 约定 |
|---|---|
| 地址 | `GET /r/<roomId>?role=host\|guest` 升级为 WebSocket，`roomId` 匹配 `^[A-Za-z0-9_-]{10,64}$` |
| 帧格式 | 二进制 `[4 字节大端 peerId][密文]`；客户端发来的文本帧和不足 4 字节的帧直接丢弃 |
| host 发出 | peerId 为 0 广播给所有 guest，非 0 只发给该 guest，帧原样转发 |
| guest 发出 | peerId 改写成该 guest 的编号后转给 host |
| 控制消息（文本，仅 relay 发出） | 给 host：`{"t":"peer-joined","peer":N}`、`{"t":"peer-left","peer":N}`；给 guest：`{"t":"room-closed"}` |
| 关闭码 | 4001 房间关闭、4004 房间不存在、4009 房间已有 host、4029 房间已满 |
| 房间生命周期 | host 断开即回收房间，guest 先收 `room-closed` 再收 4001；host 重连后重建房间，guest 编号从 1 重新分配 |

与上游参考实现 `packages/collab-web/scripts/local-relay.ts` 的同一套场景对照，事件记录完全一致。

## 比参考实现多的部分

- **开房来源限制**：可以只允许指定网段（例如 tailnet 的 `100.64.0.0/10`、`fd7a:115c:a1e0::/48`）以 host 身份开房，guest 不限。外人开不了房，也就没法把 relay 当成转发隧道。默认不限制，与参考实现一致。`role` 参数必须恰好出现一次。
- **容量上限**：房间总数（满了返回 503）、每房间 guest 数（4029）、每个来源 IP 的并发连接数（429）、单帧 16 MiB。
- **背压**：每个连接排队待发的数据按实际占用内存计（缓冲区容量加每条消息的开销），超过 32 MiB 就断开该连接，不在内存里无限堆积，大量极小帧也堆不出海量条目；客户端会自动重连并重新同步。
- **心跳**：每 15 秒 ping 一次，20 秒内没有 pong 就断开，死连接最多 35 秒被清理，host 断线后能在客户端的 150 秒重连窗口内重新拿回房间。超过 32 KiB 的消息拆成分片帧发送，ping 和 pong 能插在分片之间，慢速下行收大快照时双方的心跳都不会误判；卡住的写由 30 秒写超时兜底。
- **房间重建安全**：按房间对象而不是房间 ID 判断归属，旧房间的 guest 晚断开时不会误删新房间里同编号的 guest。
- **平滑退出**：SIGTERM 时通知所有房间关闭，客户端自动重连。
- **日志**：开房、关房、进出、被拒、超限、心跳超时都会记录（房间 ID 只记前 6 位）。

## 配置

每个参数都可以用命令行或环境变量设置，环境变量名是 `COLLAB_RELAY_` 加大写的参数名（`-` 换成 `_`），两者都给时命令行优先。

| 参数 | 环境变量 | 说明 |
|---|---|---|
| `-listen` | `COLLAB_RELAY_LISTEN` | 监听地址（默认 `127.0.0.1:7466`） |
| `-host-cidrs` | `COLLAB_RELAY_HOST_CIDRS` | 允许开房的来源网段，逗号分隔；默认留空，不限制 |
| `-trust-xff` | `COLLAB_RELAY_TRUST_XFF` | 来自回环地址的请求按 X-Forwarded-For 最后一跳识别客户端 IP（默认 `true`） |
| `-max-rooms` | `COLLAB_RELAY_MAX_ROOMS` | 房间总数上限（默认 64） |
| `-max-guests` | `COLLAB_RELAY_MAX_GUESTS` | 每个房间的 guest 上限（默认 32） |
| `-max-conns-per-ip` | `COLLAB_RELAY_MAX_CONNS_PER_IP` | 每个来源 IP 的并发连接上限（默认 32） |
| `-version` | — | 打印版本 |

```sh
COLLAB_RELAY_HOST_CIDRS=100.64.0.0/10,fd7a:115c:a1e0::/48 collab-relay
```

`GET /healthz` 返回 `ok rooms=N`，只在本机用，不经 Caddy 暴露。

## 部署

```sh
deploy/deploy.sh            # 跑测试、交叉编译 linux/amd64、安装二进制与 systemd 单元并重启
deploy/deploy.sh other-host # 部署到其他 ssh 主机
```

systemd 单元见 `deploy/collab-relay.service`（`DynamicUser`、只读文件系统、无特权），参数用 `Environment=` 设置，本机配置为只允许 tailnet 开房。

### Caddy

relay 只监听回环地址，由 Caddy 提供 WSS。Caddy 也拦一道非 tailnet 来源的 host 请求：

```caddyfile
collab.example.com {
	@hostOutsideTailnet {
		path /r/*
		query role=host
		not remote_ip 100.64.0.0/10 fd7a:115c:a1e0::/48
	}
	respond @hostOutsideTailnet 403

	handle /r/* {
		reverse_proxy 127.0.0.1:7466
	}
	handle {
		redir https://my.omp.sh/ 302
	}
}
```

### DNS 分流

host 要从 tailnet 连进来，Caddy 和 relay 才能认出是自己人，所以同一个域名要分两种解析：

- 公网 DNS：A/AAAA 指向服务器公网 IP，给 guest 用；
- tailnet 内：在 headscale 的 `dns.extra_records` 里把这个域名指向服务器的 tailnet IP，MagicDNS 会下发给所有设备。

omp 侧设置 `collab.relayUrl: wss://collab.example.com`、`collab.webUrl: https://my.omp.sh`。

## 开发

```sh
go test -race ./...
```

## 许可

MIT
