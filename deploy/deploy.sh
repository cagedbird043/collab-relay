#!/usr/bin/env bash
# 本地交叉编译并部署到服务器：deploy/deploy.sh [ssh 主机，默认 bj-edge]
# 服务器需要能 sudo 免密；Caddy 配置不在这里管理，见 README。
set -euo pipefail

host=${1:-bj-edge}
cd "$(dirname "$0")/.."

go test -race ./...
version=$(git describe --always --dirty)
bin=dist/collab-relay-linux-amd64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w -X main.version=${version}" -o "$bin" .

ssh_opts=(-o BatchMode=yes)
scp "${ssh_opts[@]}" "$bin" deploy/collab-relay.service "${host}:/tmp/"
ssh "${ssh_opts[@]}" "$host" 'set -e
sudo -n install -m 0755 /tmp/collab-relay-linux-amd64 /usr/local/bin/collab-relay
sudo -n install -m 0644 /tmp/collab-relay.service /etc/systemd/system/collab-relay.service
rm -f /tmp/collab-relay-linux-amd64 /tmp/collab-relay.service
sudo -n systemctl daemon-reload
sudo -n systemctl enable --quiet collab-relay
sudo -n systemctl restart collab-relay
sleep 1
systemctl is-active collab-relay
/usr/local/bin/collab-relay -version
curl -fsS http://127.0.0.1:7466/healthz'
