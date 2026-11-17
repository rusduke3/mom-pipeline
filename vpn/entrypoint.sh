#!/usr/bin/env bash
set -e

CONF="/etc/amneziawg/awg0.conf"
ADDR=$(grep -E '^\s*Address\s*=' "$CONF" | cut -d'=' -f2- | tr -d ' ')
MTU=$(grep -E '^\s*MTU\s*=' "$CONF" | cut -d'=' -f2- | tr -d ' ')

amneziawg-go awg0
sleep 1

ENDPOINT_IP=$(python3 - << 'PY'
import socket, configparser, base64, re

with open("/etc/amneziawg/awg0.conf", "r", encoding="utf-8") as f:
    raw = f.read()

clean = re.sub(r'^\s*(Address|DNS|MTU|Table|PreUp|PostUp|PreDown|PostDown)\s*=.*$', '', raw, flags=re.M)
cfg = configparser.ConfigParser(strict=False)
cfg.optionxform = str
cfg.read_string(clean)

key_map = {
    "jc": "jc", "jmin": "jmin", "jmax": "jmax",
    "s1": "s1", "s2": "s2", "s3": "s3", "s4": "s4",
    "h1": "h1", "h2": "h2", "h3": "h3", "h4": "h4",
    "i1": "i1", "i2": "i2", "i3": "i3", "i4": "i4", "i5": "i5",
    "randomtrailers": "random_trailers",
    "disablecookies": "disable_cookies",
    "contentpaddingaddition": "content_padding_addition",
    "rekeyaftertime": "rekey_after_time",
    "rekeytimeout": "rekey_timeout",
    "rejectaftertime": "reject_after_time",
    "keepalivetimeout": "keepalive_timeout",
    "maxhandshakeattempts": "max_handshake_attempts",
}

lines = ["set=1"]
if "Interface" in cfg:
    for k, v in cfg["Interface"].items():
        val = v.strip()
        kl = k.lower()
        if kl == "privatekey":
            lines.append(f"private_key={base64.b64decode(val).hex()}")
        elif kl == "headerprotectionkey":
            lines.append(f"header_protection_key={base64.b64decode(val).hex()}")
        elif kl == "listenport":
            lines.append(f"listen_port={val}")
        elif kl in key_map:
            lines.append(f"{key_map[kl]}={val}")

endpoint_ip = ""
if "Peer" in cfg:
    for k, v in cfg["Peer"].items():
        val = v.strip()
        kl = k.lower()
        if kl == "publickey":
            lines.append(f"public_key={base64.b64decode(val).hex()}")
        elif kl == "presharedkey":
            lines.append(f"preshared_key={base64.b64decode(val).hex()}")
        elif kl == "endpoint":
            host, port = val.rsplit(":", 1)
            endpoint_ip = socket.gethostbyname(host)
            lines.append(f"endpoint={endpoint_ip}:{port}")
        elif kl == "allowedips":
            for ip in val.split(","):
                lines.append(f"allowed_ip={ip.strip()}")
        elif kl == "persistentkeepalive":
            lines.append(f"persistent_keepalive_interval={val}")

payload = "\n".join(lines) + "\n\n"
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect("/var/run/amneziawg/awg0.sock")
s.sendall(payload.encode())
resp = s.recv(1024).decode()
s.close()
assert "errno=0" in resp, f"UAPI error: {resp}\nSent:\n{payload}"
print(endpoint_ip)
PY
)

for ip in $(echo "$ADDR" | tr ',' ' '); do
    ip addr add "$ip" dev awg0 2>/dev/null || true
done
[ -n "$MTU" ] && ip link set mtu "$MTU" dev awg0
ip link set up dev awg0

GW=$(ip route | awk '/default/ {print $3}')
DOCKER_NET=$(ip route | awk '/eth0/ && !/default/ {print $1}')
ip route add "$ENDPOINT_IP"/32 via "$GW" dev eth0
ip route add "$DOCKER_NET" dev eth0 2>/dev/null || true
ip route replace default dev awg0

DNS_IP=$(grep -E '^\s*DNS\s*=' "$CONF" | cut -d'=' -f2- | cut -d',' -f1 | tr -d ' ')
echo "nameserver ${DNS_IP:-1.1.1.1}" > /etc/resolv.conf

exec gost -L socks5://:10808 -L http://:10809
