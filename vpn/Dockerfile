FROM golang:alpine AS builder
RUN apk add --no-cache git
ENV HOST=github.com
ENV ORG=amnezia-vpn
ENV PRJ=amneziawg-go
RUN git clone --depth 1 "https://$HOST/$ORG/$PRJ.git" /src && \
    cd /src && go build -o /amneziawg-go .

FROM alpine:3.20
RUN apk add --no-cache wireguard-tools python3 iptables iproute2 bash ca-certificates
COPY --from=builder /amneziawg-go /usr/local/bin/amneziawg-go
COPY --from=gogost/gost:latest /bin/gost /usr/local/bin/gost
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
