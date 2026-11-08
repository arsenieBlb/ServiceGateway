FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY . .
RUN go build -o /opt/service-gateway/gateway ./cmd/gateway && go build -o /opt/service-gateway/backend ./cmd/backend && go build -o /opt/service-gateway/demo ./cmd/demo
ENTRYPOINT ["/opt/service-gateway/demo", "-gateway-bin", "/opt/service-gateway/gateway", "-backend-bin", "/opt/service-gateway/backend"]
