FROM alpine:3.5

RUN apk add --update ca-certificates

COPY ./dist/linux-amd64/gateway-svc /

ENTRYPOINT ["/gateway-svc"]
