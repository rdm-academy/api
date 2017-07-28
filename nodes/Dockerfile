FROM alpine:3.6

RUN apk add --update ca-certificates

COPY ./dist/linux-amd64/nodes-svc /

ENTRYPOINT ["/nodes-svc"]
