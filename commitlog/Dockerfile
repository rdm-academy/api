FROM alpine:3.5

RUN apk add --update ca-certificates

COPY ./dist/linux-amd64/commitlog-svc /

ENTRYPOINT ["/commitlog-svc"]
