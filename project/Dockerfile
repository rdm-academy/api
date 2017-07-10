FROM alpine:3.5

RUN apk add --update ca-certificates

COPY ./dist/linux-amd64/project-svc /

ENTRYPOINT ["/project-svc"]
