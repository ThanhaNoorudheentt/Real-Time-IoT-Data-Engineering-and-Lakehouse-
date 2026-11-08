FROM cgr.dev/chainguard/minio-client:latest AS minio-client

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=minio-client /usr/bin/mc /usr/bin/mc
COPY init.sh /usr/local/bin/init-minio
RUN chmod 755 /usr/local/bin/init-minio
ENTRYPOINT ["/usr/local/bin/init-minio"]
