# ADR-004: Object storage image after MinIO removed its public images

**Status:** accepted (revisit before Week 6)

**Context:** The upstream `minio/minio` and `minio/mc` images are no longer pullable (Docker Hub and Quay both answer "unauthorized"), and the MinIO community repository is archived. The lakehouse only needs an S3-compatible endpoint.

**Decision:** Keep the S3 API as the storage contract and run the Chainguard MinIO build (`cgr.dev/chainguard/minio`, `.../minio-client`) locally.

**Alternatives:** `pgsty/silo` (community MinIO fork, runs as root), Garage, RustFS or SeaweedFS (actively maintained, different configuration), building MinIO from source.

**Consequences:**
- Spark, Iceberg and Trino only see an S3 endpoint, so swapping the store later changes `docker-compose.yml` and endpoint settings, not the pipeline.
- The free Chainguard tag is `latest` only, so builds are not pinned. Record the digest you tested with (`docker images --digests`).
- Upstream security fixes are not guaranteed. Acceptable for a local, non-production project; state this in the final architecture document.
