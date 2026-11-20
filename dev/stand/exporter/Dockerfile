ARG GO_IMAGE
ARG EXPORTER_IMAGE
FROM ${GO_IMAGE} AS build
ARG UPSTREAM_REVISION
ENV GOTOOLCHAIN=local
ENV GOMAXPROCS=4
WORKDIR /src
RUN git init && \
    git remote add origin https://github.com/prometheus-community/pgbouncer_exporter.git && \
    git fetch --depth=1 origin "$UPSTREAM_REVISION" && \
    git checkout --detach FETCH_HEAD && \
    test "$(git rev-parse HEAD)" = "$UPSTREAM_REVISION"
COPY dev/stand/exporter/upstream.patch /tmp/upstream.patch
RUN git apply --check --unidiff-zero /tmp/upstream.patch && \
    git apply --unidiff-zero /tmp/upstream.patch
COPY dev/stand/exporter/workload*.go ./
RUN test -z "$(gofmt -l workload*.go)" && \
    go test -p 4 -race ./... && go vet ./...
RUN CGO_ENABLED=0 go build -p 4 -trimpath -ldflags \
    "-s -w -X github.com/prometheus/common/version.Version=0.9.0-workload" \
    -o /out/pgbouncer_exporter .

FROM ${EXPORTER_IMAGE}
ARG SOURCE_DIGEST=unknown
ARG UPSTREAM_REVISION
LABEL stand.workload_collector.sha256="${SOURCE_DIGEST}" \
      stand.exporter.upstream="${UPSTREAM_REVISION}"
COPY --from=build /out/pgbouncer_exporter /bin/pgbouncer_exporter
COPY dev/stand/exporter/LICENSE /LICENSE.workload
