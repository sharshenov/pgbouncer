# Images are supplied by the stand's immutable image lock.
ARG POSTGRES_IMAGE
FROM ${POSTGRES_IMAGE} AS tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf automake build-essential ca-certificates curl git libtool \
    libc-ares-dev libevent-dev libssl-dev meson ninja-build pandoc \
    python3 python3-venv pkg-config socat && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml /deps/pyproject.toml
RUN python3 -m venv /opt/test-tools && \
    /opt/test-tools/bin/pip install --no-cache-dir '/deps[dev]' 'psycopg[binary]'
ENV PATH="/opt/test-tools/bin:${PATH}"
COPY --chown=postgres:postgres . /src
RUN chmod -R a+rX /src
WORKDIR /src
USER postgres

FROM tools AS build
ARG BUILD_SYSTEM=autoconf
RUN if [ "$BUILD_SYSTEM" = autoconf ]; then \
      ./autogen.sh && ./configure --enable-werror --enable-cassert --with-cares && make -j4 && \
      cp pgbouncer /tmp/pgbouncer; \
    elif [ "$BUILD_SYSTEM" = meson ]; then \
      meson setup build --werror -Dcassert=true -Dcares=enabled && meson compile -C build -j4 && \
      cp build/pgbouncer /tmp/pgbouncer; \
    else exit 1; fi

FROM tools AS legacy
USER root
# Pin the release commit, not a moving branch or unauthenticated release URL.
RUN mkdir /legacy && cd /legacy && \
    git init && git remote add origin https://github.com/pgbouncer/pgbouncer.git && \
    git fetch --depth=1 origin de1e9cb75324a624d18fc3dd5d714d12a8da0db6 && \
    git checkout --detach FETCH_HEAD && \
    git submodule update --init --recursive && \
    ./autogen.sh && ./configure --with-cares && make -j4

FROM ${POSTGRES_IMAGE} AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    libevent-2.1-7 libc-ares2 && rm -rf /var/lib/apt/lists/*
ARG SOURCE_REVISION=unknown
ARG SOURCE_DIGEST=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      stand.build_context.sha256="${SOURCE_DIGEST}"
USER postgres
ENTRYPOINT ["/usr/local/bin/pgbouncer"]
CMD ["/etc/pgbouncer/pgbouncer.ini"]

FROM runtime-base AS runtime
COPY --from=build /tmp/pgbouncer /usr/local/bin/pgbouncer

FROM runtime-base AS legacy-runtime
LABEL org.opencontainers.image.revision="de1e9cb75324a624d18fc3dd5d714d12a8da0db6"
COPY --from=legacy /legacy/pgbouncer /usr/local/bin/pgbouncer

FROM tools AS workload
ENTRYPOINT ["python3", "/src/dev/stand/workload.py"]
