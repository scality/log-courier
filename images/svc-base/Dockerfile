# BUILDER: golang (production)
FROM golang:1.25.5-alpine3.21 AS builder-go

ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /usr/src/app

RUN apk add --no-cache make

COPY . /usr/src/app/
RUN make all

# BUILDER: golang (debug with symbols and race detector)
FROM golang:1.25.5-alpine3.21 AS builder-go-debug

ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /usr/src/app

RUN apk add --no-cache make gcc musl-dev

COPY . /usr/src/app/
RUN make all-debug

# Common runtime setup (shared between production and debug)
FROM alpine:3.23 AS runtime-common

RUN apk add --no-cache \
    bash \
    ca-certificates \
    curl \
    wget \
    logrotate \
    python3 \
    py3-pip \
    py3-setuptools

RUN pip3 install --no-cache-dir --break-system-packages supervisor==4.2.5

ENV USER="scality"
RUN addgroup -S ${USER} && \
    adduser -S ${USER} -G ${USER} -s /bin/bash -h /home/${USER}
ENV HOME_DIR="/home/${USER}"

ENV LOG_DIR="/logs" \
    CONF_DIR="/conf" \
    DATA_DIR="/data" \
    SUP_RUN_DIR="/var/run/supervisor"
RUN mkdir ${LOG_DIR} && \
    chown ${USER}:${USER} ${LOG_DIR} && \
    mkdir ${CONF_DIR} && \
    chown ${USER}:${USER} ${CONF_DIR} && \
    mkdir ${DATA_DIR} && \
    chown ${USER}:${USER} ${DATA_DIR} && \
    mkdir -p ${SUP_RUN_DIR} && \
    chown ${USER}:${USER} ${SUP_RUN_DIR} && \
    chmod 777 ${SUP_RUN_DIR} && \
    mkdir -p ${HOME_DIR}/log-courier/bin && \
    chown -R ${USER}:${USER} ${HOME_DIR}/log-courier

ARG BALLOT_VERSION=1.0.4
ADD --chmod=0755 https://github.com/scality/ballot/releases/download/v${BALLOT_VERSION}/ballot-v${BALLOT_VERSION}-linux-amd64 /usr/local/bin/ballot

USER ${USER}
WORKDIR ${HOME_DIR}/log-courier

CMD ["bash", "-c", "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"]

# Production runtime (extends common, adds production binaries)
FROM runtime-common AS runtime-base

COPY --from=builder-go \
    /usr/src/app/bin/log-courier \
    /usr/local/bin/.

COPY --from=builder-go \
    /usr/src/app/bin/ensureServiceUser \
    ${HOME_DIR}/log-courier/bin/

# Debug runtime (extends common, adds Go runtime and debug binaries)
FROM runtime-common AS runtime-debug

# Switch to root to install Go runtime
USER root

# Install Go runtime from official image
COPY --from=golang:1.25.4-alpine3.21 /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

# Copy debug binaries
COPY --from=builder-go-debug \
    /usr/src/app/bin/log-courier \
    /usr/local/bin/.

COPY --from=builder-go-debug \
    /usr/src/app/bin/ensureServiceUser \
    ${HOME_DIR}/log-courier/bin/

# Copy source code for stack traces
COPY --from=builder-go-debug \
    /usr/src/app \
    ${HOME_DIR}/log-courier/src

# Fix ownership after installing as root
RUN chown -R ${USER}:${USER} ${HOME_DIR}/log-courier

# Switch back to non-root user
USER ${USER}

# Default to production image
FROM runtime-base
