# BUILDER: golang
FROM golang:1.25.4-alpine3.21 AS builder-go

ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /usr/src/app

RUN apk add --no-cache make

COPY . /usr/src/app/
RUN make all

FROM alpine:3.22

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
    chmod 777 ${SUP_RUN_DIR}

COPY --from=builder-go \
    /usr/src/app/bin/log-courier \
    /usr/local/bin/.

USER ${USER}
WORKDIR ${HOME_DIR}/log-courier

CMD ["bash", "-c", "source ${CONF_DIR}/env && export && supervisord -c ${CONF_DIR}/${SUPERVISORD_CONF}"]
