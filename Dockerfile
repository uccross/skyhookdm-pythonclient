FROM debian:buster-20191224-slim

ENV DEBIAN_FRONTEND=noninteractive
ARG EXTRA_PKGS=""

RUN apt update && \
    apt install -y python3-pip python3-dev ${EXTRA_PKGS} && \
    rm -rf /var/lib/apt/lists/*

COPY . /app
RUN pip3 --no-cache-dir install /app requests && \
    rm -r /app

ENTRYPOINT ["python"]
