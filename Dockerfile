FROM debian:buster-20191224-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y python-rados python-pip liblzma-dev && \
    rm -rf /var/lib/apt/lists/*

COPY . /app

RUN pip --no-cache-dir install /app requests && \
    rm -r /app

ENTRYPOINT ["python"]
