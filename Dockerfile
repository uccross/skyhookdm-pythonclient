FROM fedora:28

ARG EXTRA_PKGS=""

RUN dnf install -y gcc python3-devel ${EXTRA_PKGS} && \
    dnf clean all

COPY . /app
RUN pip3 --no-cache-dir install /app requests && \
    rm -r /app

ENTRYPOINT ["python3"]
