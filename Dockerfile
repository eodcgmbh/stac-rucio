FROM dvrbanec/rucio-client:latest as base

WORKDIR /bin

RUN pip install --upgrade setuptools wheel

USER root

RUN dnf install -y git epel-release && \
    git clone --depth 1 --branch v1.12.2 https://github.com/cern-fts/gfal2-python.git && \
    gfal2-python/ci/fedora-packages.sh && \
    cd gfal2-python/packaging && \
    RPMBUILD_SRC_EXTRA_FLAGS="--without docs --without python2" make srpm && \
    dnf builddep -y gfal2-python-1.12.2-1.el9.src.rpm && \
    pip install gfal2-python

RUN pip install rucio==33.6.1 rucio_clients==33.6.1 ipykernel pystac_client pydantic
