FROM python:3.10-slim-bullseye as base

#
# Prepare environment
#

# Setup env
ENV PATH=/home/admin/.local/bin:$PATH

# Install OS packages
RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install sudo git build-essential curl procps nodejs npm \
    && apt-get -y clean \
    && pip install --upgrade pip

RUN npm install -g wscat

RUN useradd -G sudo -U -m -s /bin/bash admin \
    && echo "admin ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

WORKDIR /home/admin/request-proxy

USER admin

# Install development tools
RUN pip install aiohttp \
    && pip install aiodns \
    && pip install black \
    && pip install argparse

ENTRYPOINT ["python"]
