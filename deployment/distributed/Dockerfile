# ch-test-scope: standard
# Use Buster because Stretch JRE install fails with:
#
#   tempnam() is so ludicrously insecure as to defy implementation.tempnam: Cannot allocate memory
#   dpkg: error processing package openjdk-8-jre-headless:amd64 (--configure):
#    subprocess installed post-installation script returned error exit status 1

FROM ubuntu:18.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Prague

# Install all needed packages
RUN apt-get update && apt-get install -y tzdata curl git wget \
    && curl -sL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4 \
    && apt-get update && apt-get install -y build-essential nodejs imagemagick redis pwgen bzip2 

# Clone mailtrain repository
RUN cd /opt \
    && git clone https://github.com/Riko196/mailtrain.git \
    && cd /opt/mailtrain \
    && git checkout v3 
