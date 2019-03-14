FROM ubuntu:16.04

# ===============================================================
# apt install
# ---------------------------------------------------------------
RUN sed -i s/archive.ubuntu.com/mirrors.aliyun.com/g /etc/apt/sources.list && \
    sed -i s/security.ubuntu.com/mirrors.aliyun.com/g /etc/apt/sources.list

RUN apt-get -o Acquire::https::No-Cache=True -o Acquire::http::No-Cache=True update && \
	apt-get install -y --no-install-recommends software-properties-common && \
	apt-get -o Acquire::https::No-Cache=True -o Acquire::http::No-Cache=True update && \
	apt-get install -y --no-install-recommends \
	less vim curl tree \
	net-tools tcpdump lsof iptraf \
	python-dev python-pip \
	openjdk-8-jdk openjdk-8-jre-headless libfuse-dev && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

# ===============================================================
# pip install
# ---------------------------------------------------------------
RUN PIP_INSTALL="pip --no-cache-dir install --upgrade --index-url https://pypi.doubanio.com/simple" && \
		$PIP_INSTALL setuptools && $PIP_INSTALL six requests && $PIP_INSTALL alluxio

# ===============================================================
# java env
# ---------------------------------------------------------------
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# ===============================================================
# add alluxio package
# ---------------------------------------------------------------
ARG ALLUXIO_VERSION=2.0.0-preview-RC2
ADD alluxio-${ALLUXIO_VERSION}.tar.gz /opt
# if the tarball was remote, it needs to be untarred
# use ln -s instead of mv to avoid issues with Centos (see https://github.com/moby/moby/issues/27358)
RUN cd /opt && \
    (if ls | grep -q ".tar.gz"; then tar -xzf *.tar.gz && rm *.tar.gz; fi) && \
    ln -s alluxio-* alluxio

COPY conf /opt/alluxio/conf/
COPY entrypoint.sh /
ENV TERM=xterm
ENTRYPOINT ["/entrypoint.sh"]
