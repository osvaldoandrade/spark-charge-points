# ────────────────────────────────────────────────────────────────────
# Devcontainer: Spark 4.0 + Java 21 + Maven  on Ubuntu 24.04
# ────────────────────────────────────────────────────────────────────
FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

# ---------- Sistema ----------
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends \
      curl ca-certificates unzip gnupg lsb-release maven \
 && rm -rf /var/lib/apt/lists/*

# ---------- OpenJDK 21 ----------
ARG JDK_URL="https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz"
RUN curl -fsSL "$JDK_URL" | tar -xz -C /usr/local && \
    ln -s /usr/local/jdk-21*/bin/* /usr/local/bin/

ENV JAVA_HOME=/usr/local/jdk-21*

# ---------- Apache Spark 4.0.0 ----------
ARG SPARK_VER=4.0.0
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop3.tgz" \
    | tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VER}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# ---------- Definições finais ----------
WORKDIR /workspaces/${LOCAL_WORKDIR:-spark-charge-points}
CMD [ "bash" ]
