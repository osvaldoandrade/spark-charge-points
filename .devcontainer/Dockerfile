# Imagem base oficial do Dev Containers (Ubuntu + extras)
FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

# ---------- Java 21 e Maven ----------
RUN apt-get update -y \
 && apt-get install -y curl unzip maven

RUN curl -fsSL https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz | tar -xz -C /usr/local \
 && ln -s /usr/local/jdk-21*/bin/java /usr/local/bin/java \
 && ln -s /usr/local/jdk-21*/bin/javac /usr/local/bin/javac

ENV JAVA_HOME=/usr/local/jdk-21*

# ---------- Apache Spark 4.0.0 ----------
ARG SPARK_VER=4.0.0
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop3.tgz \
    | tar -xz -C /opt \
 && ln -s /opt/spark-${SPARK_VER}-bin-hadoop3 /opt/spark


ENV SPARK_HOME=/opt/spark

RUN apt-get update -y && apt-get install -y curl unzip maven

ENV PATH=$SPARK_HOME/bin:$PATH
