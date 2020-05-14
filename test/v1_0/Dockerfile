FROM debian:stretch

LABEL name="kafka" version="1.0.0" java="openjdk-8-jre-headless" linux="debian:stretch" owner="mkocikowski"
#LABEL name="kafka" version="2.3.0" java="openjdk-8-jre-headless" linux="debian:stretch" owner="mkocikowski"

RUN apt-get update && \
	apt-get install -y apt-transport-https openjdk-8-jre-headless unzip wget && \
	apt-get clean && \
	rm -fr /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN mkdir -p /opt

RUN wget -q -O - https://archive.apache.org/dist/kafka/1.0.0/kafka_2.12-1.0.0.tgz | tar -zxf - -C /opt
#RUN wget -q -O - http://mirrors.advancedhosters.com/apache/kafka/2.3.0/kafka_2.12-2.3.0.tgz | tar -zxf - -C /opt

RUN mv /opt/kafka_2.12-1.0.0 /opt/kafka
#RUN mv /opt/kafka_2.12-2.3.0 /opt/kafka

ENV PATH=$PATH:/opt/kafka

WORKDIR /opt/kafka
