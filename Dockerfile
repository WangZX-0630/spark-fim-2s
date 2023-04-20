FROM docker.io/bitnami/spark:3.3.2
LABEL maintainer="ZixianWang <wzx1057371832@gmail.com>"
LABEL description="Docker image with Spark (3.3.2), Hadoop (3.3.2) and Numpy package, based on bitnami/spark:3.3.2"

USER root

ENV HADOOP_HOME="/opt/hadoop"
ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
ENV HADOOP_LOG_DIR="/var/log/hadoop"
ENV PATH="$HADOOP_HOME/hadoop/sbin:$HADOOP_HOME/bin:$PATH"

WORKDIR /opt

RUN apt update && apt install -y openssh-server sudo
RUN apt-get install -y vim

RUN echo 'root:password' | chpasswd
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keygen -t rsa -b 4096 -f /root/.ssh/id_rsa -N "" && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

EXPOSE 22
COPY start-hadoop.sh /opt/start-hadoop.sh

RUN apt-get install -y curl

RUN curl -OL https://archive.apache.org/dist/hadoop/common/hadoop-3.3.2/hadoop-3.3.2.tar.gz
RUN tar -xzvf hadoop-3.3.2.tar.gz && \
  mv hadoop-3.3.2 hadoop && \
  rm -rf hadoop-3.3.2.tar.gz && \
  mkdir /var/log/hadoop

RUN mkdir -p /root/hdfs/namenode && \ 
    mkdir -p /root/hdfs/datanode 

COPY config/* /tmp/

RUN mv /tmp/ssh_config /root/.ssh/config && \
    mv /tmp/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml && \ 
    mv /tmp/core-site.xml $HADOOP_CONF_DIR/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml && \
    mv /tmp/workers $HADOOP_CONF_DIR/workers

COPY start-hadoop.sh /opt/start-hadoop.sh

RUN chmod +x /opt/start-hadoop.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh 

RUN hdfs namenode -format

RUN pip3 install numpy

ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/spark/run.sh" ]
