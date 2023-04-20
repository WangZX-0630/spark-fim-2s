# Frequent Itemsets Mining Algorithms implemented under Spark RDD platform

# General
Parallel version of Apriori, FP-Growth and ECLAT algorithm implemented under Spark RDD platform. The implementations are without Spark MLlib library and able to run on clusters in docker container.

# Installation
The following steps will make you run codes on your spark cluster's containers. This implementation is based on one master and two workers configuration. 

## Prerequisites
* Docker installed
* Docker compose  installed

## Clone to local
```sh
cd docker
git clone https://github.com/WangZX-0630/spark-fim-2s
```

## Build image
```sh
cd spark-fim-2s
docker build -t zxwang/spark-hadoop-2s:3.3.2 .
```

# Getting Started

## Start cluster
```sh
docker-compose up -d
```

## Start ssh and hadoop service in all cluster nodes
SSH login without password is required. You need to start ssh service in all nodes. Despite this, hadoop services are also required to be started in all nodes. 

```sh
./start.sh
```

## Enter in the master node
```sh
docker exec -it spark-fim-2s-spark-1 bash
```

## Run test
Run a simple test to check whether the cluster is running properly. The test code is stored in the ```share/test.py``` file. The test code will print the number of nodes in the cluster.

```sh
root@master:/opt >> spark-submit --master spark://master:7077 share/test.py
```

# Datasets
The following datasets are used in the test.
```
chess.dat
kosarak.dat
mushroom.dat
retail.dat
```
These datasets are from the [FIMI](http://fimi.ua.ac.be/data/) website. You could download them from the website and put under the share/data dir.
```sh
wget http://fimi.ua.ac.be/data/chess.dat.gz
wget http://fimi.ua.ac.be/data/kosarak.dat.gz
wget http://fimi.ua.ac.be/data/mushroom.dat.gz
wget http://fimi.ua.ac.be/data/retail.dat.gz
gunzip *.gz
mv *.dat share/data
```

# Run algorithm codes

## Put data sets into HDFS
Put data sets into HDFS. The data sets are stored in the ```share/data``` directory. Here put chess.dat into HDFS.
```sh
root@master:/opt >> hadoop fs -put share/data/chess.dat /
```

## Run fp-growth
Run single fp-growth algorithm on one data set chess.dat. The minimum support is 0.3. Time Cost will be printed in the console.

```sh
root@master:/opt >> spark-submit --master spark://master:7077 share/fp-growth.py chess.dat 0.3
```

## Run batches of test automatically
In the ```auto-test.py```, you can set the minimum support and the data sets you want to run. The performance results will be stored in the share/result.txt file.
    
```sh
root@master:/opt >> python share/auto_test.py
```


# Extend the cluster nodes number

You can extend the cluster nodes number by following steps:

1. Create new directory for the new image. For example, ```spark-fim-4s```. Copy all files in ```spark-fim-2s``` to ```spark-fim-4s```.
```sh
cd docker
mkdir spark-fim-4s
cp -r spark-fim-2s/* spark-fim-4s
```
2. In ```config/workers```, add more workders. 
```
worker1
worker2
worker3
worker4
```
3. Build a new image with a new tag, such as ```zxwang/spark-hadoop-4s:3.3.2``` if two more workers added.
```sh
docker build -t zxwang/spark-hadoop-4s:3.3.2 .
```
4. Modify ```docker-compose.yml``` file. Update image name and volumes. Add more spark-workers.

For example, you can add two more worker nodes by adding the following lines in the docker-compose.yml file and update the image and volumes.

```sh
  spark-worker-3:
    image: zxwang/spark-hadoop-4s:3.3.2
    hostname: worker3
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - ~/docker/spark-fim-4s/share:/opt/share
    ports:
      - '8083:8081'
  spark-worker-4:
    image: zxwang/spark-hadoop-4s:3.3.2
    hostname: worker4
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - ~/docker/spark-fim-4s/share:/opt/share
    ports:
      - '8084:8081'
```
5. Modify start.sh file as following:
```sh
docker exec -it spark-fim-4s-spark-1 sh start-hadoop.sh
docker exec -it spark-fim-4s-spark-worker-1-1 sh start-hadoop.sh
docker exec -it spark-fim-4s-spark-worker-2-1 sh start-hadoop.sh
docker exec -it spark-fim-4s-spark-worker-3-1 sh start-hadoop.sh
docker exec -it spark-fim-4s-spark-worker-4-1 sh start-hadoop.sh
```
6. Run ```docker-compose up -d``` to start the cluster.

# Directory Structure
```sh
spark-fim-2s
├── Dockerfile
├── config
│   ├── core-site.xml
│   ├── hadoop-env.sh
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   ├── ssh_config
│   ├── workers
│   └── yarn-site.xml
├── docker-compose.yml
├── share
│   ├── apriori.py
│   ├── auto_test.py
│   ├── data
│   │   ├── chess.dat
│   │   ├── kosarak.dat
│   │   ├── mushroom.dat
│   │   └── retail.dat
│   ├── fp-growth.py
│   ├── requirements.txt
│   ├── result.txt
│   └── test.py
├── start-hadoop.sh
└── start.sh
```
