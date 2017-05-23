#!/usr/bin/env bash

export JAVA_HOME=/usr/local/java/jdk1.8.0_91
HADOOP_DIR=/usr/local/java/hadoop-2.6.5
HBASE_DIR=/usr/local/java/hbase-1.2.4
export HADOOP_HOME=${HADOOP_DIR}
export HBASE_HOME=${HBASE_DIR}
export CLASSPATH=.:${HADOOP_DIR}/etc/hadoop:${HADOOP_DIR}/share/hadoop/common/lib/*:${HADOOP_DIR}/share/hadoop/common/*:${HADOOP_DIR}/share/hadoop/hdfs:${HADOOP_DIR}/share/hadoop/hdfs/lib/*:${HADOOP_DIR}/share/hadoop/hdfs/*:${HADOOP_DIR}/share/hadoop/yarn/lib/*:${HADOOP_DIR}/share/hadoop/yarn/*:${HADOOP_DIR}/share/hadoop/mapreduce/lib/*:${HADOOP_DIR}/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:${HBASE_DIR}/conf:${HBASE_DIR}/lib/*:${HBASE_DIR}/lib/*.jar:${JAVA_HOME}/lib/*.jar:${JAVA_HOME}/lib/javax.mail.jar
export PATH=${PATH}:${HADOOP_DIR}/bin:${HADOOP_DIR}/sbin:${HBASE_DIR}/bin:${JAVA_HOME}/bin
export HBASE_CLASSPATH=${HBASE_HOME}/lib/*.jar:${HBASE_HOME}/conf
export HADOOP_CLASSPATH=${CLASSPATH}:${HBASE_CLASSPATH}

cd /home/ailias/IdeaProjects/WordSegmentation/NewsmthScrapy

scrapy crawl NewsmthSpider
scrapy crawl YssySpider
scrapy crawl PkuSpider
/usr/bin/python3.5  NewsmthScrapy/jsonDataProcess.py
hdfs dfs -mkdir /files
hdfs dfs -put ../files/Dict.txt /files
hdfs dfs -put ../files/ch_stopword.txt /files
hdfs dfs -put ../files/mailContent.txt /files
hdfs dfs -rm /files/query.txt
hdfs dfs -put ../files/query.txt /files

##get yesterday date as input files dir
DATE=$(date --date=yesterday +"%Y-%m-%d")
hdfs dfs -mkdir /input
echo "Start put the files to hdfs"
hdfs dfs -put NewsmthScrapy/files/${DATE} /input
echo ${DATE}
echo "Finished"
