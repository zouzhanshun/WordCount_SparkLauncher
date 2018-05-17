/opt/apps/spark-2.3.0-bin-2.6.0-cdh5.7.0/bin/spark-submit \
--class SimpleApp \
--num-executors 2 \
--driver-memory 2G \
--executor-memory 2G \
--executor-cores 2 \
--master yarn \
--deploy-mode cluster \
/home/zouzhanshun/spark-wordcount-1.0-SNAPSHOT.jar \
