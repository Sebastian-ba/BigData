hdfs dfs -copyFromLocal data .;
export SPARK_MAJOR_VERSION=2;
spark-shell --driver-memory 10G --executor-memory 10G --executor-cores 4 -i Schemas.scala BatchLayer.scala BatchView1.scala BatchView2.scala BatchView3.scala; 