export HADOOP_CONF_DIR=/s/bach/d/under/tstubb/workspace/CS455/hadoop/conf

$HADOOP_HOME/bin/hdfs dfs -rm -r /test/analysis-out/

$HADOOP_HOME/bin/hadoop jar dist/analysis.jar cs455.hadoop.Q8.Q8Job /cs455/test /test/analysis-out

$HADOOP_HOME/bin/hdfs dfs -cat /test/analysis-out/part-r-00000 > output.txt
