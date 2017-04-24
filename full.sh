JOB=

export HADOOP_CONF_DIR=/s/bach/d/under/tstubb/workspace/CS455/hadoop/client-config

for i in {1..9}
do
	#$HADOOP_HOME/bin/hdfs dfs -rm -r /home/census/output/Q${i}

	$HADOOP_HOME/bin/hadoop jar dist/analysis.jar cs455.hadoop.Q${i}.Q${i}Job /data/census /home/census/output/Q${i} &

	#$HADOOP_HOME/bin/hdfs dfs -cat /home/census/output/Q${i}/part-r-00000 > answers/Q${i}.txt
done
