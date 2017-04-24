Each of the questions Q1-9 are able to be answered by running jobs Q1-Q9.

Example run:
$HADOOP_HOME/bin/hadoop jar dist/analysis.jar cs455.hadoop.Q1.Q1Job /data/census /home/census/output

In the file src.cs455.hadoop.Record.java, all of the work is done there for parsing the census data, the mapper will create a Record object for each line and then call different methods depending on what needs to be parsed.
Q7,8,9 have combiners set to speed up the process.
My Q9 grabs the top 10 states with the highest average renting tenants and the top 10 states with the highest average owning tenants.

Project Structure:
	answers/ - contains Q1-9 txt files with all the answers
	build/ - class files created by ant
	build.xml - ant build file
	dist/ - contains jar created by ant build
	full.sh - script used for running on the full shared data set
	out/ - class files created by intellij
	run.sh - script used for testing locally on a small data set
	src/ - all source code
	____cs455
	________hadoop
	____________Record.java
	____________Q1-Q9
	________________Q1-Q9Job.java, Q1-Q9Mapper.java, Q1-Q9Reducer.java, Q7-Q9Combiner.java
