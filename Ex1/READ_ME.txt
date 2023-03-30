> start-all.sh
> jps
> hdfs dfs -mkdir /input
> hdfs dfs -put ./input/* /input
> hadoop com.sun.tools.javac.Main *.java
> jar cf wc.jar *.class
> hadoop jar wc.jar WordCount /input /output
> hadoop fs -copyToLocal /output output
> hadoop fs -rm -r /output