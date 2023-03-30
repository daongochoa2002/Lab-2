# Assignment 3 - WeatherData Program
Apply your MapReduce programming knowledge and write a MapReduce program to process a dataset with temperature records. You need to find the Hot and Cold days in a year based on the maximum and minimum temperatures on those days.

**Problem statement**
- Let’s understand the problem through a record in the dataset as shown in the following figure:
*Input*
> 23907 20150101  2.423  -98.08   30.62     2.2    -0.6     0.8     0.9     6.2     1.47 C     3.7     1.1     2.5    99.9    85.4    97.2   0.369   0.308 -99.000 -99.000 -99.000     7.0     8.1 -9999.0 -9999.0 -9999.0

The second value `20150101` is the date `01-01-2015`, while the sixth value `-0.6` is the minimum temperature and the seventh value `0.8` is the maximum temperature in that date. 

The problem is to label that day with `Hot Day` or `Cold day`. If the maximum temperature > 40, that day is hot. If the minimum temperature < 10, that day is cold. Your MapReduce program should process this text file and should provide output as follows:

*Sample Output*

Cold Day 20150101	-0.6

**Solution**

*Workflow*
Mapper: extract date, maximum temperature and minimum temperature.If the maximum temperature > 40, label of that day is hot. If the minimum temperature < 10, that day is cold.
input: <offset, line>
output: <label, temperature>


It is interesting that this program don't need reduce tasks. In this case, the intermediate output would be written directly to the output directory configured by the job.

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf wcws.jar *.class

> hadoop jar wcws.jar WeatherData /input /output

> hadoop fs -copyToLocal /output output