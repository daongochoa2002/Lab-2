## Assignment 8 - Music Track Program

**Problem Statement**
XYZ.com is an online music website where users listen to various tracks, the data gets collected like shown below. Write a map reduce program to get following stats
- Number of unique listeners
- Number of times the track was shared with others
- Number of times the track was listened to on the radio
- Number of times the track was listened to in total
- Number of times the track was skipped on the radio

The data looks like as shown below.

*Input*

> UserId |TrackId|Shared|Radio|Skip
> 111115|222|0|1|0
> 111113|225|1|0|0
> 111117|223|0|1|1
> 111115|225|1|0|0
> 111115|222|1|0|0
> 111111|222|0|1|0

*Output*

TrackId UniqueListeners shared_count    radio_count skip_count 
> 111111	1 0 1 0
> 111113	1 1 0 0
> 111115	2 2 1 0
> 111117	1 0 1 1

**Solution**

*Workflow*

Mapper: split each record (line) into key-value documents.
input: <offset, line>
output: <TrackId, record>

Reducer: find the UniqueListeners, shared_count, radio_count, skip_count by splitting the record into features(UserId,Shared,Radio,Skip), and then processing them.
input: <TrackId, (record_1, record_2, ...)>
output: <year, max_temperature>


**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf sc.jar *.class

> hadoop jar sc.jar MusicTrack /input /output

> hadoop fs -copyToLocal /output output
