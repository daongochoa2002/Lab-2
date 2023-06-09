## Assignment 9 - Telecom Call Data Record Program

**Problem Statement**
We are going to solve a very useful problem called Call Data Record (CDR) Analytics. A Telecom company keeps records for its subscribers in specific format.
Consider following format:

> FromPhoneNumber|ToPhoneNumber|CallStartTime|CallEndTime|STDFlag

Now we have to write a map reduce code to find out all phone numbers who are making more than 60 mins of STD calls. Here if STD Flag is 1 that means it was as STD Call. STD is call is call which is made outside of your state or long distance calls. By identifying such subscribers, Telcom Company wants to offer them STD (Long Distance) Pack which would efficient for them instead spending more money without that package. The data is coming in log files and looks like as shown below.

*Input*

FromPhoneNumber|ToPhoneNumber|CallStartTime|CallEndTime|STDFlag
9665128505|8983006310|2015-03-01 07:08:10|2015-03-01 08:12:15|0
9665128505|8983006310|2015-03-01 09:08:10|2015-03-01 09:12:15|1
9665128505|8983006310|2015-03-01 09:08:10|2015-03-01 10:12:15|0
9665128506|8983006310|2015-03-01 09:08:10|2015-03-01 10:12:15|1
9665128507|8983006310|2015-03-01 09:08:10|2015-03-01 10:12:15|1
9665128505|8983006310|2015-03-01 09:08:10|2015-03-01 10:12:15|1

The problem is to identify subcribers who have total call duration more than 60 minutes.  

*Output*

> 9665128505	68
> 9665128506	64
> 9665128507	64

**Solution**

*Workflow*

Mapper: split each record (line) into key-value documents.
input: <offset, line>
output: <phoneNumber, duration>

Reducer: find the total length of call of each user, then finding potential clients by having more than 60 mins duration.
input: <phoneNumber, (duration_1, duration_2, ...)>
output: <phoneNumber, total_duration>

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf sc.jar *.class

> hadoop jar sc.jar CallDataRecord /input /output

> hadoop fs -copyToLocal /output output