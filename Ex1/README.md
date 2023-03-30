## Assignment 1 -Wordcount Program
Apply your MapReduce programming knowledge and write a MapReduce program to process a text file. You need to print the count of number of occurrences of each word in the text file.

**Problem statement**
Let’s understand the problem through a sample text file content:

*Input*

> “Hello everyone this is a sample dataset. Calculate the word size and count the number of words of that size in this text file.”

Your MapReduce program should process this text file and should provide output as follows:

*Sample Output*

Word    Word_Count
a       1 (As the word ‘a’ occurred only once)
this    2 (As the word ‘this’ occurred twice)

**Solution**

*Workflow*
Mapper: split each record (line) into words
input: <offset, line>
output: <word, 1>

Reducer: sum up the count for each word
input: <word, (1, 1, ...)>
output: <word, count>

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf wc.jar *.class

> hadoop jar wc.jar WordCount /input /output

> hadoop fs -copyToLocal /output output