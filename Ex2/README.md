## Assignment 2 -WordSizeWordCount Program
Apply your MapReduce programming knowledge and write a MapReduce program to process two text files. You need to calculate the size of each word and count the number of words of that size in the text file. 

**Problem statement**
Let’s understand the problem through a sample text file content:

*Input*

> “Hello everyone this is a sample dataset. Calculate the word size and count the number of words of that size in this text file.”

Your MapReduce program should process this text file and should provide output as follows:

*Sample Output*

Word_Size Word_Count
> 1 1 (As the word of size 1 is: a)

> 2 4 (As the words of size 2 are: is, of, of, in)

> 3 3 (As the words of size 3 are: the, and, the)

> 4 6 (As the words of size 4 are: this, word, size, that, size)

**Solution**

*Workflow*
Mapper: split each record (line) into words, then find the leng of them
input: <offset, line>
output: <length, 1>

Reducer: sum up the count for each size
input: <length, (1, 1, ...)>
output: <length, count>

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf wcws.jar *.class

> hadoop jar wcws.jar WordSizeWordCount /input /output

> hadoop fs -copyToLocal /output output