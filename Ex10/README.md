# Assignment 3 - WeatherData Program
You are given an adjacency list representation of an undirected graph. Your task is to write a MapReduce program that counts the number of connected components in the graph.

**Problem statement**
- Let’s understand the problem through a record in the dataset as shown in the following figure:
*Input*
> 0 9
> 1 4 9
> 2 7
> 3 5 8
> 4 1
> 5 3
> 6
> 7 2
> 8 3
> 9 0

*Sample Output*

> 4

**Solution**

To solve this problem, we convert into 3 sub tasks: `Find All Neighbours`, `Find the minimum neighbour` and `Count Distinct`.

*Workflow*
**Find All Neighbours**

Mapper: get the pair of a node with its neighbour node.
input: <offset, line>
output: <node, neigbour_node>

Reducer: find the total length of call of each user, then finding potential clients by having more than 60 mins duration.
input: <node, neigbour_node1, neigbour_node2, ...>
output: <node, all_neigbour_node>

- However, after completing this task, the nodes are duplicated with different sizes of neighbour lists, or different nodes have the same list of neighbourhood. So we conduct the second job to solve this problem by finding the mimimum node in the graph.

**Find the minimum neighbour**

Mapper: get the pair of a node with its neighbour node.
input: <offset, line>
output: <node, all_neigbour_node>

Reducer: find the minimum neighbourhood from the `all_neigbour_node`.
input: <node, all_neigbour_node>
output: <node, minimum_node>
 - After this step, we have found the list of minimum_nodes of graphs. However, this list is duplicated, then we remove this by counting the distinct.

**Count Distinct**

Mapper: get the pair of a node with its neighbour node.
input: <offset, line>
output: <none, node>

Reducer: get the distinct nodes.
input: <none, node1, node2, ...>
output: <none, node>

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf sc.jar *.class

> hadoop jar sc.jar Driver /input /ime1 /ime2 /output

> hadoop fs -copyToLocal /output output