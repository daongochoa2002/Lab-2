## Assignment 6 - AverageSalary Program

**Problem statement**

Calculate the average salary in the department.

Consider following format:

> Department|Employee|Salary

Let’s understand the problem through a subset of temperature records as shown in the following figure:

*Input*

> Marketing	John	50000
> Marketing	Alice	55000
> Marketing	Bob	60000
> Sales	Bill	40000
> Sales	Mary	45000
> Sales	Tom	48000
> Finance	Jane	70000
> Finance	Peter	75000
> HR	Sam	60000
> HR	Lisa	65000
> HR	Tina	70000
> IT	Mark	80000
> IT	Andy	85000
> IT	Rachel	90000
> IT	Jim	95000

The target is to find the maximum salary for each department. 

*Output*

> Finance	72500.0
> HR	65000.0
> IT	87500.0
> Marketing	55000.0
> Sales	44333.332


**Solution**
We can find the highest salary in each by using this principle: 
$$
max(x,y,z) = max(x,max(y,z))
$$

*Workflow*

Mapper: split each record (line) into key-value documents.
input: <offset, line>
output: <department, salary>

Reducer: find the maximum temperature for each year.
input: <department, (salary_1, salary_2, ...)>
output: <department, max_salary>

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf mt.jar *.class

> hadoop jar mt.jar AverageSalary /input /output

> hadoop fs -copyToLocal /output output