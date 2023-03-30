## Assignment 5 - MaxTemp Program
Apply MapReduce programming knowledge and write a MapReduce program to process a dataset with multiple temperatures for a year. The dataset nead to be processed to find out the maximum temperature for each year in the dataset.

**Problem statement**
Let’s understand the problem through a subset of temperature records as shown in the following figure:

*Input*

> 1900 36
> 1900 29
> 1901 32
> 1901 40
> 1901 29
> 1901 48
> 1901 16
> 1901 21
> 1901 6
> 1901 22
> 1902 49
> 1902 49

In this data set, the first field represents the year and the second field represents the temperature in that year. The target is to find the maximum temperature for each year.

*Output*

> 1900 36
> 1901 48
> 1902 49

**Solution**
We can find the highest temperature in each by using this principle: 
$$
max(x,y,z) = max(x,max(y,z))
$$

*Workflow*

Mapper: split each record (line) into key-value documents.
input: <offset, line>
output: <year, temperature>

Reducer: find the maximum temperature for each year.
input: <year, (temperature_1, temperature_2, ...)>
output: <year, max_temperature>