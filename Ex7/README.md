## Assignment 7 - De Identify HealthCare Program

De-identified data is an important tool in medical research and for providers looking to enhance patient care. While data sharing between different organizations could violate the Health Insurance Portability and Accountability Act of 1996 (HIPAA), the de-identification process makes sharing information HIPAA-compliant.

**Problem statement**
- Let’s understand the problem through a subset of temperature records as shown in the following figure:

*Input*
> PatientID, Name, DOB, Phone_Number, Email_Address, SSN, Gender, Disease, weight

> 11111	bbb1	12/10/50	1.23E+09	bbb1@xxx.com	1.11E+09	M	Diabetes	78

> 11112	bbb2	12/10/84	1.23E+09	bbb2@xxx.com	1.11E+09	F	PCOS	67

> 11113	bbb3	712/11/1940	1.23E+09	bbb3@xxx.com	1.11E+09	M	Fever	90

> 11114	bbb4	12/12/50	1.23E+09	bbb4@xxx.com	1.11E+09	F	Cold	88

> 11115	bbb5	12/13/60	1.23E+09	bbb5@xxx.com	1.11E+09	M	Blood Pressure	76

> 11116	bbb6	12/14/70	1.23E+09	bbb6@xxx.com	1.11E+09	F	Malaria	84

- Supposed that we are carrying on the reseach based on patients' weight and allowed to use this dataset without violating personal information of patients. So to protect patient privacy, these identical data have  to be encrypted.

*Sample Output*

> 11116,MBIO+/XwiNsUSLNnR9B7sw==,wtFH43b9zM4ZJrK5+O+ouQ==,msgB4nawyTSRW0xS/DdxeQ==,PaNHJZoYVjqkPLI8L4JuiA==,CLEExkCr20VeqEZKS6W+kA==,F,tWWJeDnnIQwtOCjV6YR7hA==,84

> 11115,wOwAJ4JvoEuEkO3il5CEJw==,WNPLw/OVZ6io4+njChMiYA==,msgB4nawyTSRW0xS/DdxeQ==,90zBX0GOU5IzeYDAHLtbGQ==,CLEExkCr20VeqEZKS6W+kA==,M,Yt/3yK6kQE+/oRVDEResrA==,76

> 11114,7Oxnmfjmg1kXGXrSkn3a3Q==,jVs55xyczzUX63ZKUQhSsQ==,msgB4nawyTSRW0xS/DdxeQ==,/zSLirFR5HBucWAKyw88cA==,CLEExkCr20VeqEZKS6W+kA==,F,CG2Zskv9C7canR9HMqKwyg==,88

> 11113,d3fvTuErIdCjb8nZd9Yx4Q==,BO4M0r06+nrLytWAxTv7MA==,msgB4nawyTSRW0xS/DdxeQ==,fMDg+phn8G5IHgpWcjgcVQ==,CLEExkCr20VeqEZKS6W+kA==,M,RLwXYW+QMQdJfQ4kHi09Iw==,90

> 11112,4UuhbSjTT1BaKzNtxSgaZw==,eN6bDEKlJEfBGtocOuL9DQ==,msgB4nawyTSRW0xS/DdxeQ==,Se8fr+0IrzhOj52w/a/1bQ==,CLEExkCr20VeqEZKS6W+kA==,F,CxnhbQJvMh9zUl7dHQImKw==,67

> 11111,Ddr9LoE9/50TF7xmO0bpDQ==,dJRub2pLyHqnOW0SxBGN2w==,msgB4nawyTSRW0xS/DdxeQ==,E4NpaFIAlrmrwb87vWEXqQ==,CLEExkCr20VeqEZKS6W+kA==,M,EzJYsk7Twbh3ztwN2de9fQ==,78


**Solution**
We can encrypt personal information by spliting each record into features, and just encrypt personal features and remain caring data. 

*Workflow*

Mapper: encrypt each record (line) into key-value documents.
input: <offset, line>
output: <null, encrypted_line>

It is interesting that this program don't need reduce tasks. In this case, the intermediate output would be written directly to the output directory configured by the job.

**Commandline**

> hdfs dfs -mkdir /input

> hdfs dfs -put ./input/* /input

> hadoop com.sun.tools.javac.Main *.java

> jar cf did.jar *.class

> hadoop jar did.jar DeIdentifyData /input /output

> hadoop fs -copyToLocal /output output