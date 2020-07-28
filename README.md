# AWS-Distributed-Systems---NLP
Maor Rocky 203246079 rocky@post.bgu.ac.il

Yoan Shimoni 317756609

Running Instructions:

3 Jar Files should be already in the S3 bucket named “maorrockyjars”.
Run main.class make sure you gave the correct Ngarm String as input to StepOne
 EMR instance will be created
 3 Steps are made with their unique jar (taken from the S3 bucket), and each
step receives different Arguments - explicit the location in S3 for input and
output.
The resulting file will be uploaded to S3//maorrockyjars unique bucket.
Discussion about our results:

When we didn't use the combiner when we executed our program on the English Ngram
we got these results:

 Reduce input groups=
 Reduce shuffle bytes=
 Reduce input records=
 Reduce output records=
but when we did use combiner we got:
 Input split bytes=
 Combine input records=
 Combine output records=
 Reduce input groups=
 Reduce shuffle bytes=
 Reduce input records=
 Reduce output records=

as we can see when we used combiner we received much fewer input records in the
"reduce" stage, of stepOne of our program. The same result was achieved when we

executed our program on Hebrew.

Bad allocations:

 1850 "he """ 120159.
 1850 "all """ 27941.
 1900 "those """ 74113.
 1730 """ Arise" 57.
 2000 who's whos NaN
 2000 leaf whorls NaN
 1690 "Israel """ 12.
 1590 5 Like 8.
 1670 Mr Samuel NaN
 1720 poflefs ' 70.
We believe that the wrong collocations were caused due to lack in the amount of
data in the corpus and since Ngram is not a perfect data set.

Good allocations:

 1930 National Survey 61690.
 1930 Foreign Trade 58638.
 1740 great commandment 106.
 1960 Spanish America 209173.
 1960 economic theory 204631.
 1730 Henry VIII 90.
 1730 great day 90.
 1970 Saudi Arabia 327622.
 1970 crude oil 379908.
 1970 Don Juan 389424.
 1890 God forgive 57316.
LOGS
 English Ngram without combiner :
Killed map tasks=
Killed reduce tasks=
Killed reduce tasks=
 Enlgish Ngram with combiner :
Killed reduce tasks=
Other local map tasks=
Hebrew Ngram without combiner:

Other local map tasks=
Hebrew Ngram with combiner:

FILE: Number of bytes read= File System Counters
FILE: Number of bytes written=
FILE: Number of read operations=
FILE: Number of large read operations=
FILE: Number of write operations=
HDFS: Number of bytes read=
HDFS: Number of bytes written=
HDFS: Number of read operations=
HDFS: Number of large read operations=
HDFS: Number of write operations=
S3: Number of bytes read=
S3: Number of bytes written=
S3: Number of read operations=
S3: Number of large read operations=
S3: Number of write operations=
Killed map tasks= Job Counters
Killed reduce tasks=
Launched map tasks=
Launched reduce tasks=
Other local map tasks=
Data-local map tasks=
(ms)= Total time spent by all maps in occupied slots
(ms)= Total time spent by all reduces in occupied slots
Total time spent by all map tasks (ms)=
Total time spent by all reduce tasks (ms)=
Total vcore-milliseconds taken by all map tasks=
Total vcore-milliseconds taken by all reduce tasks=
tasks= Total megabyte-milliseconds taken by all map
tasks=
Map input records= Map-Reduce Framework
Map output records=
Map output bytes=
Map output materialized bytes=
Input split bytes=
Combine input records=
Combine output records=
Reduce input groups=
Reduce shuffle bytes=
Reduce input records=
Reduce output records=
Spilled Records=
Shuffled Maps =
Failed Shuffles=
Merged Map outputs=
GC time elapsed (ms)=
CPU time spent (ms)=
Physical memory (bytes) snapshot=
Virtual memory (bytes) snapshot=
Total committed heap usage (bytes)=
BAD_ID= Shuffle Errors
CONNECTION=
IO_ERROR=
WRONG_LENGTH=
WRONG_MAP=
WRONG_REDUCE=
Bytes Read= File Input Format Counters
Bytes Written= File Output Format Counters
FILE: Number of bytes read= File System Counters
FILE: Number of bytes written=
FILE: Number of read operations=
FILE: Number of large read operations=
FILE: Number of write operations=
HDFS: Number of bytes read=
HDFS: Number of bytes written=
HDFS: Number of read operations=
HDFS: Number of large read operations=
HDFS: Number of write operations=
S3: Number of bytes read=
S3: Number of bytes written=
S3: Number of read operations=
S3: Number of large read operations=
S3: Number of write operations=
Killed map tasks= Job Counters
Killed reduce tasks=
Launched map tasks=
Launched reduce tasks=
Other local map tasks=
Data-local map tasks=
Total time spent by all maps in occupied slots (ms)=
Total time spent by all reduces in occupied slots (ms)=
Total time spent by all map tasks (ms)=
Total time spent by all reduce tasks (ms)=
Total vcore-milliseconds taken by all map tasks=
Total vcore-milliseconds taken by all reduce tasks=
Total megabyte-milliseconds taken by all map tasks=
Total megabyte-milliseconds taken by all reduce tasks=
Map input records= Map-Reduce Framework
Map output records=
Map output bytes=
Map output materialized bytes=
Input split bytes=
Combine input records=
Combine output records=
Reduce input groups=
Reduce shuffle bytes=
Reduce input records=
Reduce output records=
Spilled Records=
Shuffled Maps =
Failed Shuffles=
Merged Map outputs=
GC time elapsed (ms)=
CPU time spent (ms)=
Physical memory (bytes) snapshot=
Virtual memory (bytes) snapshot=
Total committed heap usage (bytes)=
BAD_ID= Shuffle Errors
CONNECTION=
IO_ERROR=
WRONG_LENGTH=
WRONG_MAP=
WRONG_REDUCE=
Bytes Read= File Input Format Counters
Bytes Written= File Output Format Counters
FILE: Number of bytes read= File System Counters
FILE: Number of bytes written=
FILE: Number of read operations=
FILE: Number of large read operations=
FILE: Number of write operations=
HDFS: Number of bytes read=
HDFS: Number of bytes written=
HDFS: Number of read operations=
HDFS: Number of large read operations=
HDFS: Number of write operations=
S3: Number of bytes read=
S3: Number of bytes written=
S3: Number of read operations=
S3: Number of large read operations=
S3: Number of write operations=
Killed map tasks= Job Counters
Killed reduce tasks=
Launched map tasks=
Launched reduce tasks=
Other local map tasks=
Data-local map tasks=
Total time spent by all maps in occupied slots (ms)=
Total time spent by all reduces in occupied slots (ms)=
Total time spent by all map tasks (ms)=
Total time spent by all reduce tasks (ms)=
Total vcore-milliseconds taken by all map tasks=
Total vcore-milliseconds taken by all reduce tasks=
Total megabyte-milliseconds taken by all map tasks=
Total megabyte-milliseconds taken by all reduce tasks=
Map input records= Map-Reduce Framework
Map output records=
Map output bytes=
Map output materialized bytes=
Input split bytes=
Combine input records=
Combine output records=
Reduce input groups=
Reduce shuffle bytes=
Reduce input records=
Reduce output records=
Spilled Records=
Shuffled Maps =
Failed Shuffles=
Merged Map outputs=
GC time elapsed (ms)=
CPU time spent (ms)=
Physical memory (bytes) snapshot=
Virtual memory (bytes) snapshot=
Total committed heap usage (bytes)=
BAD_ID= Shuffle Errors
CONNECTION=
IO_ERROR=
WRONG_LENGTH=
WRONG_MAP=
WRONG_REDUCE=
Bytes Read= File Input Format Counters
Bytes Written= File Output Format Counters
FILE: Number of bytes read= File System Counters
FILE: Number of bytes written=
FILE: Number of read operations=
FILE: Number of large read operations=
FILE: Number of write operations=
HDFS: Number of bytes read=
HDFS: Number of bytes written=
HDFS: Number of read operations=
HDFS: Number of large read operations=
HDFS: Number of write operations=
S3: Number of bytes read=
S3: Number of bytes written=
S3: Number of read operations=
S3: Number of large read operations=
S3: Number of write operations=
Failed reduce tasks= Job Counters
Killed map tasks=
Launched map tasks=
Launched reduce tasks=
Data-local map tasks=
Total time spent by all maps in occupied slots (ms)=
Total time spent by all reduces in occupied slots (ms)=
Total time spent by all map tasks (ms)=
Total time spent by all reduce tasks (ms)=
Total vcore-milliseconds taken by all map tasks=
Total vcore-milliseconds taken by all reduce tasks=
Total megabyte-milliseconds taken by all map tasks=
Total megabyte-milliseconds taken by all reduce tasks=
Map input records= Map-Reduce Framework
Map output records=
Map output bytes=
Map output materialized bytes=
Input split bytes=
Combine input records=
Combine output records=
Reduce input groups=
Reduce shuffle bytes=
Reduce input records=
Reduce output records=
Spilled Records=
Shuffled Maps =
Failed Shuffles=
Merged Map outputs=
GC time elapsed (ms)=
CPU time spent (ms)=
Physical memory (bytes) snapshot=
Virtual memory (bytes) snapshot=
Total committed heap usage (bytes)=
BAD_ID= Shuffle Errors
CONNECTION=
IO_ERROR=
WRONG_LENGTH=
WRONG_MAP=
WRONG_REDUCE=
Bytes Read= File Input Format Counters
Bytes Written= File Output Format Counters
