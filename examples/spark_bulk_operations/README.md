Spark Bulk Ops
==============

Put this together to show a demonstration of what Spark can do when combined with Cassandra.

### How to Run

1. checkout source
2. install [sbt](http://www.scala-sbt.org/download.html)
3. sbt assembly
4. upload jar to a DSE node running Spark
5. execute dse spark-submit --class \<main class you want to run\> spark_bulk_ops-assembly-0.2.0.jar


### Demonstration

On one of the nodes in the cluster do the following
 
1. copy the example.txt file in src/main/resources/example.txt to the cluster
2. copy target/scala-2.10/spark_bulk_ops-assembly.jar to the cluster ( you may have to run sbt assembly first)s
3. sudo dse hadoop fs -put example.txt src/main/resources/example.txt 
4. sudo dse spark-submit --class BulkLoad spark_bulk_ops-assembly.jar
5. sudo dse spark-submit --class FindWithArgs spark_bulk_ops-assembly.jar 1


This should create and load a table on cassandra called spark_bulk_ops.kv load it with data, and then read all that 
data from version 1

### FAQ

* <b>Can I use open source Cassandra and Spark with this?</b>Not currently tested..probably
* <b>What versions have you tested with?</b>DSE 4.6.1.
* <b>What about network latency between Cassandra and Spark?</b>Co-locate Cassandra and Spark on the same servers. 
* <b>But if I do that won't it slow down my website?</b>Separate workloads, there is OLTP and OLAP style of data. Spark should not be on the server hosting your website.
* <b>But how do I replicate data to my OLAP servers?</b>Cassandra does this by design, make another data center for OLAP servers.
* <b>That all sounds like a lot of work, is there anything easier?</b>[DSE](http://www.datastax.com) integrates Spark and Spark SQL in the easiest way possible
