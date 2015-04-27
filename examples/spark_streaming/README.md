dse_spark_streaming_examples
============================

Put this together to show a demonstration of what Spark can do when combined with Cassandra.

### How to Run

1. checkout source
2. install [sbt](http://www.scala-sbt.org/download.html)
3. sbt assembly
4. upload jar to a DSE node running Spark
5. execute dse spark-submit --class \<main class you want to run\> dse_spark_streaming_examples-assembly-0.4.0.jar


### Demonstration

On one of the nodes in the cluster do the following
 
1. make sure rabbitmq 3.4.4 is installed on the machine you plan to execute these commands from
2. copy target/scala-2.10/dse_spark_streaming_examples-assembly-0.4.0.jar to the cluster ( you may have to run sbt assembly first)
3. java -cp target/scala-2.10/dse_spark_streaming_examples-assembly-0.4.0.jar pro.foundev.javaexamples.RabbitMQEmitWarnings
4. sudo dse spark-submit --class pro.foundev.WindowedCalculationsAndEventTriggering dse_spark_streaming_examples-assembly-0.4.0.jar


This should create and load a table on cassandra called tester.warnings load it with records exceeding 999.00 dollars for the amount

### FAQ

* <b>Can I use open source Cassandra and Spark with this?</b>Not currently tested..probably
* <b>What versions have you tested with?</b>DSE 4.6.1.
* <b>What about network latency between Cassandra and Spark?</b>Co-locate Cassandra and Spark on the same servers. 
* <b>But if I do that won't it slow down my website?</b>Separate workloads, there is OLTP and OLAP style of data. Spark should not be on the server hosting your website.
* <b>But how do I replicate data to my OLAP servers?</b>Cassandra does this by design, make another data center for OLAP servers.
* <b>That all sounds like a lot of work, is there anything easier?</b>[DSE](http://www.datastax.com) integrates Spark and Spark SQL in the easiest way possible
