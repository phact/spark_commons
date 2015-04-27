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
4. sudo dse spark-submit --class pro.foundev.javaexamples.interactive.querybasedconsumer.ResponseWriter dse_spark_streaming_examples-assembly-0.4.0.jar
5. java -cp target/scala-2.10/dse_spark_streaming_examples-assembly-0.4.0.jar pro.foundev.javaexamples.interactive.querybasedconsumer.QueryConsumer