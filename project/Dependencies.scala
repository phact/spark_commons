import sbt._

object Dependencies {
  val Spark = "1.2.1"
  val SparkCassandra = "1.2.0"

  val spark_core = "org.apache.spark" % "spark-core_2.10" % Spark % "provided"
  val spark_sql = "org.apache.spark" % "spark-sql_2.10" % Spark % "provided"
  val spark_streaming = "org.apache.spark" % "spark-streaming_2.10" % Spark % "provided"
  val spark_streaming_kafka = "org.apache.spark" % "spark-streaming-kafka_2.10" % Spark % "provided"
  val spark_streaming_zeromq = "org.apache.spark" % "spark-streaming-zeromq_2.10" % Spark % "provided"
  val spark_connector = ("com.datastax.spark" %% "spark-cassandra-connector" % SparkCassandra withSources() withJavadoc()).
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("commons-beanutils","commons-beanutils")
  val spark_connector_java = ("com.datastax.spark" %% "spark-cassandra-connector-java" % SparkCassandra withSources() withJavadoc()).
    exclude("org.apache.spark","spark-core")
  val sparkDependencies = Seq(spark_core, spark_sql, spark_streaming, spark_streaming_kafka, spark_streaming_zeromq, spark_connector, spark_connector_java)
  val testDependencies = Seq(
    "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test",
    "com.github.javafaker" % "javafaker" % "0.5"
  )
}
