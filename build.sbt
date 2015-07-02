
val Spark = "1.2.1"
val SparkCassandra = "1.2.1"

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
val sparkDependencies = Seq(spark_core,
  spark_sql,
  spark_streaming,
  spark_streaming_kafka,
  spark_streaming_zeromq,
  spark_connector,
  spark_connector_java)
val testDependencies = Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "com.github.javafaker" % "javafaker" % "0.5")

lazy val commonSettings = Seq(
  organization := "pro.foundev",
  scalaVersion := "2.10.5",
  version := "0.9.0",
  libraryDependencies ++= sparkDependencies,
  libraryDependencies ++= testDependencies,
  resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  parallelExecution in Test := false,
  fork in Test := true,
  testOptions in Test += Tests.Argument("-oI")

)

lazy val commons = (project in file("commons")).
  settings(commonSettings: _*)

lazy val connector_tuning_benchmark = (project in file("benchmarks/connector_tuning"))
  .dependsOn(commons % "compile->compile;test->test")
  .settings(commonSettings: _*)

lazy val low_latency_spark_benchmark = (project in file("benchmarks/low_latency_spark"))
  .dependsOn(commons % "compile->compile;test->test")
  .settings(commonSettings: _*)

lazy val spark_streaming_throughput_benchmark = (project in file("benchmarks/spark_streaming_throughput"))
  .dependsOn(commons % "compile->compile;test->test")
  .settings(commonSettings: _*)

lazy val spark_throughput_benchmark = (project in file("benchmarks/spark_throughput"))
  .dependsOn(commons % "compile->compile;test->test")
  .settings(commonSettings: _*)

lazy val ft_spark_streaming_benchmark = (project in file("benchmarks/ft_spark_streaming"))
  .dependsOn(commons % "compile->compile;test->test")
  .settings(commonSettings: _*)

lazy val iot_reference_example = (project in file("examples/iot_reference"))
  .dependsOn(commons % "compile->compile;test->test")
   .settings(commonSettings: _*)

lazy val jdbc_example = (project in file("examples/jdbc_example"))
  .dependsOn(commons % "compile->compile;test->test")
   .settings(commonSettings: _*)

lazy val spark_bulk_operations_example = (project in file("examples/spark_bulk_operations"))
  .dependsOn(commons % "compile->compile;test->test")
   .settings(commonSettings: _*)

lazy val spark_streaming_example = (project in file("examples/spark_streaming"))
    .dependsOn(commons)
   .settings(commonSettings: _*)


