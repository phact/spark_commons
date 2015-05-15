import Dependencies._

lazy val commonSettings = Seq(
  organization := "pro.foundev",
  scalaVersion := "2.10.5",
  libraryDependencies ++= sparkDependencies,
  libraryDependencies ++= testDependencies,
  resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
)

lazy val commons = (project in file("commons")).
  settings(commonSettings: _*)

lazy val connector_tuning_benchmark = (project in file("benchmarks/connector_tuning"))
  .dependsOn(commons)
  .settings(commonSettings: _*)

lazy val low_latency_spark_benchmark = (project in file("benchmarks/low_latency_spark"))
  .dependsOn(commons)
  .settings(commonSettings: _*)

lazy val spark_streaming_throughput_benchmark = (project in file("benchmarks/spark_streaming_throughput"))
  .dependsOn(commons)
  .settings(commonSettings: _*)

lazy val ft_spark_streaming_benchmark = (project in file("benchmarks/ft_spark_streaming"))
  .dependsOn(commons)
  .settings(commonSettings: _*)

lazy val iot_reference_example = (project in file("examples/iot_reference"))
    .dependsOn(commons)
   .settings(commonSettings: _*)

lazy val jdbc_example = (project in file("examples/jdbc_example"))
    .dependsOn(commons)
   .settings(commonSettings: _*)

lazy val spark_bulk_operations_example = (project in file("examples/spark_bulk_operations"))
    .dependsOn(commons)
   .settings(commonSettings: _*)

lazy val spark_streaming_example = (project in file("examples/spark_streaming"))
    .dependsOn(commons)
   .settings(commonSettings: _*)
