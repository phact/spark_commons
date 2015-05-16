import AssemblyKeys._

name := "interactive_spark_benchmarks"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % "3.4.4",
  "org.apache.commons" % "commons-math3" % "3.4.1",
  "net.jpountz.lz4" % "lz4" % "1.2.0",
  "com.typesafe.play" %% "play-json" % "2.2.1"
)

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblySettings

mainClass in assembly := Some("pro.foundev.BenchmarkLauncher")

test in assembly := {}

traceLevel in run := 0

fork in run := true

jarName in assembly := name.value + "-assembly.jar"

mergeStrategy in assembly := {
  case PathList("META-INF", "ECLIPSEF.RSA", xs @ _*)         => MergeStrategy.discard
  case PathList("META-INF", "mailcap", xs @ _*)         => MergeStrategy.discard
  case PathList("org", "apache","commons","collections", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache","commons","logging", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last == "Driver.properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last == "plugin.properties" => MergeStrategy.discard
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}

