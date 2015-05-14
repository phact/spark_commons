import AssemblyKeys._

name := "spark_bulk_ops"

version := "0.2.0"

libraryDependencies ++= Seq(("com.typesafe.play" %% "play-json" % "2.2.1"))

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblySettings
