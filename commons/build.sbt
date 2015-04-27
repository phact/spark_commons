name := "commons"

version := "0.1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % "3.4.1",
  "net.jpountz.lz4" % "lz4" % "1.2.0",
  "com.typesafe.play" %% "play-json" % "2.2.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "com.github.javafaker" % "javafaker" % "0.5"
)

assemblySettings
