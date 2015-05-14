name := "commons"

version := "0.1.0"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % "3.4.1",
  "net.jpountz.lz4" % "lz4" % "1.2.0",
  "com.typesafe.play" %% "play-json" % "2.2.1"
)

