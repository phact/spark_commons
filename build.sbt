lazy val commonSettings = Seq(
  organization := "pro.foundev"
)

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

lazy val commons = (project in file("commons")).
  settings(commonSettings: _*)

lazy val benchmarks = (project in file("benchmarks")).
  settings(commonSettings: _*)
  .dependsOn("commons")

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*)
