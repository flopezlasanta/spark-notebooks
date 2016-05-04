name := "Spark notebooks"
version := "0.1"

scalaVersion := "2.11.7"
scalaSource in Compile := baseDirectory.value / "coursera"

licenses in ThisBuild += ("MIT", url("http://opensource.org/licenses/MIT"))

libraryDependencies ++= Seq(
  "org.apache.spark"         %% "spark-core"      % "1.6.0",
  "org.apache.spark"         %% "spark-sql"       % "1.6.0"
)
