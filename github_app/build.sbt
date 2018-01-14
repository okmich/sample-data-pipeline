import sbt.Package.ManifestAttributes

name := "github-archive-transformer"
version := "1.0"
scalaVersion := "2.10.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "runtime" excludeAll(ExclusionRule(organization = "org.apache.hadoop"))
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.2"

packageOptions in assembly := Seq(ManifestAttributes(("Main-Class", "main.App"), ("Built-By","Michael Enudi"), ("Implementation-Title", "console"), ("Implementation-Version", "1.0")))

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)
assemblyMergeStrategy in assembly := {
	case PathList("META-INF", "MANIFEST.MF") 	=> MergeStrategy.discard
	case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
	case m if m.toLowerCase.matches("meta-inf.*\\.rsa$") => MergeStrategy.discard
	case m if m.toLowerCase.matches("meta-inf.*\\.dsa$") => MergeStrategy.discard
  	case _         								=> MergeStrategy.last
}