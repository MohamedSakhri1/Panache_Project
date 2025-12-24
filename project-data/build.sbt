name := "FraudDetection"
version := "1.0"
scalaVersion := "2.12.18" // Compatible avec Spark 3.5.0

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // Indispensable pour connecter Spark à Kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)