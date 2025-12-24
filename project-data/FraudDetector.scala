import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FraudDetector {
  def main(args: Array[String]): Unit = {
    
    // 1. Initialisation
    val spark = SparkSession.builder
      .appName("Fraud Detector")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN") // Moins de blabla dans les logs

    // 2. Lecture depuis Kafka (Réseau interne Docker -> kafka:9093)
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9093") 
      .option("subscribe", "transactions")
      .option("startingOffsets", "latest")
      .load()

    // 3. Parsing du JSON
    val schema = new StructType()
      .add("id", IntegerType)
      .add("montant", DoubleType)
      .add("ville", StringType)
      .add("type", StringType)
      .add("timestamp", LongType)

    val transactionDF = kafkaDF.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")

    // 4. Traitement : Flaguer les fraudes (Montant > 10,000)
    val processedDF = transactionDF.withColumn("is_fraud", 
      when($"montant" > 10000, true).otherwise(false)
    )

    // 5. Écriture dans HDFS avec PARTITIONNEMENT
    // Le checkpoint est obligatoire pour le Streaming
    val query = processedDF.writeStream
      .format("parquet")
      .option("path", "hdfs://namenode:8020/user/hive/warehouse/transactions")
      .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoints/transactions")
      .partitionBy("ville") // <--- AJOUTE CETTE LIGNE ICI (Solution Pro)
      .outputMode("append")
      .start()

    println(">>> Pipeline de détection de fraude démarré...")
    query.awaitTermination()
  }
}