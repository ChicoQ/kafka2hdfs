package com.github.chico.kafka.hdfs

/**
  * Created by chico on 01/06/2017.
  */
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import scala.util.Random
import scala.collection.JavaConverters._

case class TopicPartOffset(topic: String, partitions: Int, offset: Int)

object Kafka2hdfs {
  def main(args: Array[String]): Unit = {

    val outDir = args(0)
    val topic = args(1)
    val brokers = args(2)

    val sc = new SparkContext()
    val spark = SparkSession.builder().appName("Kafka to HDFS").getOrCreate()
    import spark.implicits._

    val jdbcUsername = "appstar"
    val jdbcPassword = "SuperN0va"
    val jdbcHostname = "moveitapp.ck2itt5beb7u.eu-west-1.rds.amazonaws.com"
    val jdbcDatabase = "moveit"
    val offsetsTable = "offsets"
    val currentOffsetTable = "currentOffset"
    val jdbcPort = 3306
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    // fromOffset
    val from = spark.read.jdbc(jdbcUrl, currentOffsetTable, connectionProperties)
      from.createOrReplaceTempView("currentOffset")
    val fromOffset = spark.sql("SELECT topic, partitions, offset FROM currentOffset " +
      "WHERE ts = (SELECT MAX(ts) FROM currentOffset) LIMIT 1")
      // TODO for the empty table case
      .select($"offset").first().getAs[Int](0)

    // untilOffset
    val until = spark.read.jdbc(jdbcUrl, offsetsTable, connectionProperties)
      until.createOrReplaceTempView("offsets")

    //TODO
    val untilOffset = spark.sql("SELECT topic, partitions, offset FROM offsets " +
      "WHERE ts = (SELECT MAX(ts) FROM offsets) LIMIT 1")
      .select($"offset").first().getAs[Int](0)

    val preferredHosts = LocationStrategies.PreferConsistent
    val offsetRanges = Array(OffsetRange(topic, 0, fromOffset, untilOffset))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> s"test-consumer-${Random.nextInt}-${System.currentTimeMillis}").asJava
    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, preferredHosts)
      .map(_.value)

    rdd.saveAsTextFile(outDir, classOf[GzipCodec])

    val updateOffset = untilOffset
    println(s"###### ${updateOffset}")

    // Exception
    // spark.sql(s"""INSERT INTO currentOffset (topic, partitions, offset) VALUES ($topic, 0, $updateOffset)""")

    val updateRecord = sc.parallelize(Seq(TopicPartOffset(topic, 0, untilOffset))).toDF()
      .write.mode("append").jdbc(jdbcUrl, currentOffsetTable, connectionProperties)
  }
}
