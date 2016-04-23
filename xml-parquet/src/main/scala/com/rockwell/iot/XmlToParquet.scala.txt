package com.rockwell.iot

import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat;
import java.util.Date;

object XmlToParquet {
  case class HistoricalTextData(tagName: String, tagValue: String, status: String, timeStamp: String)
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("""No properties file given""")
      System.exit(1)
    }
    val date = new Date();
    val sdf = new SimpleDateFormat("yyyy/MM/dd");
    val properties = new Properties()
    properties.load(new FileInputStream(args(0)))
    val ssc = StreamingContext.getOrCreate(properties.getProperty("checkpointDir"), createStream(properties));
    ssc.start()
    ssc.awaitTermination()
  }

  def createStream(properties: Properties)(): StreamingContext = {
    val topic = Set(properties.getProperty("inletKafkaTopic"))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"),
      "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))
    val conf = new SparkConf().setAppName(properties.getProperty("appName"))
      .setMaster(properties.getProperty("master"))
      .set("spark.eventLog.enabled", properties.getProperty("eventLogEnable"))
      .set("spark.eventLog.dir", properties.getProperty("eventLogDir"))
      .set("spark.ui.port", properties.getProperty("sparkUiPort"))
      val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(properties.getProperty("batchDurationInSeconds").toInt))
    val sqc = new SQLContext(sc)
    import sqc.implicits._
    val rawStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic).map(_._2)
    
    val day = val now = Calendar.getInstance()
    val current = now.get(Calendar.MINUTE)
    
    rawStream.foreachRDD { rdd =>
      rdd.flatMap(parseXmlArray).toDF().write.parquet(properties.getProperty("outDirectoryPath") + "/" + sdf.format(date) + "/" + + System.currentTimeMillis())
    }
    return ssc
  }
  
  def parseXmlArray(xString: String): List[HistoricalTextData] = {
    println(xString)
    val nodeSeq = XML.loadString(xString).child
    val lis = nodeSeq.flatMap { node =>
      if (node.mkString.trim.size != 0)
        List(new HistoricalTextData((node \\ "TagName").text, (node \\ "TagValue").text, (node \\ "Status").text, (node \\ "TimeStamp").text))
      else
        List()
    }.toList
    return lis
  }

}
