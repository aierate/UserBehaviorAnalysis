/**
 * 一般只是测试的时候使用kafka输入程序
 */

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers","172.16.135.101:9092")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    //定义一个kafka producer
    val producer = new KafkaProducer[String,String](properties)
    val bufferedSource = io.Source.fromFile("E:\\IDEAWorkSpace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }
}
