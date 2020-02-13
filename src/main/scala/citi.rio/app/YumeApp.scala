package citi.rio.app

import citi.rio.util.Utils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}

object YumeApp {
  def main(args: Array[String]): Unit = {
    // check args()
    init(args)
    start(args)
  }

  def init(args: Array[String]): Unit = {

  }

  def start(args: Array[String]): Unit = {
    val context = Utils.getContext("Yume Validation")

    // hdp-01:9092,hdp-02:9092,hdp-03::9092 RIO_TEST streamtopic 1
    val Array(kafkaAddress, group, topics, threadNum) = args

    // interval 60s
    val ssc = new StreamingContext(context, Durations.seconds(60))
    val topicArray = topics.split(",")

    // config kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaAddress, //start kafka in local
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest", //latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //    // read records from kafka
    //    // ssc: StreamingContext,
    //    // locationStrategy: LocationStrategy,
    //    // consumerStrategy: ConsumerStrategy[K, V]
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicArray, kafkaParams)
    )

    // get value
    val Dstrem: DStream[String] = stream.map(_.value)

    Runtime.getRuntime.addShutdownHook {
      new Thread() { () =>
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        //      ssc.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
