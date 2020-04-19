package citi.rio.app

import citi.rio.util.Utils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object YumeApp {
  def main(args: Array[String]): Unit = {
    // check args()
    init(args)
    start(args)
  }

  def init(args: Array[String]): Unit = {

  }

  def start(args: Array[String]): Unit = {
    @transient
    val context = Utils.getSession("Yume Validation")

    // 同一个阶段执行10个job
    context.conf.set("spark.streaming.concurrentJobs", 10)
    context.conf.set("spark.streaming.kafka.maxRetries", 50)
    context.conf.set("spark.streaming.stopGracefullyOnShutdown", value = true)
    context.conf.set("spark.streaming.backpressure.enabled", value = true)
    context.conf.set("spark.streaming.backpressure.initialRate", 5000)
    context.conf.set("spark.streaming.kafka.maxRatePerPartition", 3000)

    // hdp-01:9092,hdp-02:9092,hdp-03::9092 RIO_TEST streamtopic 1
    val Array(kafkaAddress, group, topics, threadNum) = args

    // interval 10s
    val ssc = new StreamingContext(context.sparkContext, Seconds(10))
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

    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    //    // read records from kafka
    //    // ssc: StreamingContext,
    //    // locationStrategy: LocationStrategy,
    //    // consumerStrategy: ConsumerStrategy[K, V]
    kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicArray, kafkaParams)
    )

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(par => {
          val o = ranges(TaskContext.getPartitionId())
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        })

        // commit offset
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      }

    })

    // get value
    val Dstrem: DStream[String] = kafkaStream.map(_.value)




    //    Dstrem.window()
    //    Dstrem.transform()

    //    Dstrem.map(msg => {
    //      msg.
    //    })

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
