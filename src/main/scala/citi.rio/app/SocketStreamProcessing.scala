package citi.rio.app

import citi.rio.util.Utils
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * 所以SparkStreaming中Partition的数量公式如下：
  * Partition个数 = BatchInterval / blockInterval
  *
  * 一个 block 对应一个partition
  *
  * 默认情况下，blockInterval = 200ms，如果BatchInterval = 5s，那么Partition个数 = BatchInterval / blockInterval = 25，
  * 也就是有25个Partition，
  * 但是当一个BatchInterval中数据过少，例如只有<25个数的数据，那么是分不成25个Partition的，如下图，只有3个，有时只2个Partition。
  *
  * nc -lk 9999
  *
  */
object SocketStreamProcessing {

  def main(args: Array[String]): Unit = {
    val spark = Utils.getContext("Yume Validation")


    // 采集周期
    val ssc = new StreamingContext(spark, Seconds(3))

    // 保存数据状态
    ssc.sparkContext.setCheckpointDir("cp")

    val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("ubuntu1", 9999)

    val wordDStream: DStream[String] = socketStream.flatMap(line => line.split(" "))
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 以采集周期为单位
    val windowDStream = mapDStream.window(Seconds(6), Seconds(3))

    // transform和foreachRDD一样，是对RDD本身进行操作 每个采集周期一个RDD, foreachRDD没有返回值
    val transform1 = windowDStream.transform(rdd => {
      println("transform1: id: " + rdd.id + rdd.partitions)
      rdd
    })

    val wordToSum: DStream[(String, Int)] = transform1.updateStateByKey {
      case (seq: Seq[Int], buf: Option[Int]) =>
        val sum = buf.getOrElse(0) + seq.sum
        Option(sum)
    }
    wordToSum.print()
    //    mapDStream.reduceByKey(_+_)
    ssc.start()
    ssc.awaitTermination()

  }

}
