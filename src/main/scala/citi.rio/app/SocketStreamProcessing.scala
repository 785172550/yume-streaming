package citi.rio.app

import citi.rio.util.Utils
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketStreamProcessing {

  def main(args: Array[String]): Unit = {
    val spark = Utils.getContext("Yume Validation")


    // 采集周期
    val ssc = new StreamingContext(spark, Seconds(5))

    // 保存数据状态
    ssc.sparkContext.setCheckpointDir("cp")

    val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.2.125", 9999)
    val wordDStream: DStream[String] = socketStream.flatMap(line => line.split(" "))
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 以采集周期为单位
    val windowDStream = mapDStream.window(Seconds(10),Seconds(5))

    val wordToSum: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq: Seq[Int], buf: Option[Int]) => {
        val sum = buf.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    wordToSum.print()

    //    mapDStream.reduceByKey(_+_)
    ssc.start()
    ssc.awaitTermination()

  }

}
