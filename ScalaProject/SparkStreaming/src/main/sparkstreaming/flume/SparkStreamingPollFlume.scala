package flume

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

//todo:sparkStreaming整合flume----采用的是拉模式
object SparkStreamingPollFlume {

  //currentValues:他表示在当前批次每个单词出现的所有的1   (hadoop,1) (hadoop,1)(hadoop,1)
  //historyValues:他表示在之前所有批次中每个单词出现的总次数   (hadoop,100)
  def updateFunc(currentValues:Seq[Int], historyValues:Option[Int]):Option[Int] = {
    val newValue: Int = currentValues.sum+historyValues.getOrElse(0)
    Some(newValue)
  }


  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingPollFlume").setMaster("local[2]")
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建streamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    //setcheckpoint
    ssc.checkpoint("./flume")

    //4、通过FlumeUtils调用createPollingStream方法获取flume中的数据
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,"node-1",8888)

    //5、获取flume中event的body {"headers":xxxxxx,"body":xxxxx}
    val data: DStream[String] = pollingStream.map(x=>new String(x.event.getBody.array()))

    //6、切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_,1))
    //7、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)
    //8、打印输出
    result.print()
    //9、开启流式计算
    ssc.start()
    //阻塞
    ssc.awaitTermination()
  }
}
