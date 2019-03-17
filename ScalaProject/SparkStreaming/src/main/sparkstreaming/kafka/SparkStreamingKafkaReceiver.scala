package kafka
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.immutable
//todo:sparkStreaming整合kafka---基于receiver（高级api）
object SparkStreamingKafkaReceiver {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("SparkStreamingKafkaReceiver")
      .setMaster("local[4]") //线程数要大于receiver个数
      .set("spark.streaming.receiver.writeAheadLog.enable","true")
    //表示开启WAL预写日志，保证数据源的可靠性
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建streamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./spark_receiver")
    //4、准备zk地址
    val zkQuorum="node-1:2181,node-2:2181,node-3:2181"
    //5、准备groupId
    val groupId="spark_receiver"
    //6、定义topic   当前这个value并不是topic对应的分区数，而是针对于每一个分区使用多少个线程去消费
    val topics=Map("kafka_spark" ->2)
    //7、KafkaUtils.createStream 去接受kafka中topic数据
    //(String, String) 前面一个string表示topic名称，后面一个string表示topic中的数据
    //使用多个reveiver接受器集合去接受kafka中topic数据:3个
    val dstreamSeq: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
      val stream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
      stream
    })

    //利用streamingcontext调用union,获取得到所有receiver接受器的数据
    val totalDstream: DStream[(String, String)] = ssc.union(dstreamSeq)

    //8、获取kafka中topic的数据
    val topicData: DStream[String] = totalDstream.map(_._2)
    //9、切分每一行
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_,1))
    //10、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //11、打印结果数据
    result.print()


    //12、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
