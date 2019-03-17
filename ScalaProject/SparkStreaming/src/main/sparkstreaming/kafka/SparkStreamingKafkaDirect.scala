package kafka

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
//todo:sparkSteaming整合kafka----采用direct(低级Api)
object SparkStreamingKafkaDirect {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafkaDirect").setMaster("local[2]")
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建streamingcontext
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./spark_direct") //它会保存topic的偏移量
    //4、准备kafka参数
    val kafkaParams=Map("metadata.broker.list"->"node-1:9092,node-2:9092,node-3:9092","group.id" ->"spark_direct")
    //5、准备topic
    val topics=Set("kafka_spark")
    //6、获取kafka中的数据
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    //7、获取topic中的数据
    val data: DStream[String] = dstream.map(_._2)
    //8、切分每一行，每个单词计为1，把相同单词出现的次数累加
    val result: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //9、打印结果数据
    result.print()

    //10、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
