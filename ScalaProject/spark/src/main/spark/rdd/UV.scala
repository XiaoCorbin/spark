package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UV {
  def main(args: Array[String]): Unit = {
    //todo:构建SparkConf和 SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
//    sc.setLogLevel("WRAN")
    // todo:读取数据
    val file: RDD[String] = sc.textFile("G:\\access.log")
    // todo:对每一行分隔，获取IP地址
    val ips: RDD[(String)] = file.map(_.split(" ")).map(x => x(0))
    // todo:对ip地址进行去重，最后输出格式 ("UV",1)
    val uvAndOne: RDD[(String, Int)] = ips.distinct().map(x => ("UV", 1))
    // todo:聚合输出
    val totalUV: RDD[(String, Int)] = uvAndOne.reduceByKey(_ + _)
    totalUV.foreach(println)
    // todo:数据结果保存
    totalUV.saveAsTextFile("G:\\access.out")
    sc.stop()

  }
}
