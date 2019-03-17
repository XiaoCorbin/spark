package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo:点击流日志数据的spark分析:pv总量
object PV {
  def main(args: Array[String]): Unit = {
    //todo：创建sparkconf，设置appName
    // todo:setMaster("local[2]")在本地模拟spark运行 这里的数字表示 使用2个线程
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //todo:创建SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    // todo:读取数据
    val file: RDD[String] = sc.textFile("G:\\access.log")
    println(file.count())
    //todo:将一行数据作为输入，输出("pv",1)
    val pvAndOne = file.map(x => ("pv", 1))
    // todo:聚合输出
    val totalPV: RDD[(String, Int)] = pvAndOne.reduceByKey(_ + _)
    totalPV.foreach(println)
    sc.stop()
  }
}
