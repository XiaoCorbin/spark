package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TOPN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //读取数据
     val file: RDD[String] = sc.textFile("G:\\access.log")
    // 将一行数据作为输入,输出(来源URL,1)
     val refUrlAndOne: RDD[(String, Int)] = file.map(_.split(" ")).filter(_.length>10).map(x=>(x(10),1))
    // 聚合 排序-->降序
     val result: RDD[(String, Int)] = refUrlAndOne.reduceByKey(_+_).sortBy(_._2,false)
    // 通过take取topN，这里是取前5名
     val finalResult: Array[(String, Int)] = result.take(5)
     println(finalResult.toBuffer)
     sc.stop()
     }
}
