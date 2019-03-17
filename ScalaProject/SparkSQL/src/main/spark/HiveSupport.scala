import org.apache.spark.sql.SparkSession

/**
  * todo:Sparksql操作hive的sql
  */
object HiveSupport {
  def main(args: Array[String]): Unit = {
    //todo:1、创建sparkSession
     val spark: SparkSession = SparkSession.builder()
         .appName("HiveSupport")
         .master("local[2]")
         .config("spark.sql.warehouse.dir", "G:\\spark-warehouse")
         .enableHiveSupport() //开启支持hive
         .getOrCreate()
     spark.sparkContext.setLogLevel("WARN")// 设置日志输出级别
    // todo:2、操作sql语句
    spark.sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ' '")
    spark.sql("LOAD DATA LOCAL INPATH './data/student.txt' INTO TABLE person")
    spark.sql("select * from person ").show()

    spark.stop()
  }
}
