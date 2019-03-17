import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFromMysql {
  def main(args: Array[String]): Unit = {
    //todo:1、创建sparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFromMysql")
      .master("local[2]")
      .getOrCreate()
    //todo:2、创建Properties对象，设置连接mysql的用户名和密码
    val properties: Properties =new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","abc")
    //todo:3、读取mysql中的数据
    val mysqlDF: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/spark","iplocation",properties)
    //todo:4、显示mysql中表的数据
    mysqlDF.show()
    spark.stop()
  }
}
