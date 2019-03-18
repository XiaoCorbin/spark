//========Spark：DataFrame保存为parquet文件和永久表=========

object sparktable {
  def main(argSpark：DataFrame保存为parquet文件和永久表s: Array[String]): Unit = {
  
//1:============SparkSession：DataFrame保存为parquet文件
//读取源文件
val DataFrame = SparkSession.read.parquet("input_file_path.parquet")

//还可以直接在文件上运行 SQL 查询来加载 DataFrame ：
val DataFrame = SparkSession.sql("SELECT col1, col2 FROM parquet.`input_file_path.parquet`")

//将DataFrame持久化到parquet文件：
DataFrame.write.parquet("output_file_path.parquet")

//如果指定的输出文件存在默认会报错，也可以指定为其他模式，支持的模式在org.apache.SparkSession.sql.SaveMode下，分为以下模式：
//-------------------
// Append：追加模式
//要注意追加表和原表的结构和数据类型。不匹配会报错，尤其是数据类型，甚至可能造成文件破坏。
//ErrorIfExists：存在则报错，默认。
//Ignore：若存在什么都不做，也不报错。
//Overwrite：若存在则覆盖。

DataFrame.write.mode(SaveMode.Append).parquet("output_file_path.parquet")

//2:==============将DataFrame保存为永久表(sparkwarehouse or hivewarehose)
//查看SparkSession加载的永久表：
SparkSession.catalog.listTables.show
/*
+----+--------+-----------+---------+-----------+
|name|database|description|tableType|isTemporary|
+----+--------+-----------+---------+-----------+
| ymn| default|       null|  MANAGED|      false|
+----+--------+-----------+---------+-----------+
*/

//可以直接用SparkSession的table方法加载已经保存的永久表：
val DataFrame = SparkSession.table("ymn")

//保存为永久表：
DataFrame.write.saveAsTable("table_name")

//如果没有配置hive支持，此表的数据默认是存储在当前目录下的SparkSession-warehouse文件夹下.
//SparkSession-warehouse文件夹下的每个子文件夹是一张表，子文件夹名为表名，里面有parquet文件是表的数据文件。

/*同时会在当前目录生成一个metastore_db文件夹和一个derby.log日志文件，
metastore_db文件夹就是一个数据库名为metastore_db的derby（一个纯java的apache的开源数据库管理系统）数据库，
里面存放着SparkSession永久表的元数据信息。*/

//其实就跟hive一样：derby数据库存放元数据信息，SparkSession-warehouse存放数据。
//只不过这里的数据不是hive默认的文本文件，而是指定了SerDe（序列化反序列化）的parquet文件。
