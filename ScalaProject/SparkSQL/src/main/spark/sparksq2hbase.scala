//spark通过hfile向hbase导入数据
//hbase的put api是通过hbase的regionserver向hbase插入数据，
//      虽然这种方式插入速度极快，但是在插入过程中region会不断的split和compact，split和compact都会占用大量的计算资源和io，同时这一过程中也会短暂的中断服务。 
//hbase的数据文件是保存在region的hfile上，通过直接写入数据到hfile，并将hfile保存到hbase中，这种方式可以直接饶过region的split和compact，速度更快。
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

object HbaseSparkBulkImport {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "ht05")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tableName = "spark_test"
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val table = new HTable(conf, tableName)

    lazy val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table)

    //从txt写入到hbase
    val dataRdd = sc.textFile("hdfs://ht05:9000//zhaow/hotels/*")
    //dataRdd的partition数量决定了下面rdd的partition数量，最终也决定了会生成多少个hfile
    //hbase一个region下面hfile的数量上面默认为32，如果partition数量超过32，这里需要repartition一下
    val rdd = dataRdd.filter(_.length() > 0).sortBy(x => x, true).map(line => {
      val rowkey = line
      val kv: KeyValue = new KeyValue(Bytes.toBytes(rowkey), "cf".getBytes(), "name".getBytes(), rowkey.getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), kv)
    })
    //写入到hfile数据
    rdd.saveAsNewAPIHadoopFile("hdfs://ht05:9000/tmp/test3", classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], job.getConfiguration)

    //将保存在临时文件夹的hfile数据保存到hbase中
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("hdfs://ht05:9000/tmp/test3"), table)
  }
}
