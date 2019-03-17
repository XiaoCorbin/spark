package func

import java.io.File
import scala.io.Source

//todo:隐式转换案例一：让File类具备RichFile类中的read方法
object MyPredef{
  implicit def file2RichFile(file: File)=new RichFile(file)
}

class RichFile(val f:File) {
  def read()=Source.fromFile(f).mkString
}

object RichFile{
  def main(args: Array[String]) {
    val f=new File("d://aa.txt")
    //装饰，显示增强
    //val context=new RichFile(f).read()
    //println(context)


  //隐式转换
    import MyPredef.file2RichFile
    println(f.read())


  }
}