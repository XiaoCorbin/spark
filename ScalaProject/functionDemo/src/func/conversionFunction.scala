package func

class conversionFunction {

}

/**
  * val conversion: Nothing = null
  */
object conversionFunction  extends App{

  //隐式转换demo
  val range=1 to 10
  val range1=1.to(10)

  println("隐式:"+range+"------"+"方法:"+range1)



}