package func

class ClosureDemo {

}
/**
  * scala中的闭包
  * 闭包是一个函数，返回值依赖于声明在函数外部的一个或多个变量。
  */
object ClosureDemo{
  def main(args: Array[String]): Unit = {
    val y=10      //变量y不处于其有效作用域时,函数还能够对变量进行访问
    val add=(x:Int)=>{
      x+y
    }
    //在add中有两个变量：x和y。其中的一个x是函数的形式参数，
    // 在add方法被调用时，x被赋予一个新的值。
    // 然而，y不是形式参数，而是自由变量
    println(add(5)) // 结果15
  }
}
