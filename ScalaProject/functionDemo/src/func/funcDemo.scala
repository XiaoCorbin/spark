package func

class funcDemo {

}
//柯里化(Currying):
// 指的是把原来接受多个参数的函数变换成接受一个参数的函数过程，
// 并且返回接受余下的参数且返回结果为一个新函数的技术。
object fuc {
  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3, 4, 5)

    //默认匿名函数
    println(arr.map(x => x * 3).toList)
    println(arr.map((x: Int) => x * 2).toList)
    println(arr.map(_ * 10).toList)

    //fuc有名函数
    val m1 = (x: Int, y: Int) => x + y
    println(m1(1, 2))

    //fuc函数的柯里化
    def m2(x: Int)(y: Int) = x + y
    println(m2(1)(2))
    //或者:
    //演示其柯里化(Currying)
    val m22=m2(1)_
    println("---"+m22(2))

    //or
    def first(x:Int)=(y:Int)=>x+y



  }
}