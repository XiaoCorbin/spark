package func

//todo:隐式转换案例二:超人变身
class Man(val name:String)
class SuperMan(val name: String) {
  def heat=print("超人打怪兽")

}

object SuperMan{
  //隐式转换方法
  def main(args: Array[String]) {

    val hero=new Man("hero")

    implicit def man2SuperMan(man:Man)=new SuperMan(man.name)
    //Man具备了SuperMan的方法
      hero.heat
  }

}
