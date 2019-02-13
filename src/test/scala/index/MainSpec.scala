package index

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = 1000//Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = 4//rand.nextInt(4, 10)
    val META_ORDER = 4//rand.nextInt(4, 10)

    val DATA_MIN = DATA_ORDER - 1
    val DATA_MAX = DATA_ORDER*2 - 1

    val META_MIN = META_ORDER - 1
    val META_MAX = META_ORDER*2 - 1

    val index = new Index[String, Int, Int](UUID.randomUUID.toString, DATA_ORDER, META_ORDER)
    var data = Seq.empty[(Int, Int)]

    val n = 100//rand.nextInt(1, 1000)

    var list = Seq.empty[(Int, Int)]

    for(i<-0 until n){
      val k = rand.nextInt(0, MAX_VALUE)

      if(!list.exists(_._1 == k)){
        list = list :+ k -> k
      }
    }

    if(index.insert(list)._1){
      data = list
    }

    val dsorted = data.sortBy(_._1)
    val isorted = index.inOrder()

    println(s"order: ${DATA_ORDER}\n")
    println(s"dsorted: ${dsorted} size: ${dsorted.size}\n")
    println(s"isroted: ${isorted} size: ${isorted.size}\n")

    assert(dsorted.equals(isorted))

    println("pretty print: \n")

    println(index.toString())
  }

  "index data " should "be equal to test data" in {

    val n = 1

    for(i<-0 until n){
      test()
    }

  }

}