package index

import java.util.UUID
import scala.reflect.ClassTag

class Meta[T: ClassTag, K: ClassTag, V: ClassTag](override val id: T,
                                                  val ORDER: Int,
                                                  val level: Int)(implicit val ord: Ordering[K])
  extends Lego[T, K, Lego[T, K, V]]{

  val MIN = ORDER - 1
  val MAX = ORDER*2 - 1

  var legos: Lego[T, K, Lego[T, K, V]] = new Block[T, K, Lego[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T],
    MIN, MAX)

  override def find(k: K): Option[Lego[T, K, V]] = {
    legos.find(k)
  }

  override def near(k: K): Option[Lego[T, K, V]] = {
    legos.near(k)
  }

  override def left(k: K): Option[Lego[T, K, V]] = {
    legos.left(k)
  }

  override def right(k: K): Option[Lego[T, K, V]] = {
    legos.right(k)
  }

  override def last: Option[K] = {
    legos.last
  }

  override def split(): Lego[T, K, Lego[T, K, V]] = ???
  override def canBorrowTo(p: Lego[T, K, Lego[T, K, V]]): Boolean = ???
  override def borrowLeftTo(p: Lego[T, K, Lego[T, K, V]]): Lego[T, K, Lego[T, K, V]] = ???
  override def borrowRightTo(p: Lego[T, K, Lego[T, K, V]]): Lego[T, K, Lego[T, K, V]] = ???
  override def merge(p: Lego[T, K, Lego[T, K, V]]): Lego[T, K, Lego[T, K, V]] = ???

  override def isFull(): Boolean = {
    legos.isFull()
  }

  override def isEmpty(): Boolean = {
    legos.isEmpty()
  }

  override def hasMinimum(): Boolean = {
    legos.hasMinimum()
  }

  override def insert(data: Seq[(K, Lego[T, K, V])]): (Boolean, Int) = {
    if(legos.isFull()){

      val old = legos.inOrder()
      legos = new Index[T, K, Lego[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T], ORDER, ORDER, level + 1)

      println(s"FULL META! ${old.map{case (k, p) => p.inOrder().length -> p.asInstanceOf[Block[T, K, V]].MAX}}\n")

      legos.insert(old)
    }

    val (ok, n) = legos.insert(data)

    if(!ok) return false -> 0

    if(n == data.length) return true -> n

    insert(data.slice(n, data.length))
  }

  override def remove(keys: Seq[K]): (Boolean, Int) = {
    val (ok, n) = legos.remove(keys)

    if(!ok) return false -> 0

    if(!legos.hasMinimum() && legos.isInstanceOf[Index[T, K, Lego[T, K, V]]]){
      val old = legos.inOrder()
      legos = new Block[T, K, Lego[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX)
      legos.insert(old)
    }

    true -> n
  }

  override def inOrder(): Seq[(K, Lego[T, K, V])] = {
    legos.inOrder()
  }

  override def toString: String = {

    val sb = new StringBuilder()

    sb.append(s"meta ")
    sb.append(legos.toString)
    sb.append("end of meta")
    sb.append("\n")

    sb.toString()
  }
}
