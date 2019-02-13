package index

import java.util.UUID
import scala.reflect.ClassTag

class Index[T: ClassTag, K: ClassTag, V: ClassTag](override val id: T,
                                                   val DATA_ORDER: Int,
                                                   val META_ORDER: Int,
                                                   val level: Int = 0)
                                                  (implicit val ord: Ordering[K]) extends Lego[T, K, V]{

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1

  val meta: Lego[T, K, Lego[T, K, V]] = new Meta[T, K, V](UUID.randomUUID.toString.asInstanceOf[T],
    META_ORDER, level)

  var size = 0

  override def find(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.find(k)
    }
  }

  override def near(k: K): Option[V] = {
    meta.near(k) match {
      case None => None
      case Some(p) => p.near(k)
    }
  }

  override def left(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.left(k)
    }
  }

  override def right(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.right(k)
    }
  }

  override def last: Option[K] = ???
  override def split(): Lego[T, K, V] = ???
  override def canBorrowTo(p: Lego[T, K, V]): Boolean = ???
  override def borrowLeftTo(p: Lego[T, K, V]): Lego[T, K, V] = ???
  override def borrowRightTo(p: Lego[T, K, V]): Lego[T, K, V] = ???
  override def merge(p: Lego[T, K, V]): Lego[T, K, V] = ???

  override def isFull(): Boolean = meta.isFull()
  override def isEmpty(): Boolean = meta.isEmpty()
  override def hasMinimum(): Boolean = meta.hasMinimum()

  def insertEmpty(data: Seq[(K, V)]): (Boolean, Int) = {
    val p = new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)

    val (ok, n) = p.insert(data)
    if(!ok) return false -> 0

    meta.insert(Seq(p.last.get -> p))._1 -> n
  }

  def insertLego(p: Lego[T, K, V], data: Seq[(K, V)]): (Boolean, Int) = {
    val max = p.last.get

    if(p.isFull()){

      val right = p.split()

      return (meta.remove(Seq(max))._1 && meta.insert(Seq(
        p.last.get -> p,
        right.last.get -> right
      ))._1) -> 0
    }

    val (ok, n) = p.insert(data)

    if(!ok) return false -> 0

    (meta.remove(Seq(max))._1 && meta.insert(Seq(p.last.get -> p))._1) -> n
  }

  override def insert(data: Seq[(K, V)]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = meta.find(k) match {
        case None => insertEmpty(list)
        case Some(p) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, p.last.get)}
          if(idx > 0) list = list.slice(0, idx)

          insertLego(p, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    this.size += size

    println(s"inserting ${data.map(_._1)}...\n")

    true -> size
  }

  def merge(left: Lego[T, K, V], lmax: K, right: Lego[T, K, V], rmax: K): Boolean = {
    left.merge(right)

    return meta.remove(Seq(lmax, rmax))._1 &&
      meta.insert(Seq(left.last.get -> left))._1
  }

  def borrow(max: K, p: Lego[T, K, V]): Boolean = {

    val lopt = meta.left(max)
    val ropt = meta.right(max)

    if(lopt.isEmpty && ropt.isEmpty){

      if(p.isEmpty()){
        println(s"\nonly one node empty\n")
        return meta.remove(Seq(max))._1
      }

      println(s"\nonly one node\n")

      return meta.remove(Seq(max))._1 && meta.insert(Seq(p.last.get -> p))._1
    }

    if(lopt.isDefined && lopt.get.canBorrowTo(p)){
      val left = lopt.get
      val lmax = left.last.get

      left.borrowLeftTo(p)

      println(s"borrow from left")

      return meta.remove(Seq(lmax, max))._1 && meta.insert(Seq(
        left.last.get -> left,
        p.last.get -> p
      ))._1
    }

    if(ropt.isDefined && ropt.get.canBorrowTo(p)){

      val right = ropt.get
      val rmax = right.last.get

      right.borrowRightTo(p)

      println(s"borrow from right")

      return meta.remove(Seq(max, rmax))._1 && meta.insert(Seq(
        p.last.get -> p,
        right.last.get -> right
      ))._1
    }

    if(lopt.isDefined) {
      println(s"merge with left...")
      return merge(lopt.get, lopt.get.last.get, p, max)
    }

    println(s"merge with right...")

    merge(p, max, ropt.get, ropt.get.last.get)
  }

  def remove(p: Lego[T, K, V], data: Seq[K]): (Boolean, Int) = {

    val max = p.last.get
    val (ok, n) = p.remove(data)

    if(!ok) return false -> 0

    if(p.hasMinimum()){

      meta.remove(Seq(max))
      meta.insert(Seq(p.last.get -> p))

      return true -> n
    }

    borrow(max, p) -> n
  }

  override def remove(data: Seq[K]): (Boolean, Int) = {

    val sorted = data.sorted
    val size = data.length
    var pos = 0

    while(pos < size){
      var list = sorted.slice(pos, size)
      val k = list(0)

      val (ok, n) = meta.find(k) match {
        case None => false -> 0
        case Some(p) =>

          val idx = list.indexWhere{k => ord.gt(k, p.last.get)}
          if(idx > 0) list = list.slice(0, idx)

          remove(p, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    this.size -= size

    true -> size
  }

  override def inOrder(): Seq[(K, V)] = {
    meta.inOrder().foldLeft(Seq.empty[(K, V)]){ case (p, (_, n)) =>
      p ++ n.inOrder()
    }
  }

  override def toString(): String = {
    val sb = new StringBuilder()

    sb.append(s"index ")
    sb.append(meta.toString)
    sb.append("\n")
    sb.append("children: ").append(meta.inOrder().map(_._2.toString))
    sb.append("end of index")
    sb.append("\n")

    sb.toString()
  }
}
