package index

import java.util.UUID
import scala.reflect.ClassTag

class Block[T: ClassTag, K: ClassTag, V: ClassTag](override val id: T,
                                                   val MIN: Int,
                                                   val MAX: Int)(implicit val ord: Ordering[K])
  extends Lego[T, K, V]{

  val MIDDLE = MIN

  var size = 0
  val keys = Array.ofDim[(K, V)](MAX)

  def find(k: K, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, keys(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insertAt(k: K, v: V, idx: Int): (Boolean, Int) = {
    for(i<-size until idx by -1){
      keys(i) = keys(i - 1)
    }

    keys(idx) = k -> v

    size += 1

    true -> idx
  }

  def insert(k: K, v: V): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val (found, idx) = find(k, 0, size - 1)

    if(found) return false -> 0

    insertAt(k, v, idx)
  }

  override def insert(data: Seq[(K, V)]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val len = Math.min(MAX - size, data.length)

    for(i<-0 until len){
      val (k, v) = data(i)
      if(!insert(k, v)._1) return false -> 0
    }

    true -> len
  }

  def removeAt(idx: Int): (K, V) = {
    val data = keys(idx)

    size -= 1

    for(i<-idx until size){
      keys(i) = keys(i + 1)
    }

    data
  }

  def remove(k: K): Boolean = {
    if(isEmpty()) return false

    val (found, idx) = find(k, 0, size - 1)

    if(!found) return false

    removeAt(idx)

    true
  }

  override def remove(keys: Seq[K]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    val len = keys.length

    for(i<-0 until len){
      if(!remove(keys(i))) return false -> 0
    }

    true -> len
  }

  def slice(from: Int, n: Int): Seq[(K, V)] = {
    var slice = Seq.empty[(K, V)]

    val len = from + n

    for(i<-from until len){
      slice = slice :+ keys(i)
    }

    for(i<-(from + n) until size){
      keys(i - n) = keys(i)
    }

    size -= n

    slice
  }

  def update(data: Seq[(K, V)]): (Boolean, Int) = {

    val len = data.length

    for(i<-0 until len){
      val (k, v) = data(i)

      val (found, idx) = find(k, 0, size - 1)

      if(!found) return false -> 0

      keys(idx) = k -> v
    }

    true -> len
  }

  override def split(): Block[T, K, V] = {
    val right = new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX)

    val len = size
    val middle = len/2

    for(i<-middle until len){
      right.keys(i - middle) = keys(i)

      right.size += 1
      size -= 1
    }

    right
  }

  override def toString(): String = {
    inOrder().map(_._1).mkString("(", "," ,")")
  }

  override def find(k: K): Option[V] = {
    val (found, pos) = find(k, 0, size - 1)
    if(!found) return None
    Some(keys(pos)._2)
  }

  override def near(k: K): Option[V] = {
    val (_, pos) = find(k, 0, size - 1)
    Some(keys(if(pos < size) pos else pos - 1)._2)
  }

  override def left(k: K): Option[V] = {
    val (found, pos) = find(k, 0, size - 1)

    if(!found) return None

    val lpos = pos - 1

    if(lpos < 0) return None

    Some(keys(lpos)._2)
  }

  override def right(k: K): Option[V] = {
    val (found, pos) = find(k, 0, size - 1)

    if(!found) return None

    val rpos = pos + 1

    if(rpos >= size) return None

    Some(keys(rpos)._2)
  }

  def canBorrowTo(t: Lego[T, K, V]): Boolean = {
    val target = t.asInstanceOf[Block[T, K, V]]
    val n = MIN - target.size
    size - MIN >= n
  }

  def borrowRightTo(t: Lego[T, K, V]): Lego[T, K, V] = {
    val target = t.asInstanceOf[Block[T, K, V]]
    val n = MIN - target.size

    val list = slice(0, n)
    target.insert(list)

    target
  }

  def borrowLeftTo(t: Lego[T, K, V]): Lego[T, K, V] = {

    val target = t.asInstanceOf[Block[T, K, V]]
    val n = MIN - target.size

    val list = slice(size - n, n)
    target.insert(list)

    target
  }

  def merge(r: Lego[T, K, V]): Lego[T, K, V] = {
    val right = r.asInstanceOf[Block[T, K, V]]
    val len = right.size
    var j = size

    for(i<-0 until len){
      keys(j) = right.keys(i)
      size += 1
      j += 1
    }

    this
  }

  override def isFull(): Boolean = size == MAX
  override def isEmpty(): Boolean = size == 0
  override def hasMinimum(): Boolean = size >= MIN

  override def last: Option[K] = {
    if(isEmpty()) return None
    Some(keys(size - 1)._1)
  }

  override def inOrder(): Seq[(K, V)] = {
    if(isEmpty()) return Seq.empty[(K, V)]
    keys.slice(0, size)
  }
}
