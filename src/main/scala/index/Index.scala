package index

import java.util.UUID
import scala.reflect.ClassTag

class Index[T: ClassTag, K: ClassTag, V: ClassTag](override val id: T,
                                                   val DATA_ORDER: Int,
                                                   val META_ORDER: Int)
                                                  (implicit val ord: Ordering[K])
  extends Lego[T, K, V]{

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1

  val meta: Lego[T, K, Lego[T, K, V]] = new Block[T, K, Lego[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T],
    META_MIN, META_MAX)

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

  override def last: Option[(K, V)] = ???

  override def split(): Lego[T, K, V] = ???

  override def canBorrowTo(p: Lego[T, K, V]): Boolean = ???

  override def borrowLeftTo(p: Lego[T, K, V]): Lego[T, K, V] = ???

  override def borrowRightTo(p: Lego[T, K, V]): Lego[T, K, V] = ???

  override def merge(p: Lego[T, K, V]): Lego[T, K, V] = ???

  override def isFull(): Boolean = ???
  override def isEmpty(): Boolean = ???
  override def hasMinimum(): Boolean = ???

  override def insert(data: Seq[(K, V)]): (Boolean, Int) = ???

  override def remove(keys: Seq[K]): (Boolean, Int) = ???
  
  override def inOrder(): Seq[(K, V)] = {
    meta.inOrder().foldLeft(Seq.empty[(K, V)]){ case (p, (_, n)) =>
      p ++ n.inOrder()
    }
  }
}
