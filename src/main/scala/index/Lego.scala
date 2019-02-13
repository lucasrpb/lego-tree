package index

trait Lego[T, K, V] {

  val id: T

  def find(k: K): Option[V]
  def near(k: K): Option[V]
  def left(k: K): Option[V]
  def right(k: K): Option[V]
  def last: Option[(K, V)]
  def split(): Lego[T, K, V]
  def canBorrowTo(p: Lego[T, K, V]): Boolean
  def borrowLeftTo(p: Lego[T, K, V]): Lego[T, K, V]
  def borrowRightTo(p: Lego[T, K, V]): Lego[T, K, V]
  def merge(p: Lego[T, K, V]): Lego[T, K, V]
  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimum(): Boolean
  def insert(data: Seq[(K, V)]): (Boolean, Int)
  def remove(keys: Seq[K]): (Boolean, Int)
  def inOrder(): Seq[(K, V)]

}
