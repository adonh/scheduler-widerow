package com.pagerduty.widerow


/**
 * Allows to specify inclusive/exclusive bounds.
 */
sealed trait Bound[+T] {

  /**
   * @return true if bound is defined, false if unbounded
   */
  def isDefined: Boolean

  /**
   * Bound value.
   */
  def value: T

  /**
   * Indicates if bound is inclusive. Exclusive when false.
   */
  def inclusive: Boolean

  /**
   * Bound value option, None when bound is not defined.
   */
  def valueOption: Option[T]
}

object Bound {
  /**
   * Allows to preform unbounded queries.
   */
  object None extends Bound[Nothing] {
    override def isDefined: Boolean = false
    override def value: Nothing = throw new NoSuchElementException("Bound.None.value")
    override def inclusive: Boolean = false
    override def valueOption: Option[Nothing] = scala.None
    override def toString() :String = "Bound.None"
  }

  /**
   * Some bound.
   */
  final case class Some[T] private[Bound] (val value: T, val inclusive: Boolean)
    extends Bound[T]
  {
    override def isDefined: Boolean = true
    override def valueOption: Option[T] = scala.Some(value)
    override def toString(): String = {
      s"Bound($value, ${(if (inclusive) "inclusive" else "exclusive")})"
    }
  }

  /**
   * Main factory.
   */
  def apply[T](value: T, inclusive: Boolean = true): Bound[T] = Some(value, inclusive)

  /**
   * Special factory that will convert value option into a bound.
   */
  def from[T](option: Option[T], inclusive: Boolean = true): Bound[T] = {
    if (option.isDefined) Some(option.get, inclusive) else None
  }

  /**
   * Typed None.
   */
  def none[T] :Bound[T] = None
}
