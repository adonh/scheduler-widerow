/*
 * Copyright (c) 2015, PagerDuty
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other materials provided with
 * the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
    override def toString(): String = "Bound.None"
  }

  /**
   * Some bound.
   */
  final case class Some[T] private[Bound] (val value: T, val inclusive: Boolean)
      extends Bound[T] {
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
  def none[T]: Bound[T] = None
}
