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

package com.pagerduty.widerow.chain

import scala.collection.GenTraversableOnce
import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.language.higherKinds

/**
 * WideRow API allows to chain operation that can work directly on index elements or transform
 * chunks of the index at once. The compound transformation is applied when querying.
 *
 * You begin by declaring a concrete index. Then you can call methods like filter(), map(), and
 * flatMap() like you would on any Scala collection.
 * For example: {{{(new WideRowIndex(...)).map(_.name).pageMap(dao.asyncFind(_))}}}
 *
 * When using get(), transformations are applied in parallel to fetching pages, but only to
 * one page at a time, and in the same order as pages appear in the index.
 */
trait Chainable[Result, +Chain[_]] {

  /**
   * Shared context for executing Futures.
   */
  implicit def executor: ExecutionContextExecutor

  /**
   * Chains page mapping to the current operation.
   *
   * @param mapping page mapping
   * @tparam R result type
   * @return new operation equivalent to `this() then mapping()`
   */
  protected def chain[R](mapping: IndexedSeq[Result] => Future[IndexedSeq[R]]): Chain[R]

  /**
   * Groups elements.
   *
   * @param size the group size
   * @return a new operation that groups results
   */
  protected def group(size: Int): Chain[IndexedSeq[Result]]

  /**
   * Similar to `seq.distinct()`.
   * Keep the first occurrence of each element.
   */
  def distinct(): Chain[Result] = {
    val set = scala.collection.mutable.Set.empty[Result]

    chain(seq => Future.successful {
      set.synchronized {
        seq.filter { e =>
          val unique = !set.contains(e)
          if (unique) set += e
          unique
        }
      }
    })
  }

  /**
   * Similar to `seq.filter()`.
   */
  def filter(filter: Result => Boolean): Chain[Result] = {
    chain(seq => Future.successful(seq.filter(filter)))
  }

  /**
   * Similar to `seq.filterNot()`.
   */
  def filterNot(filter: Result => Boolean): Chain[Result] = {
    chain(seq => Future.successful(seq.filterNot(filter)))
  }

  /**
   * Similar to `seq.colect()`.
   */
  def collect[R](collector: PartialFunction[Result, R]): Chain[R] = {
    chain(seq => Future.successful(seq.collect(collector)))
  }

  /**
   * Similar to `seq.map()`.
   */
  def map[R](mapping: Result => R): Chain[R] = {
    chain(seq => Future.successful(seq.map(mapping)))
  }

  /**
   * Similar to `seq.flatMap()`.
   */
  def flatMap[R](mapping: Result => scala.collection.GenTraversableOnce[R]): Chain[R] = {
    chain(seq => Future.successful(seq.flatMap(mapping)))
  }

  /**
   * Groups elements in size chunks. Grouping will change the size of chunks that are fed into
   * subsequent pageMap() transformation. Transformations before `grouped()` cannot be executed in
   * parallel.
   *
   * {{{col.grouped(n).flatten().pageMap(query(_)}}} combination can be used to enforce batch size
   * on subsequent queries.
   */
  def grouped(size: Int): Chain[IndexedSeq[Result]] = {
    group(size)
  }

  /**
   * Similar to `seq.flatten()`.
   */
  def flatten[R](implicit asTraversable: (Result) => GenTraversableOnce[R]): Chain[R] = {
    chain(seq => Future.successful {
      val chunksAsSeqs = for (chunk <- seq) yield {
        asTraversable(chunk).toIndexedSeq
      }
      chunksAsSeqs.flatten
    })
  }

  /**
   * Similar to map(), but handles Future as mapping return type.
   */
  def asyncMap[R](mapping: Result => Future[R]): Chain[R] = {
    chain(seq => {
      val init = Future.successful(IndexedSeq.empty[R])
      seq.foldLeft(init) { (future, result) =>
        future.flatMap { accum => mapping(result).map(transformed => accum :+ transformed) }
      }
    })
  }

  /**
   * Maps a sequence of elements asynchronously using underlying pages directly.
   */
  def pageMap[R](mapping: IndexedSeq[Result] => Future[Iterable[R]]): Chain[R] = {
    chain(mapping(_).map(_.toIndexedSeq))
  }
}
