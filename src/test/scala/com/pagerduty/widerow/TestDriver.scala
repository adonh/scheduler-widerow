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

import java.util.concurrent.{Executors, ThreadFactory}

import scala.Ordering.Implicits._
import scala.concurrent.{ExecutionContext, Promise, Future}


class TestDriver[RowKey, ColName : Ordering, ColValue](
    val async: Boolean,
    val data: Map[RowKey, IndexedSeq[EntryColumn[ColName, ColValue]]])
  extends WideRowDriver[RowKey, ColName, ColValue]
{
  private val threadPool = {
    Executors.newFixedThreadPool(20,
      new ThreadFactory {
        def newThread(task: Runnable) = {
          val thread = new Thread(task)
          thread.setDaemon(true)
          thread
        }
      })
  }
  val executor = ExecutionContext.fromExecutor(threadPool)

  def asyncTask[T](block: => T) :Future[T] = {
    val promise = Promise[T]()
    threadPool.submit(new Runnable {
      def run() {
        try {
          promise.success(block)
        }
        catch {
          case error: Throwable =>
            promise.failure(error)
            throw error
        }
      }
    })
    promise.future
  }

  def fetchData(
      rowKey: RowKey,
      ascending: Boolean,
      from: Option[ColName],
      to: Option[ColName],
      limit: Int)
  :Future[IndexedSeq[Entry[RowKey, ColName, ColValue]]] = {

    def task() = {
      val res =
        if (ascending) {
          val row = data.get(rowKey).getOrElse(IndexedSeq.empty)
          val row0 = from.map(bound => row.dropWhile(_.name < bound)).getOrElse(row)
          val row01 = to.map(bound => row0.takeWhile(_.name <= bound)).getOrElse(row0)
          row01
        }
        else {
          val row = data.get(rowKey).getOrElse(IndexedSeq.empty).reverse
          val row0 = from.map(bound => row.dropWhile(_.name > bound)).getOrElse(row)
          val row01 = to.map(bound => row0.takeWhile(_.name >= bound)).getOrElse(row0)
          row01
        }

      res.take(limit).map(Entry(rowKey, _))
    }

    if (this.async) asyncTask(task())
    else Future.successful(task())
  }

  def update(
      rowKey: RowKey,
      delete: Iterable[ColName],
      insert: Iterable[EntryColumn[ColName, ColValue]])
  :Future[Unit] = {
    throw new UnsupportedOperationException("Not implemented.")
  }

  def deleteRow(rowKey: RowKey): Future[Unit] = {
    throw new UnsupportedOperationException("Not implemented.")
  }
}


object TestDriver {

  def withData(async: Boolean, blockSize: Int) = {
    def row(id: String, from: Int, count: Int)
    :(String, IndexedSeq[EntryColumn[String, Array[Byte]]]) =
    {
      val columns = for (n <- from until from + count) yield {
        new EntryColumn(id + (100 + n).toString, WideRowSet.EmptyColValue)
      }
      (id, columns)
    }

    val data = {
      ('b' to 'f').zipWithIndex.map { case (symbol, index) =>
        row(symbol.toString, 1, blockSize*(index + 1))
      }
    }.toMap + row("a", 1, 1) // Special row with 1 column.

    val nameData = data.map { case(rowKey, cols) => (rowKey, cols.map(_.name)) }

    (nameData, new TestDriver(async, data))
  }

  def expectedData(data: Map[String, IndexedSeq[String]])(
      limit: Option[Int],
      rowKeys: Seq[String],
      lower: Bound[String],
      upper: Bound[String],
      reverse: Boolean)
  :IndexedSeq[String] = {
    val unbounded = rowKeys.toIndexedSeq.flatMap(data.get).flatten
    val withLowerBound =
      if (lower.isDefined && lower.inclusive) unbounded.dropWhile(_ < lower.value)
      else if (lower.isDefined && !lower.inclusive) unbounded.dropWhile(_ <= lower.value)
      else unbounded

    val withUpperBound =
      if (upper.isDefined && !upper.inclusive) withLowerBound.takeWhile(_ < upper.value)
      else if (upper.isDefined && upper.inclusive) withLowerBound.takeWhile(_ <= upper.value)
      else withLowerBound

    val ordered = if (reverse) withUpperBound.reverse else withUpperBound
    if (limit.isDefined) ordered.take(limit.get) else ordered
  }
}
