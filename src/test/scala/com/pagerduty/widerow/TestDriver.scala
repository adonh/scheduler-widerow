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
      drop: Boolean,
      delete: Iterable[ColName],
      insert: Iterable[EntryColumn[ColName, ColValue]])
  :Future[Unit] = {
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
