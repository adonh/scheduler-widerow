package com.pagerduty.widerow.chain

import com.pagerduty.widerow.{Bound, Entry, WideRowDriver}
import scala.concurrent.{ExecutionContextExecutor, Future}


/**
 * This is a low level query page that handles a metric ton of corner cases.
 * Change this at your own risk.
 */
private[widerow] class QueryPage[RowKey, ColName, ColValue](
    val driver: WideRowDriver[RowKey, ColName, ColValue],
    val pageSize: Int,
    val rowKeys: List[RowKey],
    val ascending: Boolean,
    val from: Bound[ColName],
    val to: Bound[ColName],
    val limit: Option[Int],
    val results: IndexedSeq[Entry[RowKey, ColName, ColValue]],
    val last: Option[ColName],
    val runningTotal: Int,
    val hasNextPage: Boolean)
{
  protected implicit def executor: ExecutionContextExecutor = driver.executor

  def nextPage() :Future[QueryPage[RowKey, ColName, ColValue]] = {
    if (hasNextPage) fetchNextPage()
    else Future.failed(new NoSuchElementException("No more pages."))
  }

  protected def fetchNextPage() :Future[QueryPage[RowKey, ColName, ColValue]] = {
    val exclusiveFrom = (from.isDefined && !from.inclusive)
    val exclusiveTo = (to.isDefined && !to.inclusive)

    val pageFromLimit = (limit.isDefined && runningTotal + this.pageSize >= limit.get)
    val payloadSize = if (pageFromLimit) limit.get - runningTotal else this.pageSize

    val fetchLimit = if (last.isDefined) payloadSize + 1 else payloadSize
    val fetchFrom = if (last.isDefined) last else from.valueOption

    val future = driver.fetchData(rowKeys.head, ascending, fetchFrom, to.valueOption, fetchLimit)

    future.map { raw =>
      val payloadDrop =
        (!last.isDefined && exclusiveFrom && !raw.isEmpty && raw.head.column.name == from.value)
      val pre = if (last.isDefined || payloadDrop) raw.drop(1) else raw

      val results =
        if (exclusiveTo && !pre.isEmpty && pre.last.column.name == to.value) pre.dropRight(1)
        else pre

      val rowEndDetected = (
          (pageFromLimit && !payloadDrop) ||
          raw.size < fetchLimit ||
          (to.isDefined && raw.last.column.name == to.value))

      val morePages = !rowEndDetected || !rowKeys.tail.isEmpty

      new QueryPage(
          driver, this.pageSize,
          if (rowEndDetected) rowKeys.tail else rowKeys,
          ascending, from, to, limit,
          results,
          if (rowEndDetected) None else Some(raw.last.column.name),
          runningTotal + results.size, morePages)
    }
  }
}
