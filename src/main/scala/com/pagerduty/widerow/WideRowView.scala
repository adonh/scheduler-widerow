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

import com.pagerduty.widerow.chain.{ QueryPage, OpChain, Chainable }

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContextExecutor, Await, Future }

/**
 * WideRowView allows chaining of operations and can perform sequential queries with
 * strict limits.
 *
 * See [[Chainable]] for more details.
 *
 * @param pageSize Query results are internally fetched in blocks of this size. A good value is
 *                 on the order of 100, but the best size is likely approximately equal to the
 *                 expected number of results to the query.
 */
class WideRowView[RowKey, ColName, ColValue, QueryResult](
  protected val driver: WideRowDriver[RowKey, ColName, ColValue],
  protected val ops: OpChain[QueryResult],
  protected val pageSize: Int
)
    extends Chainable[QueryResult, /* Chainable takes a higher kinded type Chain[_]. This means that Chain is a type
     * constructor that takes type arguments. This allows us to create parametrised type later on in
     * the code.
     * Consider: Chainable.map[R](..) :Chain[R].
     * The resulting type Chain[R] is not defined until the method is applied and R is set to
     * a concrete type.
     * In this case we want to set Chain[_] = WideRowView[RowKey, ColName, ColValue, _].
     * The complexity of the type signature below comes from the need to express the partially
     * applied type. To do so we start by defining an anonymous type as {}. Then we declare our
     * partially applied type as Partial[R] = WideRowView[RowKey, ColName, ColValue, R].
     * Finally, we extract the partially applied type by using type projection #Partial on the
     * anonymous type.
     */ ({ type Partial[R] = WideRowView[RowKey, ColName, ColValue, R] })#Partial] {
  require(pageSize > 0, "pageSize must be greater that zero.")

  implicit def executor: ExecutionContextExecutor = driver.executor

  /**
   * Returns the raw map without any transformations.
   */
  def source: WideRowIndex[RowKey, ColName, ColValue] = new WideRowIndex(driver, pageSize)

  /**
   * Create an identical view, but with a different page size.
   *
   * @param pageSize Query results are internally fetched in blocks of this size. A good value is
   *                 on the order of 100, but the best size is likely approximately equal to the
   *                 expected number of results to the query.
   */
  def withPageSize(pageSize: Int): WideRowView[RowKey, ColName, ColValue, QueryResult] = {
    new WideRowView(driver, ops, pageSize)
  }

  protected def chain[R](mapping: IndexedSeq[QueryResult] => Future[IndexedSeq[R]]) = {
    new WideRowView(driver, ops.chain(mapping), pageSize)
  }
  protected def group(size: Int) = {
    new WideRowView(driver, ops.group(size), pageSize)
  }

  /**
   * Finds an element matching given predicate.
   *
   * Keep in mind that this method will always load and transform the whole page of data, even
   * when the first element matches the predicate.
   */
  def find(
    colLimit: Option[Int],
    rowKeys: Seq[RowKey],
    lowerBound: Bound[ColName] = Bound.None,
    upperBound: Bound[ColName] = Bound.None,
    reverse: Boolean = false
  )(predicate: QueryResult => Boolean): Future[Option[QueryResult]] = {
    filter(predicate)
      .limGet(Some(1), colLimit, rowKeys, lowerBound, upperBound, reverse)
      .map(_.headOption)
  }

  /**
   * Slow query that avoids making unneeded database requests. Use this method when you do not
   * have one-to-one mapping between indexed elements and query results. When limit is not defined,
   * this method will simply forward the call to the faster get() method.
   *
   * When doing reverse query, the columns are visited starting from upper bound and moving
   * towards lower bound. For example, to query the latest elements of time series, newest first:
   * {{{
   *   timeSeries.limGet(Some(10), None, Seq(rowKey), Bound.None, Bound.None, reverse = true)
   * }}}
   *
   * Order of operations: {{{
   * page = load index page
   * mapping = transformations applied to page results
   *
   *                     nextPage                 nextPage
   * --page--> --mapping--> | --page--> --mapping--> |...
   * }}}
   *
   * Example: {{{
   *   index.map(_.name)
   *     .pageMap(dao.asyncFind(_))
   *     .limGet(Some(10), rowKeys) // Transformations are applied after loading each page.
   * }}}
   *
   * @param limit maximum number of elements that will be returned
   * @param colLimit maximum number of columns that may be loaded
   * @param rowKeys rows to query, always in ascending order
   * @param lowerBound lower bound based on absolute ordering of WideRow column names
   * @param upperBound upper bound based on absolute ordering of WideRow column names
   * @param reverse true to reverse query direction and the order of results, false otherwise
   */
  def limGet(
    limit: Option[Int],
    colLimit: Option[Int],
    rowKeys: Seq[RowKey],
    lowerBound: Bound[ColName] = Bound.None,
    upperBound: Bound[ColName] = Bound.None,
    reverse: Boolean = false
  ): Future[IndexedSeq[QueryResult]] = {
    if (!limit.isDefined) return get(colLimit, rowKeys, lowerBound, upperBound, reverse)

    require(limit.get >= 0, "limit must be greater than or equal to zero.")
    if (colLimit.isDefined)
      require(colLimit.get >= 0, "colLimit must be greater than or equal to zero.")

    if (rowKeys.isEmpty) return Future.successful(IndexedSeq.empty)

    val (from, to) = if (reverse) (upperBound, lowerBound) else (lowerBound, upperBound)
    val queryRowKeys = if (reverse) rowKeys.reverse else rowKeys

    val page0 = new QueryPage(
      driver, pageSize,
      queryRowKeys.toList, !reverse, from, to, colLimit,
      IndexedSeq.empty, None, 0, true
    )

    type Results = IndexedSeq[QueryResult]
    type Page = QueryPage[RowKey, ColName, ColValue]
    type Ops = OpChain[QueryResult]

    // Recursive function that returns a future with accumulated values.
    // Note that result types match the function argument types.
    // Strictly obeys all limits, sequential execution.
    def rec(results: Results, lastPage: Page, lastOps: Ops): Future[(Results, Page, Ops)] = {
      lastPage.nextPage().flatMap { page =>
        lastOps.next(page.results).flatMap { ops =>
          ops.apply().flatMap { transformedResults =>
            val combined = results ++ transformedResults
            if (combined.size >= limit.get)
              Future.successful((combined.take(limit.get), page, ops))
            else if (!page.hasNextPage)
              ops.drain().map(remaining => ((combined ++ remaining).take(limit.get), page, ops))
            else
              rec(combined, page, ops)
          }
        }
      }
    }

    rec(IndexedSeq.empty, page0, this.ops).map(_._1)
  }

  /**
   * Fast query that loads a new index page while asynchronously applying transformation to the
   * previous page.
   *
   * When doing reverse query, the columns are visited starting from upper bound and moving
   * towards lower bound. For example, to query the latest elements of time series, newest first:
   * {{{
   *   timeSeries.get(Some(10), Seq(rowKey), Bound.None, Bound.None, reverse = true)
   * }}}
   *
   * Order of operations: {{{
   * page = load index page
   * mapping = transformations applied to page results
   *
   *                                 join              join
   * -----page----> | +-----page----> | +-----page----> |...
   *                   \              |  \              |
   *                    +--mapping--> |   +--mapping--> |...
   * }}}
   *
   * Example: {{{
   *   index.map(_.name)
   *     .pageMap(dao.asyncFind(_)) // Will be executed in parallel to page fetching.
   *     .get(rowKeys)
   * }}}
   *
   * @param colLimit maximum number of columns that can be loaded
   * @param rowKeys rows to query, always in ascending order
   * @param lowerBound lower bound based on absolute ordering of WideRow column names
   * @param upperBound upper bound based on absolute ordering of WideRow column names
   * @param reverse true to reverse query direction and the order of results, false otherwise
   */
  def get(
    colLimit: Option[Int],
    rowKeys: Seq[RowKey],
    lowerBound: Bound[ColName] = Bound.None,
    upperBound: Bound[ColName] = Bound.None,
    reverse: Boolean = false
  ): Future[IndexedSeq[QueryResult]] = {
    if (colLimit.isDefined)
      require(colLimit.get >= 0, "colLimit must be greater than or equal to zero.")

    if (rowKeys.isEmpty) return Future.successful(IndexedSeq.empty)

    val (from, to) = if (reverse) (upperBound, lowerBound) else (lowerBound, upperBound)
    val queryRowKeys = if (reverse) rowKeys.reverse else rowKeys

    val page0 = new QueryPage(
      driver, pageSize,
      queryRowKeys.toList, !reverse, from, to, colLimit,
      IndexedSeq.empty, None, 0, true
    )

    type Results = IndexedSeq[QueryResult]
    type Page = QueryPage[RowKey, ColName, ColValue]
    type Ops = OpChain[QueryResult]

    // Recursive function that returns a future with accumulated values.
    // Note that result types match the function argument types.
    // Always loads one page ahead, fetches pages in parallel to page operations.
    def rec(results: Results, lastPage: Page, lastOps: Ops): Future[(Results, Page, Ops)] = {
      val opsFuture = lastOps.next(lastPage.results)
      val resultsFuture = opsFuture.flatMap(ops => ops.apply())
      val pageFuture = lastPage.nextPage()

      resultsFuture.zip(pageFuture).flatMap {
        case (transformedResults, page) =>
          // opsFuture has to be resolved to get thrasformedResults.
          val ops = Await.result(opsFuture, Duration.Inf)
          val combined = results ++ transformedResults

          if (!page.hasNextPage)
            ops.next(page.results).flatMap { finalOps =>
              finalOps.apply().flatMap { finalResults => // apply() then drain()
                finalOps.drain().map { drainedResults =>
                  (combined ++ finalResults ++ drainedResults, page, finalOps)
                }
              }
            }
          else
            rec(combined, page, ops)
      }
    }

    rec(IndexedSeq.empty, page0, this.ops).map(_._1)
  }
}
