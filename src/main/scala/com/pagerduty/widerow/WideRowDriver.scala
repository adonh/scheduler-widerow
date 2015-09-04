package com.pagerduty.widerow

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * WideRowDriver is responsible for fetching raw data and performing batched update operations.
 */
trait WideRowDriver[RowKey, ColName, ColValue] {

  /**
   * Shared context for executing Futures.
   */
  def executor: ExecutionContextExecutor

  /**
   * Loads raw data for a give rowKey.
   *
   * @param rowKey rowKey indicates the row we are fetching from.
   * @param ascending the order of elements in the result, descending when false.
   * @param from optional inclusive bound that represents the beginning of query range;
   *        when ascending from <= to, when descending from >= to.
   * @param to optional inclusive bound that represents the end of query range;
   *        when ascending from <= to, when descending from >= to.
   * @param limit maximum number of elements that can be returned by this query.
   */
  def fetchData(
      rowKey: RowKey,
      ascending: Boolean,
      from: Option[ColName],
      to: Option[ColName],
      limit: Int)
  : Future[IndexedSeq[Entry[RowKey, ColName, ColValue]]]

  /**
   * Performs batch updates in the following order: first drop, then remove, then insert.
   *
   * @param rowKey rowKey indicates the row we are updating.
   * @param remove a list of column names to delete.
   * @param insert a list of columns to insert.
   */
  def update(
      rowKey: RowKey,
      remove: Iterable[ColName],
      insert: Iterable[EntryColumn[ColName, ColValue]])
  : Future[Unit]

  /**
   * Drops the target row.
   *
   * @param rowKey target row key
   * @return unit future
   */
  def deleteRow(rowKey: RowKey): Future[Unit]
}
