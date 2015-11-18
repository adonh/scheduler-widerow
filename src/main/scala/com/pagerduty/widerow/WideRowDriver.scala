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
