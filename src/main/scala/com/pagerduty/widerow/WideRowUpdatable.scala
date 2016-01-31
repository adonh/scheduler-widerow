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

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration

/**
 * Common interface for mutating WideRow indexes.
 */
trait WideRowUpdatable[RowKey, ColName, ColValue] {

  protected def driver: WideRowDriver[RowKey, ColName, ColValue]

  /**
   * Allows to manipulate index.
   */
  class BatchUpdater(val rowKey: RowKey) {
    protected var removes = Set.empty[ColName]
    protected var inserts = Map.empty[ColName, EntryColumn[ColName, ColValue]]

    /**
     * Queues a single column to be inserted into the index.
     */
    def queueInsert(column: EntryColumn[ColName, ColValue]): this.type = {
      removes -= column.name
      inserts += ((column.name, column))
      this
    }

    /**
     * Queues a single column to be removed from the index.
     */
    def queueRemove(colName: ColName): this.type = {
      removes += colName
      inserts -= colName
      this
    }

    /**
     * Perform queued operations asynchronously. Once invoked, updater resets to blank state.
     */
    def executeAsync(): Future[Unit] = {
      val future = driver.update(rowKey, removes, inserts.values)
      removes = Set.empty
      inserts = Map.empty
      future
    }
  }
  def apply(rowKey: RowKey): BatchUpdater = new BatchUpdater(rowKey)
  def deleteRow(rowKey: RowKey): Future[Unit] = driver.deleteRow(rowKey)
}
