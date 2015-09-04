package com.pagerduty.widerow

import scala.concurrent.{Await, Future}
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
    def queueInsert(column: EntryColumn[ColName, ColValue]) :this.type = {
      removes -= column.name
      inserts += ((column.name, column))
      this
    }

    /**
     * Queues a single column to be removed from the index.
     */
    def queueRemove(colName: ColName) :this.type = {
      removes += colName
      inserts -= colName
      this
    }

    /**
     * Perform queued operations asynchronously. Once invoked, updater resets to blank state.
     */
    def executeAsync() :Future[Unit] = {
      val future = driver.update(rowKey, removes, inserts.values)
      removes = Set.empty
      inserts = Map.empty
      future
    }
  }
  def apply(rowKey: RowKey): BatchUpdater = new BatchUpdater(rowKey)
  def deleteRow(rowKey: RowKey): Future[Unit] = driver.deleteRow(rowKey)
}
