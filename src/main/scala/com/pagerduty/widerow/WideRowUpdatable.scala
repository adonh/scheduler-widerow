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
    protected var dropRow = false
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
      if (!dropRow) removes += colName
      inserts -= colName
      this
    }

    /**
     * Queues an entire index row to be discarded.
     */
    def queueDrop() :this.type = {
      dropRow = true
      removes = Set.empty
      inserts = Map.empty
      this
    }

    /**
     * Perform queued operations asynchronously. Once invoked, updater resets to blank state.
     */
    def asyncApply() :Future[Unit] = {
      val future = driver.update(rowKey, dropRow, removes, inserts.values)
      dropRow = false
      removes = Set.empty
      inserts = Map.empty
      future
    }
    def applyAndWait() {
      Await.result(asyncApply(), Duration.Inf)
    }
  }
  def apply(rowKey: RowKey) :BatchUpdater = new BatchUpdater(rowKey)
}
