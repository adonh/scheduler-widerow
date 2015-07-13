package com.pagerduty.widerow

import com.pagerduty.widerow.chain.{Source, Chainable}


/**
 * Wide row index with column names and no column values.
 * Extend this class to tap into the WideRow API.
 *
 * See [[Chainable]] for more details.
 *
 * @param pageSize Query results are internally fetched in blocks of this size. A good value is
 *                 on the order of 100, but the best size is likely approximately equal to the
 *                 expected number of results to the query.
 */
class WideRowSet[RowKey, ColName](
    driver: WideRowDriver[RowKey, ColName, Array[Byte]],
    pageSize: Int)
  extends WideRowView[RowKey, ColName, Array[Byte], ColName](
      driver, Source[RowKey, ColName, Array[Byte]](driver.executor).map(_.column.name), pageSize)
  with WideRowUpdatable[RowKey, ColName, Array[Byte]]
{
  protected def emptyColValue = WideRowSet.EmptyColValue

  /**
   * Allows to manipulate index using pretty syntax.
   */
  class BatchUpdater(rowKey: RowKey) extends super.BatchUpdater(rowKey) {

    /**
     * Queues a single column to be inserted into the index.
     */
    def queueInsert(colName: ColName) :this.type = {
      queueInsert(EntryColumn(colName, emptyColValue))
    }
  }
  override def apply(rowKey: RowKey) = new BatchUpdater(rowKey)
}


object WideRowSet {
  final val EmptyColValue = Array.empty[Byte]
}
