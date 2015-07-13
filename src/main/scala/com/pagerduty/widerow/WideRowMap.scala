package com.pagerduty.widerow

import com.pagerduty.widerow.chain.{Source, Chainable}


/**
 * The most basic WideRow map. Extend this class to tap into the WideRow API.
 *
 * See [[Chainable]] for more details.
 *
 * @param pageSize Query results are internally fetched in blocks of this size. A good value is
 *                 on the order of 100, but the best size is likely approximately equal to the
 *                 expected number of results to the query.
 */
class WideRowMap[RowKey, ColName, ColValue](
    driver: WideRowDriver[RowKey, ColName, ColValue],
    pageSize: Int)
  extends WideRowView[RowKey, ColName, ColValue, EntryColumn[ColName, ColValue]](
      driver, Source[RowKey, ColName, ColValue](driver.executor).map(_.column), pageSize)
  with WideRowUpdatable[RowKey, ColName, ColValue]
