package com.pagerduty.widerow.chain

import com.pagerduty.widerow.Entry
import scala.concurrent.Future


/**
 * Represents a chain of operations that can be applied to wide row data.
 *
 * See [[Chainable]] for more details.
 */
abstract class OpChain[Result] private[widerow]() extends Chainable[Result, OpChain] {
  /**
   * Apply parallel part of the transformation represented by this chain.
   */
  private[widerow] def apply() :Future[IndexedSeq[Result]]

  /**
   * Apply the full chain of operations to any leftover elements.
   */
  private[widerow] def drain() :Future[IndexedSeq[Result]]

  /**
   * Apply sequential part of the transformation to the next page.
   */
  private[widerow] def next(page: IndexedSeq[Entry[_, _, _]]): Future[OpChain[Result]]

  protected[widerow] def chain[R](mapping: IndexedSeq[Result] => Future[IndexedSeq[R]])
  :OpChain[R] = new PageMap(this, mapping)

  protected[widerow] def group(size: Int)
  :OpChain[IndexedSeq[Result]] = new Accumulator(this, size)
}
