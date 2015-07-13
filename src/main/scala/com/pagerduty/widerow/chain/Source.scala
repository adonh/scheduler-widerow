package com.pagerduty.widerow.chain

import com.pagerduty.widerow.Entry

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Root operation that starts the chain, can be treated as
 * Seq[Entry(RowKey, Column[ColName, ColValue])].
 *
 * See [[Chainable]] for more details.
 */
class Source[RowKey, ColName, ColValue] private[widerow] (
    private val currentPage: IndexedSeq[Entry[RowKey, ColName, ColValue]],
    val executor: ExecutionContextExecutor)
  extends OpChain[Entry[RowKey, ColName, ColValue]]
{
  private[widerow] def apply() = Future.successful(currentPage)
  private[widerow] def drain() = Future.successful(IndexedSeq.empty)

  private[widerow] def next(page: IndexedSeq[Entry[_, _, _]]) =
    Future.successful(
      new Source(page.asInstanceOf[IndexedSeq[Entry[RowKey, ColName, ColValue]]], executor)
    )
}

object Source {
  /**
   * Create a new chain of operations, equivalent to identity transformation.
   *
   * @param executor future executor
   * @tparam RowKey row key
   * @tparam ColName col name
   * @tparam ColValue col value
   * @return an identity chainable operation
   */
  def apply[RowKey, ColName, ColValue](executor: ExecutionContextExecutor) = {
    new Source[RowKey, ColName, ColValue](IndexedSeq.empty, executor)
  }
}
