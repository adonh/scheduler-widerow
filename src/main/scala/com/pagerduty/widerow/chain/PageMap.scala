package com.pagerduty.widerow.chain

import com.pagerduty.widerow.Entry
import scala.concurrent.{ExecutionContextExecutor, Future}


private[widerow] class PageMap[Source, Result](
    val wrapped: OpChain[Source],
    val mapping: IndexedSeq[Source] => Future[IndexedSeq[Result]])
  extends OpChain[Result]
{
  implicit val executor: ExecutionContextExecutor = wrapped.executor

  private def applyMapping(seq: IndexedSeq[Source]) = {
    // Avoids applying mapping to empty sequences.
    if (seq.isEmpty) Future.successful(IndexedSeq.empty)
    else mapping(seq)
  }

  def apply() = wrapped.apply().flatMap(applyMapping)
  def drain() = wrapped.drain().flatMap(applyMapping)

  def next(page: IndexedSeq[Entry[_, _, _]])
  :Future[OpChain[Result]] = {
    wrapped.next(page).map(wrappedOp => new PageMap(wrappedOp, mapping))
  }
}
