package com.pagerduty.widerow.chain

import com.pagerduty.widerow.Entry
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Inserting this operation into the chain will force all preceding operations into sequential
 * part of the transformation.
 */
private[widerow] class Accumulator[Source](
    val wrapped: OpChain[Source],
    val batchSize: Int,
    val pending: IndexedSeq[IndexedSeq[Source]],
    val unused: IndexedSeq[Source])
  extends OpChain[IndexedSeq[Source]]
{
  implicit val executor: ExecutionContextExecutor = wrapped.executor

  def this(wrapped: OpChain[Source], batchSize: Int) = {
    this(wrapped, batchSize, IndexedSeq.empty, IndexedSeq.empty)
  }

  private[this] def getPendingAndUnused(srcResults: IndexedSeq[Source])
  :(IndexedSeq[IndexedSeq[Source]], IndexedSeq[Source]) = {
    val batches = srcResults.grouped(batchSize).toIndexedSeq

    if (!batches.isEmpty && batches.last.size < batchSize)
      (batches.dropRight(1), batches.last)
    else
      (batches, IndexedSeq.empty)
  }

  def apply() = Future.successful(pending)

  def drain() = wrapped.drain().flatMap { srcResults =>
    val pending = (unused ++ srcResults).grouped(batchSize).toIndexedSeq
    Future.successful(pending)
  }

  def next(page: IndexedSeq[Entry[_, _, _]]): Future[OpChain[IndexedSeq[Source]]] = {
    wrapped.next(page).flatMap(wrappedOp => wrappedOp.apply().map { srcResults =>
      val (pending, unused) = getPendingAndUnused(this.unused ++ srcResults)
      new Accumulator(wrappedOp, batchSize, pending, unused)
    })
  }
}
