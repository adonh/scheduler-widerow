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

package com.pagerduty.widerow.chain

import com.pagerduty.widerow.Entry
import scala.concurrent.{ ExecutionContextExecutor, Future }

/**
 * Inserting this operation into the chain will force all preceding operations into sequential
 * part of the transformation.
 */
private[widerow] class Accumulator[Source](
  val wrapped: OpChain[Source],
  val batchSize: Int,
  val pending: IndexedSeq[IndexedSeq[Source]],
  val unused: IndexedSeq[Source]
)
    extends OpChain[IndexedSeq[Source]] {
  implicit val executor: ExecutionContextExecutor = wrapped.executor

  def this(wrapped: OpChain[Source], batchSize: Int) = {
    this(wrapped, batchSize, IndexedSeq.empty, IndexedSeq.empty)
  }

  private[this] def getPendingAndUnused(srcResults: IndexedSeq[Source]): (IndexedSeq[IndexedSeq[Source]], IndexedSeq[Source]) = {
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
