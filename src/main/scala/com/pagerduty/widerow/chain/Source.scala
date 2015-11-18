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
