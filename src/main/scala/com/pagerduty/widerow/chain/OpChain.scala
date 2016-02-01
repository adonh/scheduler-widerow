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
import scala.concurrent.Future

/**
 * Represents a chain of operations that can be applied to wide row data.
 *
 * See [[Chainable]] for more details.
 */
abstract class OpChain[Result] private[widerow] () extends Chainable[Result, OpChain] {
  /**
   * Apply parallel part of the transformation represented by this chain.
   */
  private[widerow] def apply(): Future[IndexedSeq[Result]]

  /**
   * Apply the full chain of operations to any leftover elements.
   */
  private[widerow] def drain(): Future[IndexedSeq[Result]]

  /**
   * Apply sequential part of the transformation to the next page.
   */
  private[widerow] def next(page: IndexedSeq[Entry[_, _, _]]): Future[OpChain[Result]]

  protected[widerow] def chain[R](mapping: IndexedSeq[Result] => Future[IndexedSeq[R]]): OpChain[R] = new PageMap(this, mapping)

  protected[widerow] def group(size: Int): OpChain[IndexedSeq[Result]] = new Accumulator(this, size)
}
