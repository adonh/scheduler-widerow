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

package com.pagerduty.widerow

/**
 * Represents wide row entry.
 *
 * @param rowKey row key value
 * @param column column value
 * @tparam RowKey row key type
 * @tparam ColName column name type
 * @tparam ColValue column value type
 */
case class Entry[RowKey, ColName, ColValue](rowKey: RowKey, column: EntryColumn[ColName, ColValue])

/**
 * Represents wide row column.
 *
 * Note that the custom ttlSeconds parameter does not affect equals() and hashCode() methods.
 *
 * @param name column name value
 * @param value column value
 * @tparam ColName column name type
 * @tparam ColValue column value type
 */
final case class EntryColumn[ColName, ColValue](name: ColName, value: ColValue) {
  private[this] var _ttlSeconds: Option[Int] = None
  def ttlSeconds: Option[Int] = _ttlSeconds

  def this(name: ColName, value: ColValue, ttlSeconds: Option[Int]) = {
    this(name, value)
    _ttlSeconds = ttlSeconds
  }

  /**
   * Copies this instance, optionally replacing field values.
   */
  // Custom copy method to accommodate the extra ttlSeconds parameter.
  def copy(
    name: ColName = this.name,
    value: ColValue = this.value,
    ttlSeconds: Option[Int] = this.ttlSeconds
  ): EntryColumn[ColName, ColValue] = {
    new EntryColumn(name, value, ttlSeconds)
  }

  override def toString: String = {
    val ttlString = if (ttlSeconds.isDefined) ", ttl=" + ttlSeconds.get.toString else ""
    s"EntryColumn($name, $value$ttlString)"
  }
}

object EntryColumn {
  def apply[ColName, ColValue](name: ColName, value: ColValue, ttlSeconds: Option[Int]): EntryColumn[ColName, ColValue] = {
    new EntryColumn(name, value, ttlSeconds)
  }
}
