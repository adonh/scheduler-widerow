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
      ttlSeconds: Option[Int] = this.ttlSeconds)
  : EntryColumn[ColName, ColValue] = {
    new EntryColumn(name, value, ttlSeconds)
  }

  override def toString: String = {
    val ttlString = if (ttlSeconds.isDefined) ", ttl=" + ttlSeconds.get.toString else ""
    s"EntryColumn($name, $value$ttlString)"
  }
}

object EntryColumn {
  def apply[ColName, ColValue](name: ColName, value: ColValue, ttlSeconds: Option[Int])
  : EntryColumn[ColName, ColValue] = {
    new EntryColumn(name, value, ttlSeconds)
  }
}
