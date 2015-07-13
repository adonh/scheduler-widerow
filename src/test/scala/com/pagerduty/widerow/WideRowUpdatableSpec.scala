package com.pagerduty.widerow

import org.scalamock.FunctionAdapter4
import scala.concurrent.Future


class WideRowUpdatableSpec extends WideRowSpec {

  /**
   * Normal matchers do not work with iterable, so we use this shorthand to create a special
   * matcher.
   */
  def args[R, C, V](
      rowKey: R, drop: Boolean, remove: Iterable[C], insert: Iterable[EntryColumn[C, V]])
  :FunctionAdapter4[R, Boolean, Iterable[C], Iterable[EntryColumn[C, V]], Boolean] =
  where {
    case (`rowKey`, `drop`, argRemove, argInsert) =>
      argRemove.toSet == remove.toSet && argInsert.toSet == insert.toSet
    case _ =>
      false
  }

  "WideRowUpdatable" - {
    "update index correctly" - {
      val driver = mock[WideRowDriver[Int, String, Int]]
      (driver.executor _).stubs()
      val map = new WideRowMap(driver, 10)

      var valueId = 0
      def makeColumns(name: String, count: Int) = {
        for (i <- 0 until count) yield {
          valueId += 1
          EntryColumn(name + i.toString, valueId) // Unique value for each column.
        }
      }

      val rowKey = 100
      val batchUpdater = map(rowKey)

      "survive the basic sanity check" - {
        "inserting one element" in {
          val inserts = Set(makeColumns("basic", 1).head)
          val removes = Set.empty[String]
          batchUpdater.queueInsert(inserts.head)
          (driver.update _).expects(args(rowKey, false, removes, inserts)).returns(FutureUnit)
          batchUpdater.applyAndWait()
        }
        "removing one element" in {
          val inserts = Set.empty[EntryColumn[String, Int]]
          val removes = Set(makeColumns("basic", 1).map(_.name).head)
          batchUpdater.queueRemove(removes.head)
          (driver.update _).expects(args(rowKey, false, removes, inserts)).returns(FutureUnit)
          batchUpdater.applyAndWait()
        }
        "drop row" in {
          val inserts = Set.empty[EntryColumn[String, Int]]
          val removes = Set.empty[String]
          batchUpdater.queueDrop()
          (driver.update _).expects(args(rowKey, true, removes, inserts)).returns(FutureUnit)
          batchUpdater.applyAndWait()
        }
      }

      def insert(batchUpdater: map.BatchUpdater, name: String, count: Int) = {
        val columns = makeColumns(name, count)
        columns.foreach(batchUpdater.queueInsert(_))
        columns.map(c => (c.name, c))
      }
      def remove(updater: map.BatchUpdater, name: String, count: Int) = {
        val columns = makeColumns(name, count)
        columns.foreach(c => updater.queueRemove(c.name))
        columns.map(_.name)
      }

      "when dropping row" in {
        var inserts = Map.empty[String, EntryColumn[String, Int]]
        val removes = Set.empty[String]

        // Append stuff that will be dropped.
        insert(batchUpdater, "dropped", 5)
        remove(batchUpdater, "irrelevant", 5)
        batchUpdater.queueDrop()

        // Insert followed by remove.
        insert(batchUpdater, "insDel", 5)
        remove(batchUpdater, "insDel", 5)

        // Remove followed by insert.
        remove(batchUpdater, "delIns", 5)
        inserts ++= insert(batchUpdater, "delIns", 5)

        // Insert followed by remove followed by insert.
        insert(batchUpdater, "insDelIns", 5)
        remove(batchUpdater, "insDelIns", 5)
        inserts ++= insert(batchUpdater, "insDelIns", 5)

        //Remove followed by insert followed by remove.
        remove(batchUpdater, "delInsDel", 5)
        insert(batchUpdater, "delInsDel", 5)
        remove(batchUpdater, "delInsDel", 5)

        (driver.update _).expects(args(rowKey, true, removes, inserts.values)).returns(FutureUnit)
        batchUpdater.applyAndWait()
      }

      "when not dropping row" in {
        var inserts = Map.empty[String, EntryColumn[String, Int]]
        var removes = Set.empty[String]

        // Insert followed by remove.
        insert(batchUpdater, "insDel", 5)
        removes ++= remove(batchUpdater, "insDel", 5)

        // Remove followed by insert.
        remove(batchUpdater, "delIns", 5)
        inserts ++= insert(batchUpdater, "delIns", 5)

        // Insert followed by remove followed by insert.
        insert(batchUpdater, "insDelIns", 5)
        remove(batchUpdater, "insDelIns", 5)
        inserts ++= insert(batchUpdater, "insDelIns", 5)

        //Remove followed by insert followed by remove.
        remove(batchUpdater, "delInsDel", 5)
        insert(batchUpdater, "delInsDel", 5)
        removes ++= remove(batchUpdater, "delInsDel", 5)

        (driver.update _).expects(args(rowKey, false, removes, inserts.values)).returns(FutureUnit)
        batchUpdater.applyAndWait()
      }
    }

    "update set correctly" - {
      val driver = mock[WideRowDriver[Int, String, Array[Byte]]]
      (driver.executor _).stubs()
      val set = new WideRowSet(driver, 10)

      def makeColumns(name: String, count: Int) = {
        for (i <- 0 until count) yield EntryColumn(name + i.toString, WideRowSet.EmptyColValue)
      }
      def insert(batchUpdater: set.BatchUpdater, name: String, count: Int) = {
        val columns = makeColumns(name, count)
        columns.foreach(c => batchUpdater.queueInsert(c.name))
        columns.map(c => (c.name, c))
      }
      def remove(batchUpdater: set.BatchUpdater, name: String, count: Int) = {
        val columns = makeColumns(name, count)
        columns.foreach(c => batchUpdater.queueRemove(c.name))
        columns.map(_.name)
      }

      val rowKey = 100
      val batchUpdater = set(rowKey)

      "when dropping row" in {
        var inserts = Map.empty[String, EntryColumn[String, Array[Byte]]]
        var removes = Set.empty[String]

        // Append stuff that will be dropped.
        insert(batchUpdater, "dropped", 5)
        remove(batchUpdater, "irrelevant", 5)
        batchUpdater.queueDrop()

        // Insert followed by remove.
        insert(batchUpdater, "insDel", 5)
        remove(batchUpdater, "insDel", 5)

        // Remove followed by insert.
        remove(batchUpdater, "delIns", 5)
        inserts ++= insert(batchUpdater, "delIns", 5)

        // Insert followed by remove followed by insert.
        insert(batchUpdater, "insDelIns", 5)
        remove(batchUpdater, "insDelIns", 5)
        inserts ++= insert(batchUpdater, "insDelIns", 5)

        //Remove followed by insert followed by remove.
        remove(batchUpdater, "delInsDel", 5)
        insert(batchUpdater, "delInsDel", 5)
        remove(batchUpdater, "delInsDel", 5)

        (driver.update _).expects(args(rowKey, true, removes, inserts.values)).returns(FutureUnit)
        batchUpdater.applyAndWait()
      }

      "when not dropping row" in {
        var inserts = Map.empty[String, EntryColumn[String, Array[Byte]]]
        var removes = Set.empty[String]

        // Insert followed by remove.
        insert(batchUpdater, "insDel", 5)
        removes ++= remove(batchUpdater, "insDel", 5)

        // Remove followed by insert.
        remove(batchUpdater, "delIns", 5)
        inserts ++= insert(batchUpdater, "delIns", 5)

        // Insert followed by remove followed by insert.
        insert(batchUpdater, "insDelIns", 5)
        remove(batchUpdater, "insDelIns", 5)
        inserts ++= insert(batchUpdater, "insDelIns", 5)

        //Remove followed by insert followed by remove.
        remove(batchUpdater, "delInsDel", 5)
        insert(batchUpdater, "delInsDel", 5)
        removes ++= remove(batchUpdater, "delInsDel", 5)

        (driver.update _).expects(args(rowKey, false, removes, inserts.values)).returns(FutureUnit)
        batchUpdater.applyAndWait()
      }
    }
  }
}
