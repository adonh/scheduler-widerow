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

import java.util.concurrent.{Executor, Executors}

import scala.concurrent.duration._
import scala.concurrent._


class WideRowViewSpec extends WideRowSpec {

  def dump(received: IndexedSeq[String], expected: IndexedSeq[String]) {
    println("\nDumping data...")
    println("RECEIVED:")
    println(received.mkString("\n"))
    println("EXPECTED:")
    println(expected.mkString("\n"))
  }

  def callbackDriver(pageSize: Int, pageCount: Int, callback: Int => Unit) = {
    var count = 0

    new WideRowDriver[String, Int, Array[Byte]] {
      // Must be same thread executor for the tests.
      val executor = ExecutionContext.fromExecutor(new Executor {
        override def execute(command: Runnable): Unit = command.run()
      })

      def fetchData(
          rowKey: String,
          ascending: Boolean,
          from: Option[Int],
          to: Option[Int],
          limit: Int)
      : Future[IndexedSeq[Entry[String, Int, Array[Byte]]]] = synchronized {
        val res = Future.successful {
          def columns(data: Seq[Int]) = {
            data.map(colName =>
              Entry(rowKey, EntryColumn(colName, WideRowSet.EmptyColValue))
            ).toIndexedSeq
          }
          if (count == 0) columns(1 to pageSize)
          else columns(count * pageSize to (count + 1) * pageSize)
        }
        callback(count)
        count += 1
        res
      }

      def update(
          rowKey: String,
          remove: Iterable[Int],
          insert: Iterable[EntryColumn[Int, Array[Byte]]])
      : Future[Unit] = {
        throw new UnsupportedOperationException("Not implemented.")
      }

      override def deleteRow(rowKey: String): Future[Unit] = {
        throw new UnsupportedOperationException("Not implemented.")
      }
    }
  }

  "WideRowView should" - {

    "when invoking methods" - {
      "check pageSize" in {
        val (data, driver) = TestDriver.withData(false, 3)
        val set = new WideRowSet(driver, 3)

        intercept[IllegalArgumentException] {
          new WideRowSet(driver, 0)
        }
        intercept[IllegalArgumentException] {
          set.withPageSize(0)
        }
      }

      "check get() arguments" in {
        val (_, driver) = TestDriver.withData(false, 3)
        val set = new WideRowSet(driver, 3)

        intercept[IllegalArgumentException] {
          set.get(Some(-1), Seq("a"))
        }
      }

      "check limGet() arguments" in {
        val (_, driver) = TestDriver.withData(false, 3)
        val set = new WideRowSet(driver, 3)

        intercept[IllegalArgumentException] {
          set.limGet(Some(-1), None, Seq("a"))
        }
        intercept[IllegalArgumentException] {
          set.limGet(Some(1), Some(-1), Seq("a"))
        }
      }

      "obey pageSize" in {
        val key = 100

        for (pageSize <- 1 until 10) {
          val driver = mock[WideRowDriver[Int, String, Array[Byte]]]
          (driver.executor _).stubs().returns(scala.concurrent.ExecutionContext.Implicits.global)
          val res = Future.successful(IndexedSeq.empty)
          (driver.fetchData _).expects(key, true, None, None, pageSize).returns(res)
          val index = new WideRowSet(driver, 10)
          Await.result(index.withPageSize(pageSize).get(None, Seq(key)), Duration.Inf)
        }
      }
    }

    "when dealing with all possible permutations of arguments" - {
      "handle all get() corner cases" in {
        // This test ensures that page-ops are fully drained.
        val blockSize = 5; assert(blockSize == 5) // This is pretty much hard-coded for this test.
        val (data, driver) = TestDriver.withData(async = true, blockSize)
        val expecting = TestDriver.expectedData(data)_

        val set = new WideRowSet(driver, blockSize)
          .grouped(3).map(_.map(_ + "x")).flatten
          .map(_ + "y")

        object combinations {
          val pageSize = List(1, 2, 3, 5, 7)
          assert(pageSize.size == 5)

          val colLimit = None :: List(0, 1, 2, 11, 22, 33, 44, 55, 66, 77).map(Some(_))
          assert(colLimit.size == 11)

          val rowKeys =
            List("", "a", "ab", "abc", "abcdef", "b", "bc", "c", "cd", "bcdef")
              .map(_.toList.map(_.toString))
          assert(rowKeys.size == 10)

          val reverse = List(true, false)
          assert(reverse.size == 2)

          // 5 pageSize = 1, 2, 3, 5, 7
          // 11 colLimit = None, 0, 1, 2, 11, 22, 33, 44, 55, 66, 77
          // 10 rowKeys = "", a, ab, abc, abcdef, b, bc, c, cd, bcdef
          // 2 reverse = true, false
          // Total = 5*10*2*11 = 1100
          val total = 1100
        }

        var count = 0
        for {
          pageSize <- combinations.pageSize
          colLimit <- combinations.colLimit
          rowKeys <- combinations.rowKeys
          reverse <- combinations.reverse
        } {
          val res = Await.result(
            set.withPageSize(pageSize)
              .get(colLimit, rowKeys, Bound.None, Bound.None, reverse),
            Duration.Inf)

          val expected =
            expecting(colLimit, rowKeys, Bound.None, Bound.None, reverse)
            .map(_ + "xy")

          if (res != expected) {
            dump(received = res, expected = expected)
            fail(
                "Test failed with" +
                " pageSize=" + pageSize +
                ", colLimit=" + colLimit +
                ", rowKeys=" + rowKeys +
                ", reverse=" + reverse +
                ".")
          }
          count += 1
        }
        count shouldBe combinations.total
      }

      "handle all limGet() corner cases" in {
        // This test ensures limit works as expected.
        val blockSize = 6; assert(blockSize == 6) // This is pretty much hard-coded for this test.
        val (data, driver) = TestDriver.withData(async = true, blockSize)
        val set = new WideRowSet(driver, blockSize).filter(_.drop(1).toInt % 2 == 0)
        val expecting = TestDriver.expectedData(data)_

        object combinations {
          val pageSize = List(1, 2, 6)
          assert(pageSize.size == 3)

          val limit = None :: List(0, 1, 2, 3, 5, 6, 8, 9, 14, 15, 100).map(Some(_))
          assert(limit.size == 12)

          val colLimit = limit
          assert(colLimit.size == 12)

          val rowKeys =
            List("", "a", "ab", "abc", "b", "bc", "c", "cd")
              .map(_.toList.map(_.toString))
          assert(rowKeys.size == 8)

          val reverse = List(true, false)
          assert(reverse.size == 2)

          // 3 pageSize = 1, 2, 6
          // 12 limit = None, 0, 1, 2, 3, 5, 6, 8, 9, 14, 15, 100
          // 12 colLimit = None, 0, 1, 2, 3, 5, 6, 8, 9, 14, 15, 100
          // 8 rowKeys = "", a, ab, abc, b, bc, c, cd
          // 2 reverse = true, false
          // Total = 3*12*8*2*12 = 6912
          val total = 6912
        }

        var count = 0
        for {
          pageSize <- combinations.pageSize
          limit <- combinations.limit
          colLimit <- combinations.colLimit
          rowKeys <- combinations.rowKeys
          reverse <- combinations.reverse
        } {
          val res = Await.result(
            set.withPageSize(pageSize)
              .limGet(limit, colLimit, rowKeys, Bound.None, Bound.None, reverse),
            Duration.Inf)

          val expectedUnlim =
            expecting(colLimit, rowKeys, Bound.None, Bound.None, reverse)
              .filter(_.drop(1).toInt % 2 == 0)

          val expected = if (limit.isDefined) expectedUnlim.take(limit.get) else expectedUnlim

          if (res != expected) {
            dump(received = res, expected = expected)
            fail(
                "Test failed with" +
                " pageSize=" + pageSize +
                ", limit=" + limit +
                ", colLimit=" + colLimit +
                ", rowKeys=" + rowKeys +
                ", reverse=" + reverse +
                ".")
          }
          count += 1
        }
        count shouldBe combinations.total
      }

      "handle all QueryPage corner cases" in {
        // This test checks all the corner cases for QueryPage.
        val blockSize = 3; assert(blockSize == 3) // This is pretty much hard-coded for this test.
        val (data, driver) = TestDriver.withData(async = false, blockSize)
        val set = new WideRowSet(driver, blockSize)
        val expecting = TestDriver.expectedData(data)_

        object combinations {
          val pageSize = (1 to 4)
          assert(pageSize.size == 4)

          val colLimit = None :: ((0 to 10).toList ::: (14 to 16).toList).map(Some(_))
          assert(colLimit.size == 15)

          val rowKeys =
            List("a", "ab", "abc", "b", "bc", "c", "cd")
              .map(_.toList.map(_.toString))
          assert(rowKeys.size == 7)

          private val bk = List("a101", "c101", "c102", "c105x", "c106", "d109")
          private val bounds = Bound.None :: bk.map(Bound(_, true)) ::: bk.map(Bound(_, false))

          val lowerBound = bounds
          assert(lowerBound.size == 13)

          val upperBound = bounds
          assert(upperBound.size == 13)

          val reverse = List(true, false)
          assert(reverse.size == 2)

          // 4 pageSize = 1, 2, 3, 4
          // 15 colLimit = None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 15, 16
          // 7 rowKeys = a, ab, abc, b, bc, c, cd
          // 13 lowerBound = None, 1a, 4c, 1d // include/exclude
          // 13 upperBound = None, 1a, 4c, 1d // include/exclude
          // 2 reverse = true, false
          // Total = 4*15*7*13*13*2 = 141960
          val total = 141960
        }

        var count = 0
        for {
          pageSize <- combinations.pageSize
          colLimit <- combinations.colLimit
          rowKeys <- combinations.rowKeys
          lowerBound <- combinations.lowerBound
          upperBound <- combinations.upperBound
          reverse <- combinations.reverse
        } {
          val res = Await.result(
            set.withPageSize(pageSize)
              .get(colLimit, rowKeys, lowerBound, upperBound, reverse), Duration.Inf)
          val expected = expecting(colLimit, rowKeys, lowerBound, upperBound, reverse)

          if (res != expected) {
            dump(received = res, expected = expected)
            fail(
                "Test failed with" +
                " pageSize=" + pageSize +
                ", colLimit=" + colLimit +
                ", rowKeys=" + rowKeys +
                ", lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                ", reverse=" + reverse +
                ".")
          }
          count += 1
        }
        count shouldBe combinations.total
      }
    }

    "when executing get() methods" - {
      val pageLoad = (0 to 4).map("page" + _)
      val seqPart = (0 to 4).map("seq" + _)
      val pageContents = IndexedSeq(1, 2)

      "run transformations sequentially for limGet()" in {
        val pageSize = 2; assert(pageSize == 2) // Value is hard-coded in this test.
        val pageCount = 3; assert(pageCount == 3) // Value is hard-coded in this test.

        // Tracks the sequence of events.
        var events = IndexedSeq.empty[String]
        def appendEvent(event: String) = {
          events :+= event
        }

        // Page requests are fulfilled on demand.
        val driver = callbackDriver(pageSize, pageCount, index => appendEvent("page" + index))

        var seqCount = 0
        val seqPromises = (1 to pageCount).map(_ => Promise[IndexedSeq[Int]]())

        val set =
          new WideRowSet(driver, pageSize)
            .pageMap { seq =>
              // Returns a fake promise instead of actual mapping.
              val res = seqPromises(seqCount).future
              appendEvent("seq" + seqCount)
              seqCount += 1
              res
            }

        // Run the query first.
        val queryFuture =
          set.limGet(Some(pageSize * pageCount), None, Seq("rowKey"), Bound.None, Bound.None, false)

        events should contain theSameElementsAs (pageLoad.take(1) ++ seqPart.take(1))

        seqPromises(0).success(pageContents)
        events should contain theSameElementsAs (pageLoad.take(2) ++ seqPart.take(2))

        seqPromises(1).success(pageContents)
        events should contain theSameElementsAs (pageLoad.take(3) ++ seqPart.take(3))

        seqPromises(2).success(pageContents)

        // Ensure query completion.
        Await.result(queryFuture, 1.second)
      }

      "run transformations independently for get()" in {
        val pageSize = 2; assert(pageSize == 2) // Value is hard-coded in this test.
        val pageCount = 3; assert(pageCount == 3) // Value is hard-coded in this test.

        // Tracks the sequence of events.
        var events = IndexedSeq.empty[String]
        def appendEvent(event: String) = synchronized {
          events :+= event
        }

        // Page requests are fulfilled on demand.
        val driver = callbackDriver(pageSize, pageCount, index => appendEvent("page" + index))

        var seqCount = 0
        val seqPromises = (1 to pageCount).map(_ => Promise[IndexedSeq[Int]]())

        val set =
          new WideRowSet(driver, pageSize)
            .pageMap { seq =>
              // Returns a fake promise instead of actual mapping.
              val res = seqPromises(seqCount).future
              appendEvent("seq" + seqCount)
              seqCount += 1
              res
            }

        // Run the query first.
        val queryFuture =
          set.get(Some(pageSize * pageCount), Seq("rowKey"), Bound.None, Bound.None, false)

        events should contain theSameElementsAs (pageLoad.take(2) ++ seqPart.take(1))

        seqPromises(0).success(pageContents)
        events should contain theSameElementsAs (pageLoad.take(3) ++ seqPart.take(2))

        seqPromises(1).success(pageContents)
        events should contain theSameElementsAs (pageLoad.take(3) ++ seqPart.take(3))

        seqPromises(2).success(pageContents)

        // Ensure query completion.
        Await.result(queryFuture, 1.second)
      }
    }
  }

  "WideRowView transformations should" - {
    val blockSize = 3
    val rowKeys = Seq("a", "b", "c", "d", "e", "f")

    val (data, driver) = TestDriver.withData(async = false, blockSize)
    val set = new WideRowSet(driver, blockSize)
    val expecting = TestDriver.expectedData(data)(None, rowKeys, Bound.None, Bound.None, false)

    "find distinct correctly" in {
      val view = set.map(_.drop(1)).distinct()
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1)).distinct
    }

    "filter correctly" in {
      val view = set.map(_.drop(1).toInt).filter(_ % 2 == 0)
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1).toInt).filter(_ % 2 == 0)
    }

    "filterNot correctly" in {
      val view = set.map(_.drop(1).toInt).filterNot(_ % 2 == 0)
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1).toInt).filterNot(_ % 2 == 0)
    }

    "collect correctly" in {
      val IntStr = """\w(\d*)""".r
      val view = set.collect{ case IntStr(x) => x.toInt }
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.collect{ case IntStr(x) => x.toInt }
    }

    "map correctly" in {
      val view = set.map(_.drop(1).toInt)
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1).toInt)
    }

    "flatMap correctly" in {
      val view = set.flatMap(_.toSeq)
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.flatMap(_.toSeq)
    }

    "group correctly" in {
      val view = set.map(_.drop(1).toInt).grouped(3)
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1).toInt).grouped(3).toSeq
    }

    "flatten correctly" in {
      val view = set.map(_.toSeq).flatten
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.toSeq).flatten
    }

    "asyncMap correctly" in {
      val view = set.asyncMap(e => Future.successful(e.drop(1).toInt))
      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1).toInt)
    }

    "pageMap correctly" in {
      var fullPageCount = 0
      val view = set.pageMap { seq =>
        if (seq.size == blockSize) fullPageCount += 1
        Future.successful(seq.map(_.drop(1).toInt))
      }

      val res = Await.result(view.get(None, rowKeys), Duration.Inf)
      res shouldBe expecting.map(_.drop(1).toInt)
      fullPageCount shouldBe expecting.size/blockSize
    }
  }
}
