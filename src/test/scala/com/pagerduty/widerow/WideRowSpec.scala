package com.pagerduty.widerow

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, FreeSpec}
import scala.concurrent.Future


trait WideRowSpec extends FreeSpec with Matchers with MockFactory {

  val FutureUnit: Future[Unit] = Future.successful(Unit)
}
