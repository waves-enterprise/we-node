package com.wavesenterprise

import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.state.ByteStr
import monix.eval.Task
import monix.execution.Scheduler.global
import monix.execution.schedulers.SchedulerService
import monix.execution.{Ack, Scheduler}
import monix.reactive.{Observable, Observer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._

trait RxSetup extends BeforeAndAfterAll { _: Suite =>

  implicit val rxScheduler: SchedulerService = Scheduler.singleThread("rx-scheduler")

  def test[A](block: => Task[A], timeout: FiniteDuration = 10.seconds): A = block.runSyncUnsafe(timeout)

  def send[A](observer: Observer[A])(message: A): Task[Ack] = {
    Task.deferFuture(observer.onNext(message)).delayResult(500.millis)
  }

  def buildTestSign(id: Int): ByteStr = {
    ByteStr(id.toByte +: Array.fill[Byte](SignatureLength - 1)(0))
  }

  def loadEvents[A](observer: Observable[A], duration: FiniteDuration = 1000.millis)(implicit scheduler: Scheduler): Task[Seq[A]] = {
    @volatile var collected = Seq.empty[A]
    observer.foreach(newItem => collected = collected :+ newItem)

    Task.sleep(duration).map { _ =>
      val temp = collected
      collected = Seq.empty
      temp
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    rxScheduler.awaitTermination(10.seconds, global)
  }
}
