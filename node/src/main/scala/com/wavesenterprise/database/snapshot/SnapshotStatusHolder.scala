package com.wavesenterprise.database.snapshot

import com.google.common.annotations.VisibleForTesting
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}
import play.api.libs.json.{JsObject, Json}

sealed trait SnapshotStatus {
  def name: String
  def toJson: JsObject = Json.obj("status" -> name)
}

case object Exists extends SnapshotStatus {
  override def name: String = "Exists"
}

case object Verified extends SnapshotStatus {
  override def name: String = "Verified"
}

case object NotExists extends SnapshotStatus {
  override def name: String = "NotExists"
}

case object InProgress extends SnapshotStatus {
  override def name: String = "InProgress"
}

case class Failed(error: String) extends SnapshotStatus {
  override def name: String     = "Failed"
  override def toJson: JsObject = super.toJson ++ Json.obj("error" -> error)
}

object Failed {
  def apply(e: Throwable): Failed = new Failed(e.getMessage)
}

case object Swapped extends SnapshotStatus {
  override def name: String = "Swapped"
}

class SnapshotStatusHolder(observable: Observable[SnapshotStatus])(implicit val scheduler: Scheduler) {

  @volatile var status: SnapshotStatus = NotExists

  observable.foreach { newStatus =>
    status = newStatus
  }

  /* Needed for tests */
  @VisibleForTesting
  def setStatus(newStatus: SnapshotStatus): Unit = {
    status = newStatus
  }
}

trait SnapshotStatusObserver {
  def statusObserver: Observer[SnapshotStatus]
  protected def onStatus(status: SnapshotStatus): Task[Unit] = Task.deferFuture(statusObserver.onNext(status)).void
}
