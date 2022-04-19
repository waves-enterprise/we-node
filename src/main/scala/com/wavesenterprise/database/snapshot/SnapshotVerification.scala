package com.wavesenterprise.database.snapshot

import java.nio.file.{Files, Paths}

import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, Observer}

import scala.collection.JavaConverters._

class SnapshotVerification(snapshotDirectory: String,
                           statusObservable: Observable[SnapshotStatus],
                           val statusObserver: Observer[SnapshotStatus],
                           verification: Task[Unit])(implicit val scheduler: Scheduler)
    extends ScorexLogging
    with SnapshotStatusObserver
    with AutoCloseable {

  import SnapshotVerification._

  private[this] val cancelable = statusObservable
    .find { status =>
      status == Exists
    }
    .observeOn(scheduler)
    .mapEval { _ =>
      if (alreadyVerified()) onStatus(Verified) else runVerification
    }
    .executeOn(scheduler)
    .logErr
    .subscribe()

  protected def alreadyVerified(): Boolean = {
    val snapshotDir = Paths.get(snapshotDirectory)
    Files.exists(snapshotDir.resolve(VerifiedFile))
  }

  protected def markVerified(): Task[Unit] = Task {
    val snapshotDir = Paths.get(snapshotDirectory)
    Files.createFile(snapshotDir.resolve(VerifiedFile))
  }

  protected def runVerification: Task[Unit] = {
    (Task(log.debug("Starting snapshot verification...")) >>
      verification >> markVerified() >> onStatus(Verified) >>
      Task(log.debug("Snapshot verification completed successfully"))).onErrorHandleWith {
      case e: VerificationException =>
        Task(log.error("An snapshot verification error occurred, snapshot files will be deleted", e)) >> deleteSnapshot() >> onStatus(Failed(e))
      case t: Throwable =>
        Task(log.error("Snapshot verification failed", t)) >> onStatus(Failed(t))
    }
  }

  protected def deleteSnapshot(): Task[Unit] = Task {
    Task(Files.list(Paths.get(snapshotDirectory))).bracket { files =>
      Task {
        files.iterator().asScala.foreach(Files.delete)
      }
    } { stream =>
      Task(stream.close())
    }
  }

  override def close(): Unit = {
    cancelable.cancel()
  }
}

object SnapshotVerification {
  private val VerifiedFile = "VERIFIED"

  def apply(snapshotDirectory: String, statusConcurrentSubject: ConcurrentSubject[SnapshotStatus, SnapshotStatus], verification: Task[Unit])(
      implicit scheduler: Scheduler): SnapshotVerification = {
    new SnapshotVerification(snapshotDirectory, statusConcurrentSubject, statusConcurrentSubject, verification)(scheduler)
  }
}
