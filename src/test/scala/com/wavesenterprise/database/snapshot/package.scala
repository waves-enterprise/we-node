package com.wavesenterprise.database

import java.nio.file.{Files, Path}

import com.wavesenterprise.TestHelpers
import monix.eval.Task

import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.util.Random

package object snapshot {

  def withDirectory(dir: String)(test: Path => Unit): Unit = {
    val directory = Files.createTempDirectory(dir)
    try {
      test(directory)
    } finally {
      TestHelpers.deleteRecursively(directory)
    }
  }

  def randomBytes(maxSize: Int): Array[Byte] = {
    val bytes = new Array[Byte](Random.nextInt(maxSize))
    Random.nextBytes(bytes)
    bytes
  }

  def restartUntilStatus[T: ClassTag](task: Task[Seq[SnapshotStatus]]): Task[SnapshotStatus] = {
    task
      .delayExecution(200.millis)
      .restartUntil { statuses =>
        statuses.exists(s => classTag.runtimeClass == s.getClass)
      }
      .map { statuses =>
        statuses.filter(s => classTag.runtimeClass == s.getClass)
      }
      .map(_.head)
      .timeout(30.seconds)
  }
}
