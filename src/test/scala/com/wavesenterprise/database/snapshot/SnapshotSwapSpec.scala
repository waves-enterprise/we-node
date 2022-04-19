package com.wavesenterprise.database.snapshot

import java.nio.file.{Files, Path}

import com.wavesenterprise.RxSetup
import com.wavesenterprise.TestSchedulers.consensualSnapshotScheduler
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ResourceUtils.withResource
import monix.reactive.subjects.ConcurrentSubject
import org.apache.commons.lang3.RandomStringUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SnapshotSwapSpec extends FreeSpec with MockFactory with Matchers with RxSetup {

  import SnapshotSwapSpec._

  case class FixtureParams(snapshotSwap: SnapshotSwap,
                           snapshotFiles: List[FileWithContent],
                           dataDir: Path,
                           dataFiles: List[FileWithContent],
                           status: Future[SnapshotStatus])

  protected def fileGen(dir: Path): FileWithContent = {
    val prefix = RandomStringUtils.randomAlphanumeric(FileNameLength)
    val file   = Files.createTempFile(dir, prefix, "tmp")
    val bytes  = randomBytes(Short.MaxValue)
    Files.write(file, bytes)
    FileWithContent(dir.relativize(file), ByteStr(bytes))
  }

  protected def fixture(test: FixtureParams => Unit): Unit = {
    withDirectory("snapshot-swap-test") { baseDir =>
      val dataDir = baseDir.resolve("data")
      Files.createDirectory(dataDir)
      val dataFiles = List.fill(DataFilesCount)(fileGen(dataDir))

      val snapshotDir = dataDir.resolve("snapshot")
      Files.createDirectory(snapshotDir)
      val snapshotFiles = List.fill(SnapshotFilesCount)(fileGen(snapshotDir))

      val statusPublisher = ConcurrentSubject.publish[SnapshotStatus](consensualSnapshotScheduler)
      val statusEvents    = loadEvents(statusPublisher)(consensualSnapshotScheduler)
      val statusFuture    = restartUntilStatus[Swapped.type](statusEvents).runToFuture(consensualSnapshotScheduler)

      val snapshotSwap = new SnapshotSwap(snapshotDir, dataDir, statusPublisher)

      test(FixtureParams(snapshotSwap, snapshotFiles, dataDir, dataFiles, statusFuture))
    }
  }

  "should successfully swap old state and snapshot (backup = false)" in fixture {
    case FixtureParams(snapshotSwap, snapshotFiles, dataDir, _, statusFuture) =>
      test(snapshotSwap.asTask(backupOldState = false))

      val status = Await.result(statusFuture, 2.seconds)
      status shouldBe Swapped

      val swappedFiles = listFilesInDir(dataDir)
      swappedFiles should contain theSameElementsAs snapshotFiles
  }

  "should successfully swap old state and snapshot (backup = true)" in fixture {
    case FixtureParams(snapshotSwap, snapshotFiles, dataDir, dataFiles, statusFuture) =>
      test(snapshotSwap.asTask(backupOldState = true))

      val status = Await.result(statusFuture, 2.seconds)
      status shouldBe Swapped

      val swappedFiles = listFilesInDir(dataDir)
      swappedFiles should contain theSameElementsAs snapshotFiles

      val backupDir   = dataDir.resolve(SnapshotSwap.BackupDir)
      val backupFiles = listFilesInDir(backupDir)
      backupFiles should contain theSameElementsAs dataFiles
  }
}

object SnapshotSwapSpec {
  private val FileNameLength     = 10
  private val DataFilesCount     = 10
  private val SnapshotFilesCount = 5

  case class FileWithContent(file: Path, bytes: ByteStr)

  private def listFilesInDir(dir: Path): List[FileWithContent] = {
    withResource(Files.list(dir)) { stream =>
      stream
        .iterator()
        .asScala
        .filter { path =>
          !Files.isDirectory(path)
        }
        .map { file =>
          FileWithContent(dir.relativize(file), ByteStr(Files.readAllBytes(file)))
        }
        .toList
    }
  }
}
