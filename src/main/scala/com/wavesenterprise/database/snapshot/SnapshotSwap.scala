package com.wavesenterprise.database.snapshot

import cats.implicits.{catsStdInstancesForEither, catsSyntaxFlatMapOps}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.SnapshotFileSystemError
import com.wavesenterprise.database.snapshot.PackedSnapshot.PackedSnapshotFile
import com.wavesenterprise.utils.ResourceUtils.withResource
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.reactive.Observer

import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.util.Try

class SnapshotSwap(snapshotDir: Path, dataDir: Path, val statusObserver: Observer[SnapshotStatus]) extends ScorexLogging with SnapshotStatusObserver {

  import SnapshotSwap._

  def this(snapshotDir: String, dataDir: String, statusObserver: Observer[SnapshotStatus]) {
    this(Paths.get(snapshotDir), Paths.get(dataDir), statusObserver)
  }

  protected def swap(backupOldState: Boolean): Either[ApiError, Unit] = {
    Either.cond(Files.exists(snapshotDir), (), SnapshotFileSystemError(s"Snapshot directory '$snapshotDir' doesn't exist")) >>
      Either.cond(Files.isDirectory(snapshotDir), (), SnapshotFileSystemError(s"Snapshot path '$snapshotDir' is not a directory")) >>
      Either.cond(Files.list(snapshotDir).findFirst().isPresent, (), SnapshotFileSystemError(s"Snapshot directory '$snapshotDir' is empty")) >>
      Try {
        log.info("Starting to swap old state to snapshot...")

        val backupDirOpt: Option[Path] = if (backupOldState) {
          Some(Files.createDirectory(dataDir.resolve(BackupDir)))
        } else None

        log.info(s"${if (backupOldState) "Backup" else "Delete"} state files from data-directory...")
        withResource(Files.list(dataDir)) { stream =>
          stream
            .iterator()
            .asScala
            .filter { path =>
              !Files.isDirectory(path) && path != snapshotDir
            }
            .foreach { path =>
              backupDirOpt match {
                case Some(backupDir) => Files.move(path, backupDir.resolve(path.getFileName))
                case None            => Files.delete(path)
              }
            }
        }

        log.info("Moving snapshot files to data-directory...")
        withResource(Files.list(snapshotDir)) { stream =>
          stream
            .iterator()
            .asScala
            .filter { path =>
              !Files.isDirectory(path) && path != snapshotDir && path.getFileName.toString != PackedSnapshotFile
            }
            .foreach { path =>
              Files.move(path, dataDir.resolve(path.getFileName))
            }
        }
      }.toEither.left.map { err =>
        log.error("Error while swapping snapshot", err)
        SnapshotFileSystemError(err.getMessage)
      }
  }

  def asTask(backupOldState: Boolean): Task[Either[ApiError, Unit]] =
    Task {
      swap(backupOldState)
    } >> onStatus(Swapped).map(Right(_))
}

object SnapshotSwap {
  val BackupDir = "PreSnapshotBackup"
}
