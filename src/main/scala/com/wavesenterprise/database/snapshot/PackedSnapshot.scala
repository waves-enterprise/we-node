package com.wavesenterprise.database.snapshot

import cats.data.EitherT
import cats.implicits._
import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidSignature}
import com.wavesenterprise.transaction.{Signed, ValidationError}
import com.wavesenterprise.utils.DatabaseUtils.ByteArrayDataInputExt
import com.wavesenterprise.utils.{Base58, ScorexLogging, ZipUtils}
import monix.eval.{Coeval, Task}

import java.io.FileInputStream
import java.nio.file._
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}
import scala.collection.JavaConverters._
import scala.util.Try

object PackedSnapshot extends ScorexLogging {

  val PackedSnapshotFile = "snapshot.zip"

  def packSnapshot(snapshotDirectory: String, ownerKey: PrivateKeyAccount): Task[Path] = Task.defer {
    val snapshotDir  = Paths.get(snapshotDirectory)
    val snapshotFile = snapshotDir.resolve(PackedSnapshotFile)

    for {
      exists <- Task(Files.exists(snapshotFile))
      _ <- if (exists) {
        Task(log.debug(s"Packed snapshot '$snapshotFile' already exists"))
      } else {
        packSnapshot(snapshotDir, Files.createFile(snapshotFile)) >> signZip(snapshotFile, ownerKey)
      }
    } yield snapshotFile
  }

  protected[snapshot] def packSnapshot(snapshotDir: Path, snapshotFile: Path): Task[Unit] = {
    Task(log.debug(s"Starting to pack snapshot files to '$snapshotFile'...")) >> Task
      .eval(Files.walk(snapshotDir))
      .bracketE { stream =>
        packFiles(stream.iterator().asScala, snapshotDir, snapshotFile)
      } {
        case (stream, Right(_)) => Task { Try(stream.close()) }
        case (stream, Left(_)) =>
          Task {
            Try(stream.close())
            Files.delete(snapshotFile)
          }
      } >> Task(log.debug("Snapshot files were packed successfully"))
  }

  private def packFiles(files: Iterator[Path], snapshotDir: Path, snapshotFile: Path): Task[Unit] = {
    Task
      .eval(new ZipOutputStream(Files.newOutputStream(snapshotFile)))
      .bracket { zipOS =>
        Task {
          files
            .filter { path =>
              !Files.isDirectory(path) && path != snapshotFile
            }
            .foreach { path =>
              log.debug(s"Packing snapshot file '$path'...")
              val zipEntry = new ZipEntry(snapshotDir.relativize(path).toString)
              zipOS.putNextEntry(zipEntry)
              Files.copy(path, zipOS)
              zipOS.closeEntry()
            }
          zipOS.flush()
        }
      } { os =>
        Task(os.close())
      }
  }

  private def signZip(snapshotFile: Path, signer: PrivateKeyAccount): Task[Unit] = {
    Task(log.debug(s"Signing snapshot archive '$snapshotFile' ...")) >>
      ZipUtils
        .getFileChecksum(snapshotFile)
        .flatMap { checksum =>
          Task {
            val signature = crypto.sign(signer, checksum)
            ZipSignature(signature, signer)
          }
        }
        .flatMap { zipSignature =>
          Task(new ZipOutputStream(Files.newOutputStream(snapshotFile, StandardOpenOption.APPEND)))
            .bracket { zipOs =>
              Task {
                zipOs.setComment(zipSignature.base58)
                zipOs.flush()
              }
            } { os =>
              Task(os.close())
            }
        }
        .doOnFinish {
          case None => Task(log.debug(s"Snapshot archive '$snapshotFile' has been signed"))
          case _    => Task(log.error(s"Snapshot archive signing has been failed"))
        }
  }

  /**
    * Unpack to the same directory that 'snapshot.zip' file is in
    */
  def unpackSnapshot(snapshotDirectory: String): Task[Either[ValidationError, Unit]] =
    unpackSnapshot(snapshotDirectory, snapshotDirectory)

  def unpackSnapshot(snapshotDirectory: String, targetDirectory: String): Task[Either[ValidationError, Unit]] = Task.defer {
    val snapshotDir  = Paths.get(snapshotDirectory)
    val snapshotFile = snapshotDir.resolve(PackedSnapshotFile)
    val targetDir    = Paths.get(targetDirectory)

    (EitherT.cond[Task](snapshotFile.toFile.exists(), (), GenericError(s"Snapshot file not found in path '$snapshotFile'"): ValidationError) *>
      EitherT(verifySnapshotZip(snapshotFile)) *>
      EitherT.right[ValidationError](unpackSnapshot(snapshotFile, targetDir))).value
  }

  private[snapshot] def verifySnapshotZip(snapshotFile: Path): Task[Either[ValidationError, Unit]] = {
    (for {
      eocdInfo     <- EitherT(ZipUtils.getEOCDInfo(snapshotFile))
      checksum     <- EitherT.right(ZipUtils.getFileChecksum(snapshotFile, Some(eocdInfo.startPosition)))
      zipSignature <- EitherT(decodeZipSignature(eocdInfo.comment))
      checksumSigned = SignedChecksum(checksum, zipSignature.signer, zipSignature.checksumSignature)
      _ <- EitherT.cond[Task].apply[ValidationError, Unit](checksumSigned.signatureValid(), (), InvalidSignature(checksumSigned))
    } yield ()).value
  }

  private def decodeZipSignature(zipComment: String): Task[Either[ValidationError, ZipSignature]] = {
    Task {
      Base58.decode(zipComment).toEither.left.map(GenericError(_)).flatMap(ZipSignature.fromBytes)
    }
  }

  protected[snapshot] def unpackSnapshot(snapshotFile: Path, targetDir: Path): Task[Unit] = {
    Task(log.debug(s"Starting to unpack snapshot files to '$targetDir'...")) >>
      Task
        .eval(new ZipInputStream(new FileInputStream(snapshotFile.toFile)))
        .bracket { stream =>
          Task {
            Iterator.continually(stream.getNextEntry).takeWhile(_ != null).foreach { entry =>
              val file = targetDir.resolve(entry.getName)
              Files.copy(stream, file, StandardCopyOption.REPLACE_EXISTING)
              stream.closeEntry()
            }
          }
        } { stream =>
          Task(Try(stream.close()))
        } >> Task(log.debug("Snapshot files were unpacked successfully"))
  }
}

case class ZipSignature(checksumSignature: Array[Byte], signer: PublicKeyAccount) {
  def toBytes: Array[Byte] = {
    //noinspection UnstableApiUsage
    val output = newDataOutput()
    output.write(ZipSignature.Version)
    output.write(ZipSignature.CryptoByte)
    output.write(signer.publicKey.getEncoded)
    output.write(checksumSignature)
    output.toByteArray
  }

  def base58: String = Base58.encode(toBytes)
}

object ZipSignature {
  val CryptoByte: Byte = 0

  private val Version: Byte = 1
  private val BytesLength   = 1 + 1 + crypto.KeyLength + crypto.SignatureLength

  def fromBytes(bytes: Array[Byte]): Either[ValidationError, ZipSignature] = {
    //noinspection UnstableApiUsage
    for {
      _ <- Either.cond(bytes.length == BytesLength, (), GenericError(s"Invalid bytes length '${bytes.length}', expected: '$BytesLength'"))
      in        = newDataInput(bytes)
      versionIn = in.readByte()
      _ <- Either.cond(versionIn == Version, (), GenericError(s"Unsupported zip signature Version '$versionIn'"))
      cryptoByteIn = in.readByte()
      _      <- Either.cond(cryptoByteIn == CryptoByte, (), GenericError(s"Unexpected crypto type: '$cryptoByteIn'"))
      signer <- PublicKeyAccount.fromBytes(in.readBytes(crypto.KeyLength)).leftMap(ValidationError.fromCryptoError)
      signature = in.readBytes(crypto.SignatureLength)
    } yield ZipSignature(signature, signer)
  }
}

case class SignedChecksum(checksum: Array[Byte], sender: PublicKeyAccount, signature: Array[Byte]) extends Signed {
  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce(crypto.verify(signature, checksum, sender.publicKey))
}
