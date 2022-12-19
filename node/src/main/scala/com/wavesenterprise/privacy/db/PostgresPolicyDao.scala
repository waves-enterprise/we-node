package com.wavesenterprise.privacy.db

import cats.syntax.option._
import com.wavesenterprise.crypto.util.Sha256Hash
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyMetaData}
import com.wavesenterprise.privacy.db.largeobject.LargeObjectStreamingDBIOAction
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.Base58
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.postgresql.PGConnection
import org.postgresql.largeobject.LargeObjectManager
import org.reactivestreams.Publisher
import slick.dbio.FutureAction
import slick.jdbc.JdbcProfile

import java.sql.{Connection, SQLException}
import scala.util.chaining.scalaUtilChainingOps
import scala.util.control.NonFatal

case class DbAsyncTaskError(validationError: ValidationError) extends Exception

class PostgresPolicyDao(
    private val policyDataDBIO: PolicyDataDBIO,
    private val policyMetaDataDBIO: PolicyMetaDataDBIO,
    private val db: slick.jdbc.JdbcBackend#Database,
    private val driver: JdbcProfile
) extends CommonSlickTrait
    with AutoCloseable {
  import driver.api._

  def isLargeObject(policyId: String, hash: String): Task[Either[DBError, Option[Boolean]]] =
    db.task {
      policyMetaDataDBIO.isLargeObject(policyId, hash)
    }

  def deletePolicy(
      policyId: ByteStr,
      hash: String
  )(implicit s: Scheduler): Task[DBResult[Int]] =
    db.task((for {
      delMeta <- policyMetaDataDBIO.delete(policyId.base58, hash)
      delData <- policyDataDBIO.delete(policyId.base58, hash)
    } yield delMeta + delData).transactionally)

  def policyHashes(policyId: String): Task[DBResult[Seq[String]]] =
    db.task(policyMetaDataDBIO.policyHashes(policyId))

  def policyItemMeta(policyId: String, hash: String): Task[DBResult[Option[PolicyMetaData]]] =
    db.task(policyMetaDataDBIO.policyItemMeta(policyId, hash))

  def policyItemsMetas(policyIds: Set[String], hashes: Set[String]): Task[DBResult[Seq[PolicyMetaData]]] =
    db.task(policyMetaDataDBIO.policyItemsMetas(policyIds, hashes))

  def insertMeta(policyMetaData: PolicyMetaData): Task[DBResult[Int]] =
    db.task(policyMetaDataDBIO.insert(policyMetaData))

  def insertPolicyData(policyData: ByteStr, policyMeta: PolicyMetaData)(implicit s: Scheduler): Task[DBResult[Int]] =
    db.task((for {
      _                 <- validateHashes(PolicyDataHash.fromDataBytes(policyData.arr).toString, policyMeta.hash)
      insertedMetaCount <- policyMetaDataDBIO.insert(policyMeta)
      insertedDataCount <- policyDataDBIO.insert(policyMeta.policyId, policyMeta.hash, policyData)
    } yield insertedMetaCount + insertedDataCount).transactionally)

  def insertPolicyDataStream(
      policyDataStream: Observable[Array[Byte]],
      policyMeta: PolicyMetaData
  )(implicit s: Scheduler): Task[DBResult[Int]] =
    db.task(
      (for {
        (loId, hash) <- uploadLargeObject(policyDataStream)
        _            <- validateHashes(Base58.encode(hash), policyMeta.hash)
        metaWithLoId = policyMeta.copy(loId = loId.some)
        n <- policyMetaDataDBIO.insert(metaWithLoId)
      } yield n).transactionally
    )

  def allMeta(): Task[DBResult[Seq[PolicyMetaData]]] = db.task {
    policyMetaDataDBIO.all()
  }

  def allData(): Task[DBResult[Seq[PolicyData]]] = db.task {
    policyDataDBIO.all()
  }

  def findItemData(policyId: String, dataHash: String): Task[DBResult[Option[ByteStr]]] =
    db.task(policyDataDBIO.find(policyId, dataHash))

  def findItemDataAsSource(
      policyId: String,
      dataHash: String
  )(implicit s: Scheduler): Task[DBResult[Option[Observable[Array[Byte]]]]] =
    db.task(policyMetaDataDBIO.policyItemMeta(policyId, dataHash) map {
      case Some(meta) =>
        meta.loId match {
          case Some(loId) =>
            downloadLargeObject(loId)
              .pipe(publisher => Observable.fromReactivePublisher(publisher))
              .some
          case None => None
        }
      case None => None
    })

  def itemMetaDataExists(policyId: String, dataHash: String): Task[DBResult[Boolean]] =
    db.task(policyMetaDataDBIO.exists(policyId, dataHash))

  def deleteAllMeta(): Task[DBResult[Int]] =
    db.task(policyMetaDataDBIO.deleteAll())

  def deleteAllData(): Task[DBResult[Int]] =
    db.task(policyDataDBIO.deleteAll())

  def insertData(policyId: String, hash: String, data: ByteStr): Task[DBResult[Int]] =
    db.task(policyDataDBIO.insert(policyId, hash, data))

  def healthCheck: Task[DBResult[Unit]] =
    db.task(sql"SELECT 1".as[Int].head).map(_.map(_ => ()))

  private val errorMsgPrefix = "Error while uploading Large Object"

  /**
    * @param data - Observable for upload to db
    * @return OID for uploaded LargeObject and calculated SHA256 hash of data
    */
  private def uploadLargeObject(data: Observable[Array[Byte]])(implicit s: Scheduler): DBIO[(Long, Array[Byte])] = {
    FutureAction {
      @volatile var connection: Connection = null
      Task {
        connection = db.source.createConnection()
        val autoCommit = connection.getAutoCommit
        connection.setAutoCommit(false)
        val largeObjectApi = connection.unwrap(classOf[PGConnection]).getLargeObjectAPI
        val largeObjectId  = largeObjectApi.createLO()
        val largeObject    = largeObjectApi.open(largeObjectId, LargeObjectManager.WRITE)
        (connection, autoCommit, largeObjectId, largeObject)
      }.flatMap {
        case (connection, autoCommit, largeObjectId, largeObject) =>
          data
            .foldLeft(Sha256Hash()) {
              case (hash, chunk) =>
                largeObject.write(chunk)
                hash.update(chunk)
            }
            .map(largeObjectId -> _.result())
            .headL
            .guarantee {
              Task {
                largeObject.close()
                connection.setAutoCommit(autoCommit)
              }.onErrorRecoverWith {
                case e: SQLException =>
                  log.error(s"$errorMsgPrefix setAutoCommit($autoCommit): '${e.getMessage}'")
                  Task.raiseError(e)
                case NonFatal(e) =>
                  log.error(s"$errorMsgPrefix: '${e.getMessage}'")
                  Task.raiseError(e)
              }
            }
            .onErrorRecoverWith {
              case e: Throwable =>
                log.error(s"$errorMsgPrefix: '${e.getMessage}'")
                Task.raiseError(e)
            }
      }
        .guarantee {
          Task {
            if (connection != null && !connection.isClosed) {
              connection.close()
            }
          }
        }
        .runToFuture
    }
  }

  /**
    * @param oid - LargeObject OID
    * @return akka stream representing LargeObject from db
    */
  private def downloadLargeObject(oid: Long): Publisher[Array[Byte]] =
    LargeObjectStreamingDBIOAction(oid)
      .pipe(action => db.stream(action.transactionally))

  private def validateHashes(actual: String, expected: String): DBIOAction[Unit, NoStream, Effect] = {
    if (actual == expected) {
      DBIO.successful(())
    } else {
      DBIO.failed(DbAsyncTaskError(ValidationError.InvalidPolicyDataHash(s"Invalid data hash. Actual '$actual', expected '$expected'")))
    }
  }

  override def close(): Unit =
    db.close()
}
