package com.wavesenterprise.http

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.implicits._
import com.typesafe.config.{ConfigObject, ConfigRenderOptions}
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.http.ApiError.CustomValidationError
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.Administrator
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.consensus.GeneratingBalanceProvider
import com.wavesenterprise.crypto
import com.wavesenterprise.docker.ContractAuthTokenService
import com.wavesenterprise.docker.ContractExecutor.ContractTxClaimContent
import com.wavesenterprise.mining.{Miner, MinerDebugInfo}
import com.wavesenterprise.network._
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.{ApiSettings, WESettings}
import com.wavesenterprise.state.diffs.TransactionDiffer
import com.wavesenterprise.state.{Blockchain, ByteStr, LeaseBalance, NG, Portfolio}
import com.wavesenterprise.transaction.ValidationError.InvalidRequestSignature
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.ExecutedContractTransaction
import com.wavesenterprise.transaction.smart.Verifier
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.utils.{Base58, ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wallet.Wallet
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import play.api.libs.json._

import java.io.File
import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, ThreadInfo}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class DebugApiRoute(ws: WESettings,
                    val time: Time,
                    feeCalculator: FeeCalculator,
                    blockchain: Blockchain,
                    wallet: Wallet,
                    ng: NG,
                    permissionValidator: PermissionValidator,
                    rollbackTask: ByteStr => Task[Either[ValidationError, Seq[Block]]],
                    activePeerConnections: ActivePeerConnections,
                    utxStorage: UtxPool,
                    txBroadcaster: TxBroadcaster,
                    miner: Miner with MinerDebugInfo,
                    historyReplier: HistoryReplier,
                    extLoaderStateReporter: Coeval[BlockLoader.State],
                    mbsCacheSizesReporter: Coeval[MicroBlockLoaderStorage.CacheSizes],
                    syncChannelSelectorReporter: Coeval[SyncChannelSelector.Stats],
                    configRoot: ConfigObject,
                    val nodeOwner: Address,
                    maybeContractAuthTokenService: Option[ContractAuthTokenService],
                    val scheduler: Scheduler,
                    freezeApp: () => Unit)
    extends ApiRoute
    with AdditionalDirectiveOps
    with ScorexLogging {

  import DebugApiRoute._

  private lazy val configStr           = configRoot.render(ConfigRenderOptions.concise().setJson(true).setFormatted(true))
  private lazy val fullConfig: JsValue = Json.parse(configStr)
  private lazy val weConfig: JsObject  = Json.obj("node" -> (fullConfig \ "node").get)

  override val settings: ApiSettings = ws.api

  protected def userAuth          = withAuth()
  protected def adminAuthOrApiKey = withAuth(ApiKeyProtection, Administrator)

  override lazy val route: Route = pathPrefix("debug") {
    addedGuard {
      userAuth(validate) ~ adminAuthOrApiKey {
        blocks ~ state ~ info ~ stateWE ~ rollback ~ rollbackTo ~ portfolios ~ minerInfo ~ historyInfo ~ configInfo ~ print ~ freeze ~ cleanState ~ createAuthForGrpcContract ~
          threadDump ~ utxRebroadcast
      }
    }
  }

  /**
    * GET /debug/blocks/{howMany}
    *
    * Get sizes and full hashes for last blocks
    **/
  def blocks: Route = {
    (path("blocks" / Segment) & get) { howManyStr =>
      PositiveInt(howManyStr).processRoute { howMany =>
        complete(JsArray(ng.lastBlocks(howMany).map { block =>
          val bytes = block.bytes()
          Json.obj(bytes.length.toString -> Base58.encode(crypto.fastHash(bytes)))
        }))
      }
    }
  }

  /**
    * POST /debug/print
    *
    * Prints a string at DEBUG level, strips to 250 chars
    **/
  def print: Route = (path("print") & post) {
    json[DebugMessage] { params =>
      log.debug(params.message.take(250))
      ""
    }
  }

  /**
    * GET /debug/portfolios/{address}
    *
    * Get current portfolio considering pessimistic transactions in the UTX pool
    **/
  def portfolios: Route = path("portfolios" / Segment) { rawAddress =>
    (get & parameter('considerUnspent.as[Boolean].?)) { considerUnspent =>
      complete {
        Address
          .fromString(rawAddress)
          .map { address =>
            val portfolio = if (considerUnspent.getOrElse(true)) utxStorage.portfolio(address) else ng.addressPortfolio(address)
            Json.toJson(portfolio)
          }
          .leftMap(ApiError.fromCryptoError)
      }
    }
  }

  /**
    * GET /debug/state
    **/
  def state: Route = (path("state") & get) {
    complete(ng.addressWestDistribution(ng.height).map { case (a, b) => a.stringRepr -> b })
  }

  /**
    * GET /debug/stateWE/{height}
    *
    * Get state at specified height
    **/
  def stateWE: Route = (path("stateWE" / IntNumber) & get) { height =>
    complete(ng.addressWestDistribution(height).map { case (a, b) => a.stringRepr -> b })
  }

  private def rollbackToBlock(blockId: ByteStr, returnTransactionsToUtx: Boolean): Future[ToResponseMarshallable] = {
    import monix.execution.Scheduler.Implicits.global

    rollbackTask(blockId).asyncBoundary.map {
      case Right(blocks) =>
        activePeerConnections.broadcast(LocalScoreChanged(ng.score))

        if (returnTransactionsToUtx) {
          blocks
            .flatMap(_.transactionData)
            .map {
              case ect: ExecutedContractTransaction =>
                ect.tx

              case atomicTx: AtomicTransaction =>
                AtomicUtils.rollbackExecutedTxs(atomicTx)

              case tx: Transaction =>
                tx
            }
            .foreach(utxStorage.putIfNew(_, None)) // todo: add certs
        }

        miner.scheduleNextMining()
        Json.obj("BlockId" -> blockId.toString): ToResponseMarshallable
      case Left(error) => ApiError.fromValidationError(error): ToResponseMarshallable
    }.runAsyncLogErr
  }

  /**
    * POST /debug/rollback
    *
    * Removes all blocks after given height
    **/
  def rollback: Route = (path("rollback") & post & withRequestTimeout(15.minutes)) {
    json[RollbackParams] { params =>
      ng.blockAt(params.rollbackTo) match {
        case Some(block) =>
          rollbackToBlock(block.uniqueId, params.returnTransactionsToUtx)
        case None =>
          (StatusCodes.BadRequest, "Block at height not found")
      }
    } ~ complete(StatusCodes.BadRequest)
  }

  /**
    * GET /debug/info
    **/
  def info: Route = (path("info") & get) {
    complete(
      Json.obj(
        "stateHeight"                      -> ng.height,
        "extensionLoaderState"             -> extLoaderStateReporter().toString,
        "historyReplierCacheSizes"         -> Json.toJson(historyReplier.cacheSizes),
        "microBlockSynchronizerCacheSizes" -> Json.toJson(mbsCacheSizesReporter()),
        "scoreObserverStats"               -> syncChannelSelectorReporter().json,
        "minerState"                       -> Json.toJson(miner.state)
      ))
  }

  /**
    * GET /debug/minerInfo
    **/
  def minerInfo: Route = (path("minerInfo") & get) {
    complete(
      wallet.privateKeyAccounts
        .filterNot(account => ng.hasScript(account.toAddress))
        .map { account =>
          (account.toAddress, miner.getNextBlockGenerationOffset(account))
        }
        .collect {
          case (address, Right(offset)) =>
            AccountMiningInfo(
              address.stringRepr,
              ng.effectiveBalance(address, ng.height, GeneratingBalanceProvider.BalanceDepth),
              System.currentTimeMillis() + offset.toMillis
            )
        }
    )
  }

  /**
    * GET /debug/historyInfo
    **/
  def historyInfo: Route = (path("historyInfo") & get) {
    val a = ng.lastPersistedBlockIds(10)
    val b = ng.microblockIds
    complete(HistoryInfo(a, b))

  }

  /**
    * GET /debug/configInfo
    **/
  def configInfo: Route = (path("configInfo") & get & parameter('full.as[Boolean] ? false)) { full =>
    complete(if (full) fullConfig else weConfig)
  }

  /**
    * DELETE /debug/rollback-to/{signature}
    *
    * Rollback the state to the block with a given signature
    **/
  def rollbackTo: Route = (path("rollback-to" / Segment) & delete) { signature =>
    val signatureEi: Either[ValidationError, ByteStr] =
      ByteStr
        .decodeBase58(signature)
        .toEither
        .leftMap(_ => InvalidRequestSignature)
    signatureEi
      .fold(
        err => complete(ApiError.fromValidationError(err)),
        sig => complete(rollbackToBlock(sig, returnTransactionsToUtx = false))
      )
  }

  /**
    * POST /debug/validate
    *
    * Validates a transaction and measures time spent in milliseconds
    **/
  def validate: Route = (path("validate") & post) {
    json[JsObject] { jsv =>
      val h  = blockchain.height
      val t0 = System.nanoTime

      val diffEi = for {
        tx <- TransactionFactory.fromSignedRequest(jsv)
        _  <- feeCalculator.validateTxFee(blockchain.height, tx)
        _  <- Verifier(blockchain, h)(tx)
        // todo: add certs
        ei <- TransactionDiffer(
          ws.blockchain,
          permissionValidator,
          blockchain.lastBlockTimestamp,
          time.correctedTime(),
          h,
          ws.utx.txExpireTimeout
        )(blockchain, tx, None)
      } yield ei
      val timeSpent = (System.nanoTime - t0) / 1000 / 1000.0
      val response  = Json.obj("valid" -> diffEi.isRight, "validationTime" -> timeSpent)
      diffEi.fold(err => response + ("error" -> JsString(ApiError.fromValidationError(err).message)), _ => response)
    }
  }

  /**
    * POST /debug/freeze
    *
    * Freeze node working process. Stop mining, stop RocksDB, stop everything.
    **/
  def freeze: Route = {
    (path("freeze") & post) {
      log.info("Request to freeze application")
      freezeApp()
      complete(Json.obj("freezed" -> true))
    }
  }

  def cleanState: Route = {
    (path("cleanState") & post) {
      log.info("Request to clean state (full RocksDB state removing)")

      freezeApp()
      val removedFilesCount = removeFilesInRocksDB()

      complete(Json.obj("freezed" -> true, "rocksdb_files_removed" -> removedFilesCount))
    }
  }

  /**
    * POST /debug/createGrpcAuth
    *
    * Returns token for readonly gRPC methods (with empty 'txId' and 'executionId' fields)
    **/
  def createAuthForGrpcContract: Route = {
    (path("createGrpcAuth") & post) {
      json[ContractAuthRequest] { request =>
        for {
          contractAuthTokenService <- maybeContractAuthTokenService.toRight(CustomValidationError("Can't create token – docker-engine is disabled"))
          claimContent = ContractTxClaimContent(ByteStr.empty, request.contractId)
          token        = contractAuthTokenService.create(claimContent, ContractAuthTokenExpire)
        } yield ContractAuthResponse(token)
      }
    }
  }

  /**
    * GET /debug/threadDump
    *
    * Returns formatted thread dump
    */
  def threadDump: Route = (path("threadDump") & get) {
    complete(ManagementFactory.getThreadMXBean.dumpAllThreads(true, true))
  }

  private def removeFilesInRocksDB(): Int = {
    var filesRemoved  = 0
    val dataDirectory = new File(ws.dataDirectory)
    if (dataDirectory.exists() && dataDirectory.isDirectory) {
      val dataFiles = dataDirectory.listFiles()
      for (i <- Range(0, dataFiles.length)) {
        val dataFile = dataFiles(i)
        if (dataFile.isFile) {
          log.debug(s"removing RocksDB file '${dataFile.getName}'")
          if (dataFile.delete()) {
            filesRemoved += 1
          }
        }
      }
    }
    filesRemoved
  }

  /**
    * GET /debug/utx-rebroadcast
    *
    * Returns number of rebroadcasted transactions
    */
  def utxRebroadcast: Route = (path("utx-rebroadcast") & get) {
    val utxTxsWithCerts    = utxStorage.selectTransactionsWithCerts(_ => true)
    val numberOfTxs        = utxTxsWithCerts.length
    def txIds: Seq[String] = utxTxsWithCerts.map(_.tx.id().toString)
    complete {
      utxTxsWithCerts.toList
        .traverse(txWithCert => txBroadcaster.forceBroadcast(txWithCert.tx, txWithCert.maybeCerts))
        .bimap(
          ApiError.fromCryptoError,
          _ => {
            log.debug(s"Number of rebroadbcasted transactions from utx: $numberOfTxs")
            log.trace(s"Ids of rebroadcasted transactions from utx: [${txIds.mkString("; ")}]")
            Json.obj(("txsToBroadcast" -> s"$numberOfTxs"))
          }
        )
        .value
        .runToFuture(scheduler)
    }
  }
}

object DebugApiRoute {
  implicit val assetsFormat: Format[Map[ByteStr, Long]] = Format[Map[ByteStr, Long]](
    {
      case JsObject(m) =>
        m.foldLeft[JsResult[Map[ByteStr, Long]]](JsSuccess(Map.empty)) {
          case (e: JsError, _) => e
          case (JsSuccess(m, _), (rawAssetId, JsNumber(count))) =>
            (ByteStr.decodeBase58(rawAssetId), count) match {
              case (Success(assetId), count) if count.isValidLong => JsSuccess(m.updated(assetId, count.toLong))
              case (Failure(_), _)                                => JsError(s"Can't parse '$rawAssetId' as base58 string")
              case (_, count)                                     => JsError(s"Invalid count of assets: $count")
            }
          case (_, (_, rawCount)) =>
            JsError(s"Invalid count of assets: $rawCount")
        }
      case _ => JsError("The map is expected")
    },
    m => Json.toJson(m.map { case (assetId, count) => assetId.base58 -> count })
  )
  implicit val leaseInfoFormat: Format[LeaseBalance] = Json.format
  implicit val portfolioFormat: Format[Portfolio]    = Json.format

  case class AccountMiningInfo(address: String, miningBalance: Long, timestamp: Long)

  implicit val accountMiningBalanceFormat: Format[AccountMiningInfo] = Json.format

  implicit val addressWrites: Format[Address] = new Format[Address] {
    override def writes(o: Address): JsValue = JsString(o.stringRepr)

    override def reads(json: JsValue): JsResult[Address] = ???
  }

  case class HistoryInfo(lastBlockIds: Seq[BlockId], microBlockIds: Seq[BlockId])

  implicit val historyInfoFormat: Format[HistoryInfo] = Json.format

  implicit val hrCacheSizesFormat: Format[HistoryReplier.CacheSizes]           = Json.format
  implicit val mbsCacheSizesFormat: Format[MicroBlockLoaderStorage.CacheSizes] = Json.format
  implicit val BigIntWrite: Writes[BigInt]                                     = (bigInt: BigInt) => JsNumber(BigDecimal(bigInt))

  import MinerDebugInfo._

  implicit val minerStateWrites: Writes[MinerDebugInfo.State] = (s: MinerDebugInfo.State) =>
    JsString(s match {
      case MiningBlocks      => "mining blocks"
      case MiningMicroblocks => "mining microblocks"
      case Disabled          => "disabled"
      case Error(err)        => s"error: $err"
    })

  case class ContractAuthRequest(contractId: ByteStr)
  implicit val ContractAuthRequestFormat: Format[ContractAuthRequest] = Json.format

  case class ContractAuthResponse(token: String)
  implicit val ContractAuthResponseFormat: Format[ContractAuthResponse] = Json.format

  private val ContractAuthTokenExpire = 365 days

  implicit val ThreadInfoWrites: OWrites[ThreadInfo] = OWrites {
    Option(_).fold(JsObject.empty) { ti =>
      Json.obj(
        "threadName"          -> ti.getThreadName,
        "threadId"            -> ti.getThreadId,
        "blockedTime"         -> ti.getBlockedTime,
        "blockedCount"        -> ti.getBlockedCount,
        "waitedTime"          -> ti.getWaitedTime,
        "lockName"            -> ti.getLockName,
        "lockOwnerId"         -> ti.getLockOwnerId,
        "lockOwnerName"       -> ti.getLockOwnerName,
        "inNative"            -> ti.isInNative,
        "suspended"           -> ti.isSuspended,
        "threadState"         -> ti.getThreadState.name(),
        "stackTrace"          -> ti.getStackTrace,
        "lockedMonitors"      -> ti.getLockedMonitors,
        "lockedSynchronizers" -> ti.getLockedSynchronizers,
        "lockInfo"            -> ti.getLockInfo
      )
    }
  }

  implicit val StackTraceElementWrites: OWrites[StackTraceElement] = OWrites {
    Option(_).fold(JsObject.empty) { ste =>
      Json.obj(
        "methodName"   -> ste.getMethodName,
        "fileName"     -> ste.getFileName,
        "lineNumber"   -> ste.getLineNumber,
        "className"    -> ste.getClassName,
        "nativeMethod" -> ste.isNativeMethod
      )
    }
  }

  implicit val MonitorInfoWrites: OWrites[MonitorInfo] = OWrites {
    Option(_).fold(JsObject.empty) { mi =>
      Json.obj(
        "className"        -> mi.getClassName,
        "identityHashCode" -> mi.getIdentityHashCode,
        "lockedStackFrame" -> mi.getLockedStackFrame,
        "lockedStackDepth" -> mi.getLockedStackDepth
      )
    }
  }

  implicit val LockInfoWrites: OWrites[LockInfo] = OWrites {
    Option(_).fold(JsObject.empty) { li =>
      Json.obj(
        "className"        -> li.getClassName,
        "identityHashCode" -> li.getIdentityHashCode
      )
    }
  }
}
