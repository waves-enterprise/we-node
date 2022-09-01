package com.wavesenterprise.api.http.service

import cats.data.EitherT
import cats.implicits.{showInterpolator, _}
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, ForceSyncError, PolicyItemDataIsMissing}
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.parseCertChain
import com.wavesenterprise.database.PrivacyLostItemUpdater
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.metrics.privacy.PrivacyMeasurementType._
import com.wavesenterprise.metrics.privacy.PrivacyMetrics
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.privacy.PolicyItemTypeSelector.PolicyItemTypeSelectorError.{ItemNotFoundError, StorageUnavailableError}
import com.wavesenterprise.network.privacy.{PolicyDataSynchronizer, PolicyInventoryBroadcaster, PolicyItemTypeSelector}
import com.wavesenterprise.network.{TransactionWithSize, TxBroadcaster}
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.privacy._
import com.wavesenterprise.state.diffs.{PolicyDataHashTransactionDiff, PolicyDiff}
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.wallet.Wallet
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable

import scala.concurrent.Future
import scala.util.Try

object PrivacyApiService {
  // each char is 8 bytes, so we skip multiplication by 8
  final val maxPolicyDataStringLength: Int = 1024 * 1024 * 34 // string ~ 20 MB

  def decodePolicyId(policyId: String): Either[GenericError, ByteStr] =
    ByteStr.decodeBase58(policyId).toEither.leftMap(_ => GenericError(s"Failed to decode policyId: '$policyId'"))

  case class ValidSendDataSetup(
      policyId: ByteStr,
      signer: PrivateKeyAccount,
      dataHash: PolicyDataHash,
      feeAssetId: Option[AssetId],
      atomicBadge: Option[AtomicBadge],
      certChain: Option[CertChain]
  )
}

class PrivacyApiService(val state: Blockchain with PrivacyLostItemUpdater,
                        wallet: Wallet,
                        owner: PrivateKeyAccount,
                        val storage: PolicyStorage,
                        policyDataSynchronizer: PolicyDataSynchronizer,
                        feeCalculator: FeeCalculator,
                        time: Time,
                        txBroadcaster: TxBroadcaster)(implicit scheduler: Scheduler)
    extends PolicyInventoryBroadcaster
    with PolicyItemTypeSelector
    with ScorexLogging {

  import PrivacyApiService._
  private case class AddressWithPolicy(address: Address, policyId: ByteStr)

  def policyRecipients(policyIdStr: String): Either[ValidationError, List[String]] = {
    checkPolicyExists(policyIdStr).map { policyId =>
      state.policyRecipients(policyId).map(_.stringRepr).toList
    }
  }

  def policyRecipients(policyId: ByteStr): Either[ValidationError, List[Address]] = {
    PolicyDiff.checkPolicyExistence(policyId, state).map { _ =>
      state.policyRecipients(policyId).toList
    }
  }

  def policyOwners(policyIdStr: String): Either[ValidationError, List[String]] = {
    checkPolicyExists(policyIdStr).map { policyId =>
      state.policyOwners(policyId).map(_.stringRepr).toList
    }
  }

  def policyOwners(policyId: ByteStr): Either[ValidationError, List[Address]] = {
    PolicyDiff.checkPolicyExistence(policyId, state).map { _ =>
      state.policyOwners(policyId).toList
    }
  }

  def policyHashes(policyIdStr: String): Either[ValidationError, Set[String]] = {
    for {
      policyId   <- checkPolicyExists(policyIdStr)
      dataHashes <- Either.fromTry(Try(state.policyDataHashes(policyId).map(_.stringRepr))).leftMap(GenericError.apply)
    } yield dataHashes
  }

  def policyHashes(policyId: ByteStr): Either[ValidationError, Set[PolicyDataHash]] = {
    for {
      _          <- PolicyDiff.checkPolicyExistence(policyId, state)
      dataHashes <- Either.fromTry(Try(state.policyDataHashes(policyId))).leftMap(GenericError.apply)
    } yield dataHashes
  }

  def policyItemTypeOrError(policyId: ByteStr, dataHash: PolicyDataHash): EitherT[Task, ApiError, PrivacyDataType] = {
    val result = policyItemType(policyId, dataHash).map(Right(_)).onErrorRecover {
      case _: ItemNotFoundError                 => Left(ApiError.PolicyItemNotFound(policyId.toString(), dataHash.stringRepr))
      case StorageUnavailableError(_, _, error) => Left(error)
    }

    EitherT(result)
  }

  def policyItemData(policyIdStr: String, policyItemHash: String): Future[Either[ApiError, ByteStr]] = {
    (checkPolicyExists(policyIdStr), PolicyDataHash.fromBase58String(policyItemHash)).tupled match {
      case Right((policyId, dataHash)) =>
        policyItemTypeOrError(policyId, dataHash)
          .flatMapF {
            case PrivacyDataType.Default =>
              Task.fromFuture(processFindResult(policyId, dataHash) {
                storage.policyItemData(policyIdStr, policyItemHash)
              })
            case PrivacyDataType.Large =>
              Task.pure(
                Left(
                  ApiError.WrongPolicyItemDataGetMethod(
                    s"Policy '$policyIdStr' item '$policyItemHash' data size is too big, you should use 'getPolicyItemDataLarge' for grpc and " +
                      s"'policyItemDataLarge' for rest methods instead")))
          }
          .value
          .runToFuture

      case Left(error) =>
        Future.successful(Left(ApiError.fromValidationError(error)))
    }
  }

  def policyItemLargeData(policyIdStr: String, policyItemHash: String): Future[Either[ApiError, Observable[Array[Byte]]]] = {
    (checkPolicyExists(policyIdStr), PolicyDataHash.fromBase58String(policyItemHash)).tupled match {
      case Right((policyId, dataHash)) =>
        policyItemTypeOrError(policyId, dataHash)
          .flatMapF {
            case PrivacyDataType.Large =>
              Task.fromFuture(processFindResult(policyId, dataHash) {
                storage.policyItemDataStream(policyIdStr, policyItemHash)
              })
            case PrivacyDataType.Default =>
              Task.pure(
                Left(
                  ApiError.WrongPolicyItemDataGetMethod(
                    s"Policy '$policyIdStr' item '$policyItemHash' data size is small, you should use 'getPolicyItemData' for grpc and " +
                      s"'policyItemData' for rest methods instead")))
          }
          .value
          .runToFuture

      case Left(error) =>
        Future.successful(Left(ApiError.fromValidationError(error)))
    }
  }

  def policyItemInfo(policyIdStr: String, policyItemHash: String): Future[Either[ApiError, PolicyItemInfo]] = {
    (checkPolicyExists(policyIdStr), PolicyDataHash.fromBase58String(policyItemHash)).tupled match {
      case Right((policyId, dataHash)) =>
        processFindResult(policyId, dataHash) {
          storage.policyItemMeta(policyIdStr, policyItemHash)
        } map { metaDataOrError =>
          metaDataOrError.map(policyMetaItemInfo)
        }

      case Left(error) =>
        Future.successful(Left(ApiError.fromValidationError(error)))
    }
  }

  /**
    * Starts force sync when a lost item is detected
    */
  private def processFindResult[T](policyId: ByteStr, dataHash: PolicyDataHash)(
      findTask: Task[Either[ApiError, Option[T]]]
  ): CancelableFuture[Either[ApiError, T]] = {
    EitherT(findTask)
      .flatMap {
        case None =>
          EitherT.left[T] {
            Task(state.forceUpdateIfNotInProgress(PolicyDataId(policyId, dataHash), owner.toAddress))
              .as[ApiError](PolicyItemDataIsMissing(dataHash.toString))
          }
        case Some(result) =>
          EitherT.rightT[Task, ApiError](result)
      }
      .value
      .runToFuture
  }

  def policyItemsInfo(policiesMetaInfoReq: PoliciesMetaInfoRequest): Future[Either[ApiError, PoliciesMetaInfoResponse]] = {
    val validPolicyIdsWithHashes = policiesMetaInfoReq.policiesDataHashes.flatMap { policyIdWithDataHash =>
      decodePolicyId(policyIdWithDataHash.policyId).toOption.map(_ => policyIdWithDataHash.policyId -> policyIdWithDataHash.datahashes)
    }.toMap

    if (validPolicyIdsWithHashes.isEmpty) {
      Future.successful(Left(CustomValidationError("Failed to decode all policy ids.")))
    } else {
      storage
        .policyItemsMetas(validPolicyIdsWithHashes)
        .map(_.map { policyMetas =>
          val metaByPolicyIdMap = policyMetas.groupBy(_.policyId)

          val policiesDatasInfo = metaByPolicyIdMap.map {
            case (policyId, policyMetaData) =>
              val policyMetaItemsInfo = policyMetaData.map(policyMetaItemInfo)
              PolicyDatasInfo(policyId, policyMetaItemsInfo)
          }.toSeq

          PoliciesMetaInfoResponse(policiesDatasInfo)
        })
        .runToFuture
    }
  }

  def policyDataExists(policyIdStr: String, policyItemHash: String): Future[Either[ApiError, Boolean]] = {
    storage.policyItemExists(policyIdStr, policyItemHash).runToFuture
  }

  def sendData(policyItem: PolicyItem, broadcast: Boolean = true): Future[Either[ApiError, PolicyDataHashTransaction]] = {
    log.debug(show"Receive sendData request. '$policyItem'")
    validatePolicyItem(policyItem) match {
      case Left(validationError) => Future.successful(Left(ApiError.fromValidationError(validationError)))
      case Right(setup) =>
        internalSendData(policyItem, setup, broadcast)
    }
  }

  def sendLargeData(policyItem: PolicyItem,
                    setup: ValidSendDataSetup,
                    data: Observable[Byte],
                    broadcast: Boolean = true): Future[Either[ApiError, PolicyDataHashTransaction]] =
    internalSendData(policyItem, setup, broadcast, Some(data))

  def forceSync(): Future[Either[ApiError, PrivacyForceSyncResponse]] = {
    Future {
      policyDataSynchronizer.forceSync()
    }.map(launched => PrivacyForceSyncResponse(launched).asRight[ApiError])
      .recover {
        case ex =>
          log.error("Error on force sync", ex)
          ForceSyncError.asLeft[PrivacyForceSyncResponse]
      }
  }

  def forceSync(encodedPolicyId: String, nodeOwner: Address): Future[Either[ApiError, PrivacyForceSyncResponse]] = {
    (for {
      policyId <- EitherT.fromEither[Future](checkPolicyExists(encodedPolicyId).leftMap(ApiError.fromValidationError))
      _        <- EitherT.fromEither[Future](checkSenderIsInPolicyRecipients(policyId, nodeOwner).leftMap(ApiError.fromValidationError))
      response <- EitherT {
        Future {
          policyDataSynchronizer.forceSync(policyId)
        }.map(launched => PrivacyForceSyncResponse(launched).asRight[ApiError])
          .recover {
            case ex =>
              log.error("Error on force sync", ex)
              ForceSyncError.asLeft[PrivacyForceSyncResponse]
          }
      }
    } yield response).value
  }

  private def internalSendData(policyItem: PolicyItem,
                               setup: ValidSendDataSetup,
                               broadcast: Boolean,
                               largeData: Option[Observable[Byte]] = None): Future[Either[ApiError, PolicyDataHashTransaction]] = {
    import setup._
    val dataType = largeData.fold[PrivacyDataType](PrivacyDataType.Default)(_ => PrivacyDataType.Large)

    def buildTx: Either[ValidationError, PolicyDataHashTransaction] =
      policyItem.version match {
        case 1 => PolicyDataHashTransactionV1.selfSigned(signer, dataHash, policyId, time.getTimestamp(), policyItem.fee)
        case 2 => PolicyDataHashTransactionV2.selfSigned(signer, dataHash, policyId, time.getTimestamp(), policyItem.fee, feeAssetId)
        case 3 => PolicyDataHashTransactionV3.selfSigned(signer, dataHash, policyId, time.getTimestamp(), policyItem.fee, feeAssetId, atomicBadge)
      }

    def validatedTx: Either[ValidationError, PolicyDataHashTransaction] = {
      PrivacyMetrics.measureEither(SendDataTxValidation, policyItem.policyId, policyItem.hash) {
        for {
          _                <- PolicyDataHashTransactionDiff.checkPolicyDataHashExistence(policyId, dataHash, state)
          policyDataHashTx <- buildTx
          _                <- feeCalculator.validateTxFee(state.height, policyDataHashTx)
        } yield policyDataHashTx
      }
    }

    def saveData(hashInStorage: Boolean): EitherT[Task, ApiError, Unit] = {
      if (hashInStorage) {
        EitherT.rightT[Task, ApiError] {
          log.debug(
            s"DataHash '${dataHash.stringRepr}' for policy id '${policyId.base58}' is not found in blockchain," +
              s" but is present in privacy storage. Going to create and broadcast PolicyDataHash transaction.")
        }
      } else {
        EitherT(PrivacyMetrics.measureTask(SendDataPolicyItemSaving, policyItem.policyId, policyItem.hash) {
          Task.defer {
            largeData.fold(storage.savePolicyItem(policyItem)) { dataStream =>
              storage.savePolicyDataWithMeta(Right(dataStream), PolicyMetaData.fromPolicyItem(policyItem, dataType))
            }
          } <* Task {
            log.debug(s"Successfully saved policyItem in DB for '${PolicyMetaData.fromPolicyItem(policyItem, dataType)}'")
          }
        }) *> EitherT.right(Task(state.putItemDescriptor(policyId, dataHash, PrivacyItemDescriptor(dataType))))
      }
    }

    def validationToApiError(e: ValidationError): ApiError = {
      log.error(s"Validation error: '$e', policyId: '${policyItem.policyId}', policyDataHash: '${policyItem.hash}'")
      ApiError.fromValidationError(e)
    }

    def checkDiffIfNeed(tx: Transaction, certChain: Option[CertChain]): EitherT[Task, ApiError, Diff] = {
      if (broadcast)
        EitherT.fromEither[Task](txBroadcaster.txDiffer(tx, certChain)).leftMap(validationToApiError)
      else
        EitherT.rightT[Task, ApiError](Diff.empty)
    }

    def broadcastTxIfNeed(tx: PolicyDataHashTransaction, diff: Diff, certChain: Option[CertChain]): EitherT[Task, ApiError, Unit] =
      if (broadcast) {
        EitherT.right[ApiError] {
          Task {
            val txWithSize = TransactionWithSize(tx.bytes().length, tx)
            txBroadcaster.forceBroadcast(txWithSize, diff, certChain)
            log.debug(s"Successfully broadcast transaction with id: '${tx.id()}'")
          }
        }
      } else {
        EitherT.rightT[Task, ApiError](())
      }

    def broadcastInventoryIfNeed(tx: PolicyDataHashTransaction): EitherT[Task, ApiError, Unit] =
      if (broadcast) {
        EitherT.right[ApiError] {
          buildPrivacyInventory(dataType, policyId, dataHash, signer)
            .map(inventory => broadcastInventory(inventory)) <* Task(log.debug(s"Successfully broadcast inventory for tx with id '${tx.id()}'"))
        }
      } else {
        EitherT.rightT[Task, ApiError](())
      }

    (for {
      tx            <- EitherT.fromEither[Task](validatedTx).leftMap(validationToApiError)
      hashInStorage <- EitherT(storage.policyItemExists(policyId.toString, dataHash.toString))
      diff          <- checkDiffIfNeed(tx, setup.certChain)
      _             <- saveData(hashInStorage)
      _             <- broadcastTxIfNeed(tx, diff, setup.certChain)
      _             <- broadcastInventoryIfNeed(tx)
    } yield tx).value.runToFuture
  }

  private def policyMetaItemInfo(policyMeta: PolicyMetaData): PolicyItemInfo = {
    val metaInfo = PolicyItemFileInfo(filename = policyMeta.filename,
                                      size = policyMeta.size,
                                      timestamp = policyMeta.timestamp,
                                      author = policyMeta.author,
                                      comment = policyMeta.comment)

    PolicyItemInfo(
      sender = policyMeta.sender,
      policy = policyMeta.policyId,
      info = metaInfo,
      hash = policyMeta.hash
    )
  }

  private def validatePolicyItem(policyItem: PolicyItem): Either[ValidationError, ValidSendDataSetup] =
    PrivacyMetrics.measureEither(SendDataRequestValidation, policyItem.policyId, policyItem.hash) {
      for {
        _          <- checkVersionFields(policyItem)
        _          <- checkInfo(policyItem.info)
        _          <- checkPolicyDataSize(policyItem)
        certChain  <- parseCertChain(policyItem.certificatesBytes)
        feeAssetId <- policyItem.parsedFeeAssetId
        dataHash   <- PolicyDataHash.fromBase58String(policyItem.hash)
        policyId   <- decodePolicyId(policyItem.policyId)
        sender <- Address
          .fromString(policyItem.sender)
          .leftMap(err => GenericError(s"Failed to decode sender address '${policyItem.sender}', reason: $err"))
        privateKey <- sendersPrivateKey(sender, policyItem.password)
      } yield ValidSendDataSetup(policyId, privateKey, dataHash, feeAssetId, policyItem.atomicBadge, certChain)
    }

  def validateLargePolicyItem(policyItem: PolicyItem): Either[ValidationError, ValidSendDataSetup] =
    PrivacyMetrics.measureEither(SendDataRequestValidation, policyItem.policyId, policyItem.hash) {
      for {
        _          <- checkVersionFields(policyItem)
        _          <- checkInfo(policyItem.info)
        certChain  <- parseCertChain(policyItem.certificatesBytes)
        pdh        <- PolicyDataHash.fromBase58String(policyItem.hash)
        policyId   <- decodePolicyId(policyItem.policyId)
        _          <- PolicyDiff.checkPolicyDataHashNotExists(policyId, pdh, state)
        feeAssetId <- policyItem.parsedFeeAssetId
        sender <- Address
          .fromString(policyItem.sender)
          .leftMap(err => GenericError(s"Failed to decode sender address '${policyItem.sender}', reason: $err"))
        privateKey <- sendersPrivateKey(sender, policyItem.password)
      } yield ValidSendDataSetup(policyId, privateKey, pdh, feeAssetId, policyItem.atomicBadge, certChain)
    }

  def isLargeObjectFeatureActivated(): Boolean = {
    import com.wavesenterprise.features.FeatureProvider._

    state.isFeatureActivated(BlockchainFeature.PrivacyLargeObjectSupport, state.height)
  }

  private def checkVersionFields(policyItem: PolicyItem): Either[GenericError, Unit] =
    Either.cond(!(policyItem.version == 1 && policyItem.feeAssetId.isDefined),
                (),
                GenericError(s"'feeAssetId' field is not allowed for the SendDataRequest version '1'"))

  private def checkInfo(dataInfo: PrivacyDataInfo): Either[ValidationError, Unit] = {
    for {
      _ <- Either.cond(
        dataInfo.filename.length <= PrivacyDataInfo.fileNameMaxLength,
        (),
        GenericError(s"File name is too long '${dataInfo.filename.length}', maximum is '${PrivacyDataInfo.fileNameMaxLength}'")
      )
      _ <- Either.cond(
        dataInfo.author.length <= PrivacyDataInfo.authorMaxLength,
        (),
        GenericError(s"Author name is too long '${dataInfo.author.length}', maximum is '${PrivacyDataInfo.authorMaxLength}'")
      )
      _ <- Either.cond(
        dataInfo.comment.length <= PrivacyDataInfo.commentMaxLength,
        (),
        GenericError(s"Comment is too long '${dataInfo.comment.length}', maximum is '${PrivacyDataInfo.commentMaxLength}'")
      )
    } yield ()
  }

  private def checkPolicyDataSize(policyItem: PolicyItem): Either[ValidationError, Unit] = {
    //this is approximation size, we don't want to spent a lot of time to decode super big string here
    val policyDataSize = policyItem.dataLength
    Either.cond(
      policyDataSize < PrivacyApiService.maxPolicyDataStringLength,
      (),
      ValidationError.PolicyDataTooBig(policyDataSize, PrivacyApiService.maxPolicyDataStringLength)
    )
  }

  private def checkPolicyExists(policyIdStr: String): Either[ValidationError, ByteStr] = {
    for {
      policyId <- decodePolicyId(policyIdStr)
      _        <- PolicyDiff.checkPolicyExistence(policyId, state)
    } yield policyId
  }

  private def checkSenderIsInPolicyRecipients(policyId: ByteStr, sender: Address): Either[ValidationError, AddressWithPolicy] = {
    val policyRecipients = state.policyRecipients(policyId)
    Either.cond(policyRecipients.contains(sender),
                AddressWithPolicy(sender, policyId),
                GenericError(s"Sender '${sender.stringRepr}' is not in policy recipients"))
  }

  private def sendersPrivateKey(sender: Address, password: Option[String]): Either[ValidationError, PrivateKeyAccount] = {
    val passwordOpt = password.map(_.toCharArray)
    wallet
      .privateKeyAccount(sender, passwordOpt)
      .leftMap(error => GenericError(s"Failed to get senders private key, maybe password is wrong: '$passwordOpt', error reason: '$error'"))
  }

  override def peers: ActivePeerConnections = txBroadcaster.activePeerConnections
}
