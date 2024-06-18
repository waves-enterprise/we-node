package com.wavesenterprise.api.http.service.confidentialcontract

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError._
import com.wavesenterprise.api.http.service.ContractKeysOps
import com.wavesenterprise.api.parseCertChain
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.SaltBytes
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.database.rocksdb.confidential.PersistentConfidentialState
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.network.contracts.{ConfidentialDataInventoryBroadcaster, ConfidentialDataType, ConfidentialDataUtils}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ConfidentialInventory, TransactionWithSize, TxBroadcaster}
import com.wavesenterprise.state.contracts.confidential.ConfidentialInput
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId, DataEntry, Diff}
import com.wavesenterprise.transaction.{AtomicBadge, AtomicInnerTransaction, PaymentsV1ToContract, Transaction, ValidationError}
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidContractId}
import com.wavesenterprise.transaction.docker.{
  CallContractTransaction,
  CallContractTransactionV6,
  CallContractTransactionV7,
  ExecutedContractTransactionV4,
  ExecutedContractTransactionV5
}
import monix.eval.Task

import scala.util.{Failure, Success}

class ConfidentialContractsApiService(
    override val blockchain: Blockchain,
    override val peers: ActivePeerConnections,
    nodeOwner: PrivateKeyAccount,
    txBroadcaster: TxBroadcaster,
    persistentConfidentialState: PersistentConfidentialState
) extends ConfidentialDataInventoryBroadcaster with ContractKeysOps with ConfidentialParticipantsAtomicValidations {
  case class CommitmentWithKey(
      commitment: Commitment,
      commitmentKey: SaltBytes
  )

  private val confidentialRocksDBStorage = persistentConfidentialState.storage

  def call(
      request: ConfidentialContractCallRequest,
      broadcast: Boolean,
      commitmentVerification: Boolean
  ): Task[Either[ApiError, ConfidentialContractCallResponse]] =
    (for {
      contractIdStr <-
        EitherT.fromEither[Task](ByteStr.decodeBase58(request.contractId)
          .toEither
          .leftMap(_ => ApiError.fromValidationError(InvalidContractId(request.contractId))))
      contractId   = ContractId(contractIdStr)
      contractInfo = blockchain.contract(contractId)
      _                 <- validateConfidentialContractIfNotAtomic(request.atomicBadge, contractInfo, contractId)
      commitmentWithKey <- processCommitment(request, commitmentVerification)

      feeAssetId <- (request.feeAssetId
        .map(ByteStr.decodeBase58(_)
          .toEither
          .leftMap(_ => InvalidAssetId("failed decoding asset id"))) match {
        case Some(result) => result.map(_.some)
        case None         => Right(None)
      }).toEitherT[Task]

      tx <- EitherT.fromEither[Task] {
        makeCallTransaction(
          request = request,
          sender = PrivateKeyAccount(nodeOwner.privateKey, nodeOwner.publicKey),
          inputCommitment = commitmentWithKey.commitment,
          contractId = contractId,
          feeAssetId = feeAssetId,
          contractInfoOpt = contractInfo
        )
          .leftMap(fromValidationError)
      }

      confidentialDataRecipientsAndMessage <- EitherT.fromEither[Task](getConfidentialDataRecipients(request, contractInfo))
      certChain                            <- EitherT.fromEither[Task](parseCertChain(request.certificatesBytes).leftMap(fromValidationError))
      diff                                 <- checkDiff(broadcast, tx, certChain)
      _                                    <- broadcastTx(broadcast, tx, diff, certChain)

      confidentialInput = ConfidentialInput(
        commitment = commitmentWithKey.commitment,
        txId = tx.id(),
        contractId = contractId,
        commitmentKey = commitmentWithKey.commitmentKey,
        entries = request.params
      )
      _         = confidentialRocksDBStorage.saveInput(confidentialInput)
      inventory = ConfidentialInventory(nodeOwner, confidentialInput.contractId, confidentialInput.commitment, ConfidentialDataType.Input)

      (confidentialDataRecipients, message) = confidentialDataRecipientsAndMessage
      _                                     = broadcastInventoryToRecipients(confidentialDataRecipients, inventory)
    } yield ConfidentialContractCallResponse(tx, confidentialInput, message))
      .value

  private def broadcastTx(
      broadcast: Boolean,
      tx: Transaction,
      diff: Diff,
      certChain: Option[CertChain]
  ): EitherT[Task, ApiError, Unit] =
    if (broadcast) {
      val txWithSize = TransactionWithSize(tx.bytes().length, tx)
      txBroadcaster
        .broadcast(txWithSize, diff, certChain)
        .leftMap(ApiError.fromCryptoError)
    } else {
      EitherT.rightT[Task, ApiError](())
    }

  private def checkDiff(broadcast: Boolean, tx: Transaction, certChain: Option[CertChain]): EitherT[Task, ApiError, Diff] = {
    val diff = if (broadcast) {
      txBroadcaster.txDiffer(tx, certChain).leftMap(ApiError.fromValidationError)
    } else {
      Right(Diff.empty)
    }

    EitherT.fromEither(diff)
  }

  private def validateConfidentialContractIfNotAtomic(atomicBadge: Option[AtomicBadge],
                                                      contractInfo: Option[ContractInfo],
                                                      contractId: ContractId): EitherT[Task, ApiError, Unit] = {
    atomicBadge match {
      case None    => validateConfidentialContract(contractInfo, contractId)
      case Some(_) => EitherT.fromEither[Task](Either.right(()))
    }
  }

  private def validateConfidentialContract(contractInfo: Option[ContractInfo], contractId: ContractId): EitherT[Task, ApiError, Unit] = {
    val contractIdStr = contractId.byteStr.toString()
    val result = for {
      contractInfo <-
        contractInfo
          .toRight(ContractNotFound(contractIdStr))

      _ <- Either.cond(
        test = contractInfo.isConfidential,
        right = (),
        left = ConfidentialCallNotAllowedForContract(contractIdStr)
      )

      _ <- checkConfidentialGroupMembership(contractInfo)

      _ <- Either.cond(
        test = contractInfo.groupParticipants.size >= 3,
        right = (),
        left = NotEnoughGroupParticipants(contractId = contractIdStr),
      )
    } yield ()

    EitherT.fromEither[Task](result)
  }

  private def processCommitment(
      request: ConfidentialContractCallRequest,
      commitmentVerification: Boolean
  ): EitherT[Task, ApiError, CommitmentWithKey] = {
    // prepare params for check or create Commitment
    val data = ConfidentialDataUtils.entriesToBytes(request.params)

    val commitmentWithKey = if (commitmentVerification) {
      for {
        commitmentString <- request.commitment.toRight(CommitmentNotPresent)

        commitment <- ByteStr.decodeBase58(commitmentString)
          .toEither
          .bimap(_ => ApiError.fromValidationError(GenericError("error decoding commitment")), byteStr => Commitment(byteStr))

        commitmentKeyString <- request.commitmentKey.toRight(CommitmentKeyNotPresent)

        commitmentKey <- ByteStr.decodeBase58(commitmentKeyString)
          .toEither
          .bimap(_ => ApiError.fromValidationError(GenericError("error decoding commitmentKey")), byteStr => SaltBytes(byteStr))

        commitmentWithKey <-
          Either.cond(
            test = commitment.validate(data, commitmentKey),
            right = CommitmentWithKey(commitment, commitmentKey),
            left = CommitmentValidationFailed
          )
      } yield commitmentWithKey
    } else {
      val (_, commitmentKey) = crypto.algorithms.saltedSecureHash(data)
      val commitment         = Commitment.create(data, commitmentKey)

      Right(CommitmentWithKey(commitment, commitmentKey))
    }

    EitherT.fromEither(commitmentWithKey)
  }

  // TODO: get tx need to refactor due large amount of match, case and if
  def confidentialTxByExecutableTxId(transactionId: String): Either[ApiError, ConfidentialExecutableTxResponse] =
    ByteStr.decodeBase58(transactionId) match {
      case Success(txId) =>
        blockchain.executedTxFor(txId) match {
          case Some(exTx: ExecutedContractTransactionV4) => {
            (exTx, blockchain.contract(ContractId(exTx.tx.contractId))) match {
              case (ExecutedContractTransactionV4(_, tx: CallContractTransactionV6, _, _, _, _, _, _, _, _, outputCommitment), Some(contractInfo))
                  if contractInfo.isConfidential && checkConfidentialGroupMembershipBoolean(
                    contractInfo) =>
                val inputCommitment = tx.inputCommitment
                (confidentialRocksDBStorage.getInput(inputCommitment), confidentialRocksDBStorage.getOutput(outputCommitment)) match {
                  case (Some(confidentialInput), Some(confidentialOutput)) =>
                    Right(ConfidentialExecutableTxV4Response(exTx, confidentialInput, confidentialOutput))
                  case _ => Left(
                      ApiError.fromValidationError(GenericError(s"Confidential input or output not found for transaction with id: '$transactionId'")))
                }
              case (_: ExecutedContractTransactionV4, Some(contractInfo)) if !contractInfo.isConfidential =>
                Left(ApiError.fromValidationError(
                  GenericError(s"Contract: '${contractInfo.contractId}' for transaction with id: '$transactionId' is not confidential")))
              case (_: ExecutedContractTransactionV4, Some(contractInfo)) if !checkConfidentialGroupMembershipBoolean(contractInfo) =>
                Left(ApiError.fromValidationError(GenericError(
                  s"Node owner with address '${nodeOwner.toAddress}' is not in confidential groups for contract with id: '${contractInfo.contractId}'")))
              case _ => Left(ExecutedTransactionNotFound(transactionId))
            }
          }
          case Some(exTx: ExecutedContractTransactionV5) => {
            (exTx, blockchain.contract(ContractId(exTx.tx.contractId))) match {
              case (ExecutedContractTransactionV5(_, tx: CallContractTransactionV7, _, _, _, _, _, _, _, outputCommitmentOpt, _, _, _),
                    Some(contractInfo))
                  if contractInfo.isConfidential && checkConfidentialGroupMembershipBoolean(
                    contractInfo) => {
                val confidentialInputOpt  = tx.inputCommitmentOpt.flatMap(confidentialRocksDBStorage.getInput)
                val confidentialOutputOpt = outputCommitmentOpt.flatMap(confidentialRocksDBStorage.getOutput)
                (confidentialInputOpt, confidentialOutputOpt) match {
                  case (Some(confidentialInput), Some(confidentialOutput)) =>
                    Right(ConfidentialExecutableTxV5Response(exTx, confidentialInput, confidentialOutput))
                  case _ =>
                    Left(
                      ApiError.fromValidationError(GenericError(s"Confidential input or output not found for transaction with id: '$transactionId'")))
                }
              }
              case _ => Left(ExecutedTransactionNotFound(transactionId))
            }
          }
          case _ => Left(ExecutedTransactionNotFound(transactionId))
        }
      case Failure(_) => Left(ApiError.fromValidationError(GenericError(s"Failed to decode base58 transaction id value '$transactionId'")))
    }

  def contractKeys(contractId: String,
                   offsetOpt: Option[Int],
                   limitOpt: Option[Int],
                   matches: Option[String]): Either[ApiError, Vector[DataEntry[_]]] = for {
    contractInfo <- findContract(contractId)
    _            <- checkConfidentialGroupMembership(contractInfo)
    keysFilter   <- validateRegexKeysFilter(matches)
  } yield contractKeysWithFilter(contractInfo, offsetOpt, limitOpt, keysFilter)

  def contractKeys(contractIdStr: String, keys: Iterable[String]): Either[ApiError, Vector[DataEntry[_]]] = for {
    contractInfo <- findContract(contractIdStr)
    _            <- checkConfidentialGroupMembership(contractInfo)
  } yield persistentConfidentialState.contractData(ContractId(contractInfo.contractId), keys).toVector

  protected def contractKeysWithFilter(contractInfo: ContractInfo,
                                       offsetOpt: Option[Int],
                                       limitOpt: Option[Int],
                                       keysFilter: Option[String => Boolean]): Vector[DataEntry[_]] = {
    val keysRequest = KeysRequest(contractInfo.contractId, offsetOpt, limitOpt, keysFilter)
    val keys        = persistentConfidentialState.contractKeys(keysRequest)
    persistentConfidentialState.contractData(ContractId(contractInfo.contractId), keys).toVector
  }

  private def checkConfidentialGroupMembershipBoolean(contractInfo: ContractInfo): Boolean = {
    contractInfo.groupOwners.contains(nodeOwner.toAddress) || contractInfo.groupParticipants.contains(nodeOwner.toAddress)
  }

  private def checkConfidentialGroupMembership(contractInfo: ContractInfo): Either[ApiError, Unit] = {
    Either.cond(
      test = checkConfidentialGroupMembershipBoolean(contractInfo),
      right = (),
      left = GroupOwnersNotContainsNodeOwner(
        contractId = contractInfo.contractId.toString(),
        nodeOwner = nodeOwner.toAddress.address
      )
    )
  }

  private def makeCallTransaction(request: ConfidentialContractCallRequest,
                                  sender: PrivateKeyAccount,
                                  inputCommitment: Commitment,
                                  contractId: ContractId,
                                  feeAssetId: Option[ByteStr],
                                  contractInfoOpt: Option[ContractInfo])
      : Either[ValidationError, CallContractTransaction with AtomicInnerTransaction with PaymentsV1ToContract] = {
    val contractIdByteStr = contractInfoOpt.map(_.contractId).getOrElse(contractId.byteStr)
    if (request.contractEngine == "wasm") {
      CallContractTransactionV7.selfSigned(
        sender = sender,
        contractId = contractIdByteStr,
        params = List.empty,
        fee = request.fee,
        timestamp = request.timestamp,
        contractVersion = request.contractVersion,
        feeAssetId = feeAssetId,
        atomicBadge = request.atomicBadge,
        payments = request.payments,
        inputCommitmentOpt = inputCommitment.some,
        contractEngine = "wasm",
        callFunc = request.callFunc,
      )
    } else {
      CallContractTransactionV6.selfSigned(
        sender = sender,
        contractId = contractIdByteStr,
        params = List.empty,
        fee = request.fee,
        timestamp = request.timestamp,
        contractVersion = request.contractVersion,
        feeAssetId = feeAssetId,
        atomicBadge = request.atomicBadge,
        payments = request.payments,
        inputCommitment = inputCommitment
      )
    }
  }
}
