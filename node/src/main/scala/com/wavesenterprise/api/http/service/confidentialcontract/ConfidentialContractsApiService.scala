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
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidContractId}
import com.wavesenterprise.transaction.docker.{CallContractTransactionV6, ExecutedContractTransactionV4}
import monix.eval.Task

import scala.util.{Failure, Success}

class ConfidentialContractsApiService(
    override val blockchain: Blockchain,
    override val peers: ActivePeerConnections,
    nodeOwner: PrivateKeyAccount,
    txBroadcaster: TxBroadcaster,
    persistentConfidentialState: PersistentConfidentialState
) extends ConfidentialDataInventoryBroadcaster with ContractKeysOps {
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
      contractId <-
        EitherT.fromEither[Task](ByteStr.decodeBase58(request.contractId)
          .toEither
          .leftMap(_ => ApiError.fromValidationError(InvalidContractId(request.contractId))))

      _                 <- validateConfidentialContract(contractId)
      commitmentWithKey <- processCommitment(request, commitmentVerification)

      feeAssetId <- (request.feeAssetId
        .map(ByteStr.decodeBase58(_)
          .toEither
          .leftMap(_ => InvalidAssetId("failed decoding asset id"))) match {
        case Some(result) => result.map(_.some)
        case None         => Right(None)
      }).toEitherT[Task]

      tx <- EitherT.fromEither[Task](
        CallContractTransactionV6.selfSigned(
          sender = PrivateKeyAccount(nodeOwner.privateKey, nodeOwner.publicKey),
          contractId = contractId,
          params = List.empty,
          fee = request.fee,
          timestamp = request.timestamp,
          contractVersion = request.contractVersion,
          feeAssetId = feeAssetId,
          atomicBadge = request.atomicBadge,
          payments = List.empty,
          inputCommitment = commitmentWithKey.commitment
        ).leftMap(fromValidationError))

      certChain <- EitherT.fromEither[Task](parseCertChain(request.certificatesBytes).leftMap(fromValidationError))
      diff      <- checkDiff(broadcast, tx, certChain)
      _         <- broadcastTx(broadcast, tx, diff, certChain)

      confidentialInput = ConfidentialInput(
        commitment = commitmentWithKey.commitment,
        txId = tx.id(),
        contractId = ContractId(contractId),
        commitmentKey = commitmentWithKey.commitmentKey,
        entries = request.params
      )
      _         = confidentialRocksDBStorage.saveInput(confidentialInput)
      inventory = ConfidentialInventory(nodeOwner, confidentialInput.contractId, confidentialInput.commitment, ConfidentialDataType.Input)
      _         = broadcastInventory(inventory)
    } yield ConfidentialContractCallResponse(tx, confidentialInput))
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

  private def validateConfidentialContract(contractId: ByteStr): EitherT[Task, ApiError, Unit] = {
    val result = for {
      contractInfo <-
        blockchain
          .contract(ContractId(contractId))
          .toRight(ContractNotFound(contractId.toString))

      _ <- Either.cond(
        test = contractInfo.isConfidential,
        right = (),
        left = ConfidentialCallNotAllowedForContract(contractId.toString)
      )

      _ <- Either.cond(
        test = checkGroupMembership(contractInfo, nodeOwner),
        right = (),
        left = GroupOwnersNotContainsNodeOwner(
          contractId = contractId.toString(),
          nodeOwner = nodeOwner.toAddress.address
        )
      )

      _ <- Either.cond(
        test = contractInfo.groupParticipants.size >= 3,
        right = (),
        left = NotEnoughGroupParticipants(contractId = contractId.toString()),
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

  def confidentialTxByExecutableTxId(transactionId: String): Either[ApiError, ConfidentialTxByExecutableTxIdResponse] =
    ByteStr.decodeBase58(transactionId) match {
      case Success(txId) =>
        blockchain.executedTxFor(txId) match {
          case Some(exTx: ExecutedContractTransactionV4) =>
            (exTx, blockchain.contract(ContractId(exTx.tx.contractId))) match {
              case (ExecutedContractTransactionV4(_, tx: CallContractTransactionV6, _, _, _, _, _, _, _, _, outputCommitment), Some(contractInfo))
                  if contractInfo.isConfidential && checkGroupMembership(contractInfo, nodeOwner) =>
                val inputCommitment = tx.inputCommitment
                (confidentialRocksDBStorage.getInput(inputCommitment), confidentialRocksDBStorage.getOutput(outputCommitment)) match {
                  case (Some(confidentialInput), Some(confidentialOutput)) =>
                    Right(ConfidentialTxByExecutableTxIdResponse(exTx, confidentialInput, confidentialOutput))
                  case _ => Left(
                      ApiError.fromValidationError(GenericError(s"Confidential input or output not found for transaction with id: '$transactionId'")))
                }
              case (_: ExecutedContractTransactionV4, Some(contractInfo)) if !contractInfo.isConfidential =>
                Left(ApiError.fromValidationError(
                  GenericError(s"Contract: '${contractInfo.contractId}' for transaction with id: '$transactionId' is not confidential")))
              case (_: ExecutedContractTransactionV4, Some(contractInfo)) if !checkGroupMembership(contractInfo, nodeOwner) =>
                Left(ApiError.fromValidationError(GenericError(
                  s"Node owner with address '${nodeOwner.toAddress}' is not in confidential groups for contract with id: '${contractInfo.contractId}'")))
              case _ => Left(ExecutedTransactionNotFound(transactionId))
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
    _ <- Either.cond(
      test = checkGroupMembership(contractInfo, nodeOwner),
      right = (),
      left = GroupOwnersNotContainsNodeOwner(
        contractId = contractId,
        nodeOwner = nodeOwner.toAddress.address
      )
    )
    keysFilter <- validateRegexKeysFilter(matches)
  } yield contractKeysWithFilter(contractInfo, offsetOpt, limitOpt, keysFilter)

  def contractKeys(contractIdStr: String, keys: Iterable[String]): Either[ApiError, Vector[DataEntry[_]]] = {
    findContract(contractIdStr).map { contract =>
      persistentConfidentialState.contractData(ContractId(contract.contractId), keys).toVector
    }
  }

  protected def contractKeysWithFilter(contractInfo: ContractInfo,
                                       offsetOpt: Option[Int],
                                       limitOpt: Option[Int],
                                       keysFilter: Option[String => Boolean]): Vector[DataEntry[_]] = {
    val keysRequest = KeysRequest(contractInfo.contractId, offsetOpt, limitOpt, keysFilter)
    val keys        = persistentConfidentialState.contractKeys(keysRequest)
    persistentConfidentialState.contractData(ContractId(contractInfo.contractId), keys).toVector
  }

  private def checkGroupMembership(contractInfo: ContractInfo, nodeOwner: PrivateKeyAccount): Boolean = {
    contractInfo.groupOwners.contains(nodeOwner.toAddress) || contractInfo.groupParticipants.contains(nodeOwner.toAddress)
  }

}
