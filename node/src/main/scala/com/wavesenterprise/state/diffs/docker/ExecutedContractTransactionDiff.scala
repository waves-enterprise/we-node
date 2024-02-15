package com.wavesenterprise.state.diffs.docker

import cats.implicits._
import cats.kernel.Monoid
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.docker.ExecutedContractTransactionDiff._
import com.wavesenterprise.state.diffs.{AssetOpsSupport, TransferOpsSupport}
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.state.{Account, AssetHolder, AssetInfo, Blockchain, ByteStr, Contract, ContractId, Diff, LeaseBalance, LeaseId, Portfolio}
import com.wavesenterprise.transaction.ValidationError.{ContractNotFound, GenericError, InvalidSender}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{
  ContractBurnV1,
  ContractCancelLeaseV1,
  ContractIssueV1,
  ContractLeaseV1,
  ContractPaymentV1,
  ContractReissueV1,
  ContractTransferOutV1
}
import com.wavesenterprise.transaction.{AssetId, Signed, ValidationError}

/**
  * Creates [[Diff]] for [[ExecutedContractTransaction]]
  */
case class ExecutedContractTransactionDiff(
    blockchain: Blockchain,
    blockTimestamp: Long,
    height: Int,
    blockOpt: Option[Signed],
    minerOpt: Option[PublicKeyAccount],
    contractTxExecutor: ContractTxExecutorType = MiningExecutor
) extends AssetOpsSupport
    with TransferOpsSupport {

  def apply(tx: ExecutedContractTransaction): Either[ValidationError, Diff] =
    checkExecutableTxIsNotExecuted(tx) >>
      ((blockOpt, minerOpt) match {
        case (Some(block), _)    => applyInner(tx, block.sender)
        case (None, Some(miner)) => applyInner(tx, miner)
        case _                   => Left(GenericError("Block or microblock is needed to validate this transaction"))
      })

  private def applyInner(executedTx: ExecutedContractTransaction, miner: PublicKeyAccount): Either[ValidationError, Diff] =
    validateMinerIsSender(miner, executedTx) >>
      checkResultsHashIfMiner(executedTx) >>
      checkValidationProofsIfNotValidator(executedTx, miner.toAddress) >>
      calcDiff(executedTx)

  private def calcDiff(executedTx: ExecutedContractTransaction): Either[ValidationError, Diff] = {
    val baseDiff = executedTx.tx match {
      case tx: CreateContractTransaction =>
        CreateContractTransactionDiff(blockchain, None, height)(tx).map { innerDiff =>
          Monoid.combine(
            innerDiff,
            Diff(
              height = height,
              tx = executedTx,
              contractsData = getContractsData(executedTx),
              executedTxMapping = Map(tx.id() -> executedTx.id())
            )
          )
        }

      case tx: CallContractTransaction =>
        CallContractTransactionDiff(blockchain, None, height)(tx).map { innerDiff =>
          Monoid.combine(innerDiff,
                         Diff(
                           height = height,
                           tx = executedTx,
                           contractsData = getContractsData(executedTx),
                           executedTxMapping = Map(tx.id() -> executedTx.id())
                         ))
        }

      case tx: UpdateContractTransaction =>
        for {
          _         <- blockchain.contract(ContractId(tx.contractId)).toRight(ContractNotFound(tx.contractId))
          innerDiff <- UpdateContractTransactionDiff(blockchain, None, height)(tx)
        } yield Monoid.combine(
          innerDiff,
          Diff(
            height = height,
            tx = executedTx,
            executedTxMapping = Map(tx.id() -> executedTx.id())
          )
        )

      case _ => Left(ValidationError.GenericError("Unknown type of transaction"))
    }

    executedTx match {
      case tx: ExecutedContractTransactionV5 if tx.statusCode == 0 => baseDiff.flatMap(calcAssetOperationsDiffV5(_)(tx))
      case _: ExecutedContractTransactionV5                        => baseDiff.map(_.copy(contractsData = Map.empty))
      case tx: ExecutedContractTransactionV3                       => baseDiff.flatMap(calcAssetOperationsDiff(_)(tx))
      case _                                                       => baseDiff
    }
  }

  private def getContractsData(executedTx: ExecutedContractTransaction) = {
    executedTx match {
      case tx: ExecutedContractTransactionV5 =>
        tx.resultsMap.mapping.mapValues(list => ExecutedContractData(list.map(e => e.key -> e).toMap))
      case _ => Map(executedTx.tx.contractId -> ExecutedContractData(executedTx.results.map(v => (v.key, v)).toMap))
    }

  }

  private def calcAssetOperationsDiffV5(initDiff: Diff)(executedTx: ExecutedContractTransactionV5): Either[ValidationError, Diff] =
    for {
      _         <- checkContractIssueNonces(executedTx.assetOperations)
      _         <- checkContractLeaseNonces(executedTx.assetOperations)
      totalDiff <- applyContractOpsToDiff(initDiff, executedTx)
    } yield totalDiff

  private def calcAssetOperationsDiff(initDiff: Diff)(executedTx: ExecutedContractTransactionV3): Either[ValidationError, Diff] =
    for {
      _         <- checkContractIssueNonces(executedTx.assetOperations)
      _         <- checkContractLeaseNonces(executedTx.assetOperations)
      totalDiff <- applyContractOpsToDiff(initDiff, executedTx)
    } yield totalDiff

  private def applyContractOpsToDiff(initDiff: Diff, executedTx: ExecutedContractTransactionV5): Either[ValidationError, Diff] = {
    def smartDiffAssetsCombining(prevDiff: Diff, assetInfoChangingDiff: Diff) = {
      val combinedDiff  = prevDiff |+| assetInfoChangingDiff
      val changedAssets = collection.mutable.Map[AssetId, AssetInfo]()

      for ((assetId, combinedAssetInfo) <- combinedDiff.assets) {
        if (prevDiff.assets.contains(assetId) && assetInfoChangingDiff.assets.contains(assetId)) {
          changedAssets += assetId -> assetInfoChangingDiff.assets(assetId)
        } else {
          changedAssets += assetId -> combinedAssetInfo
        }
      }

      val fixedDiff = combinedDiff.copy(assets = changedAssets.toMap)
      fixedDiff
    }

    // val contractId = ContractId(executedTx.tx.contractId)

    val appliedAssetOpsDiff = executedTx
      .assetOperationsMap.mapping
      .flatMap { case (cid, ops) => ops.map(cid -> _) }.foldLeft(initDiff.asRight[ValidationError]) {
        case (Right(diff), (cid, issueOp: ContractIssueV1)) =>
          for {
            _ <- checkAssetIdLength(issueOp.assetId)
            _ <- checkAssetNotExist(blockchain, issueOp.assetId)
            issueDiff = diffFromContractIssue(executedTx, cid, issueOp, height)
          } yield diff |+| issueDiff

        case (Right(diff), (cid, reissueOp: ContractReissueV1)) =>
          val assetId  = reissueOp.assetId
          val contract = Contract(ContractId(cid))
          for {
            asset <- findAssetForContract(blockchain, diff, assetId)
            _     <- checkAssetIdLength(assetId)
            _     <- Either.cond(asset.issuer == contract, (), GenericError(s"Asset '$assetId' was not issued by '$contract'"))
            _     <- Either.cond(asset.reissuable, (), GenericError("Asset is not reissuable"))
            _     <- checkOverflowAfterReissue(asset, reissueOp.quantity, isDataTxActivated = true)
            reissueDiff = diffFromContractReissue(executedTx, cid, reissueOp, asset, height)
          } yield smartDiffAssetsCombining(diff, reissueDiff)

        case (Right(diff), (cid, burnOp: ContractBurnV1)) =>
          val burnDiffEither = burnOp.assetId match {
            case Some(assetId) =>
              for {
                asset <- findAssetForContract(blockchain, diff, assetId)
                _     <- checkAssetIdLength(assetId)
                diff  <- diffFromContractBurn(executedTx, cid, burnOp, asset, height)
              } yield diff
            case None => GenericError("Attempt to burn WEST token").asLeft[Diff]
          }

          burnDiffEither.map(burnDiff => smartDiffAssetsCombining(diff, burnDiff))

        case (Right(diff), (cid, transferOp: ContractTransferOutV1)) =>
          val validateAssetExistence = transferOp.assetId match {
            case None =>
              Right(())
            case Some(assetId) =>
              for {
                _ <- checkAssetIdLength(assetId)
                _ <- Either.cond(
                  diff.assets.contains(assetId) || blockchain.assetDescription(assetId).isDefined,
                  (),
                  GenericError(s"Asset '$assetId' does not exist")
                )
              } yield ()
          }

          for {
            _         <- validateAssetExistence
            recipient <- blockchain.resolveAlias(transferOp.recipient).map(_.toAssetHolder)
            transferDiff = Diff(height, executedTx, getPortfoliosMap(transferOp, ContractId(cid).toAssetHolder, recipient))
          } yield diff |+| transferDiff

        case (Right(diff), (cid, leaseOp: ContractLeaseV1)) =>
          val contractId = ContractId(cid)
          for {
            _ <- checkLeaseIdLength(leaseOp.leaseId)
            _ <- checkLeaseIdNotExist(blockchain, leaseOp.leaseId)

            recipientAddress <- blockchain.resolveAlias(leaseOp.recipient)
            contractPortfolio = diff.portfolios.getOrElse(Contract(contractId), Portfolio.empty) |+| blockchain.contractPortfolio(contractId)
            _ <- Either.cond(
              contractPortfolio.balance - contractPortfolio.lease.out >= leaseOp.amount,
              (),
              GenericError(s"Cannot lease more than own: balance:${contractPortfolio.balance}, already leased: ${contractPortfolio.lease.out}")
            )
          } yield {
            val sender    = Contract(contractId)
            val recipient = Account(recipientAddress)

            val portfolioDiff: Map[AssetHolder, Portfolio] = Map(
              sender    -> Portfolio(0, LeaseBalance(0, leaseOp.amount), Map.empty),
              recipient -> Portfolio(0, LeaseBalance(leaseOp.amount, 0), Map.empty)
            )

            val leaseDetails = LeaseDetails(
              sender,
              leaseOp.recipient,
              height,
              leaseOp.amount,
              isActive = true,
              leaseTxId = Some(executedTx.id())
            )

            val leaseDiff = Diff(
              height = height,
              tx = executedTx,
              portfolios = portfolioDiff,
              leaseMap = Map(LeaseId(leaseOp.leaseId) -> leaseDetails)
            )

            diff |+| leaseDiff
          }

        case (Right(diff), (cid, leaseCancelOp: ContractCancelLeaseV1)) =>
          val contractId = ContractId(cid)
          val maybeLease = diff.leaseMap.get(LeaseId(leaseCancelOp.leaseId)).orElse(blockchain.leaseDetails(LeaseId(leaseCancelOp.leaseId)))

          for {
            lease            <- maybeLease.toRight(GenericError("Related LeaseTransaction not found"))
            recipientAddress <- blockchain.resolveAlias(lease.recipient)
            _                <- checkLeaseActive(lease)
            _                <- Either.cond(Contract(contractId) == lease.sender, (), GenericError(s"Lease '${leaseCancelOp.leaseId}' was leased by other sender"))

          } yield {
            val sender    = Contract(contractId)
            val recipient = Account(recipientAddress)

            val portfolioDiff: Map[AssetHolder, Portfolio] = Map(
              sender    -> Portfolio(0, LeaseBalance(0, -lease.amount), Map.empty),
              recipient -> Portfolio(0, LeaseBalance(-lease.amount, 0), Map.empty)
            )

            val leaseDetails = lease.copy(isActive = false)

            val leaseCancelDiff = Diff(
              height = height,
              tx = executedTx,
              portfolios = portfolioDiff,
              leaseCancelMap = Map(LeaseId(leaseCancelOp.leaseId) -> leaseDetails)
            )

            diff |+| leaseCancelDiff
          }
        case (Right(diff), (cid, paymentOp: ContractPaymentV1)) =>
          val contractId = ContractId(cid)
          val validateAssetExistence = paymentOp.assetId match {
            case None =>
              Right(())
            case Some(assetId) =>
              for {
                _ <- checkAssetIdLength(assetId)
                _ <- Either.cond(
                  diff.assets.contains(assetId) || blockchain.assetDescription(assetId).isDefined,
                  (),
                  GenericError(s"Asset '$assetId' does not exist")
                )
              } yield ()
          }

          for {
            _ <- validateAssetExistence
            recipient <- blockchain.contract(ContractId(paymentOp.recipient))
              .toRight(ValidationError.ContractNotFound(paymentOp.recipient))
              .map(_ => ContractId(paymentOp.recipient).toAssetHolder)
            paymentDiff = Diff(height, executedTx, getPortfoliosMap(paymentOp, contractId.toAssetHolder, recipient))
          } yield diff |+| paymentDiff

        case (diffError @ Left(_), _) => diffError
      }

    appliedAssetOpsDiff
  }

  private def applyContractOpsToDiff(initDiff: Diff, executedTx: ExecutedContractTransactionV3): Either[ValidationError, Diff] = {
    def smartDiffAssetsCombining(prevDiff: Diff, assetInfoChangingDiff: Diff) = {
      val combinedDiff  = prevDiff |+| assetInfoChangingDiff
      val changedAssets = collection.mutable.Map[AssetId, AssetInfo]()

      for ((assetId, combinedAssetInfo) <- combinedDiff.assets) {
        if (prevDiff.assets.contains(assetId) && assetInfoChangingDiff.assets.contains(assetId)) {
          changedAssets += assetId -> assetInfoChangingDiff.assets(assetId)
        } else {
          changedAssets += assetId -> combinedAssetInfo
        }
      }

      val fixedDiff = combinedDiff.copy(assets = changedAssets.toMap)
      fixedDiff
    }

    val contractId = ContractId(executedTx.tx.contractId)

    val appliedAssetOpsDiff = executedTx.assetOperations.foldLeft(initDiff.asRight[ValidationError]) {
      case (Right(diff), issueOp: ContractIssueV1) =>
        for {
          _ <- checkAssetIdLength(issueOp.assetId)
          _ <- checkAssetNotExist(blockchain, issueOp.assetId)
          issueDiff = diffFromContractIssue(executedTx, issueOp, height)
        } yield diff |+| issueDiff

      case (Right(diff), reissueOp: ContractReissueV1) =>
        val assetId  = reissueOp.assetId
        val contract = Contract(contractId)
        for {
          asset <- findAssetForContract(blockchain, diff, assetId)
          _     <- checkAssetIdLength(assetId)
          _     <- Either.cond(asset.issuer == contract, (), GenericError(s"Asset '$assetId' was not issued by '$contract'"))
          _     <- Either.cond(asset.reissuable, (), GenericError("Asset is not reissuable"))
          _     <- checkOverflowAfterReissue(asset, reissueOp.quantity, isDataTxActivated = true)
          reissueDiff = diffFromContractReissue(executedTx, reissueOp, asset, height)
        } yield smartDiffAssetsCombining(diff, reissueDiff)

      case (Right(diff), burnOp: ContractBurnV1) =>
        val burnDiffEither = burnOp.assetId match {
          case Some(assetId) =>
            for {
              asset <- findAssetForContract(blockchain, diff, assetId)
              _     <- checkAssetIdLength(assetId)
              diff  <- diffFromContractBurn(executedTx, burnOp, asset, height)
            } yield diff
          case None => GenericError("Attempt to burn WEST token").asLeft[Diff]
        }

        burnDiffEither.map(burnDiff => smartDiffAssetsCombining(diff, burnDiff))

      case (Right(diff), transferOp: ContractTransferOutV1) =>
        val validateAssetExistence = transferOp.assetId match {
          case None =>
            Right(())
          case Some(assetId) =>
            for {
              _ <- checkAssetIdLength(assetId)
              _ <- Either.cond(
                diff.assets.contains(assetId) || blockchain.assetDescription(assetId).isDefined,
                (),
                GenericError(s"Asset '$assetId' does not exist")
              )
            } yield ()
        }

        for {
          _         <- validateAssetExistence
          recipient <- blockchain.resolveAlias(transferOp.recipient).map(_.toAssetHolder)
          transferDiff = Diff(height, executedTx, getPortfoliosMap(transferOp, contractId.toAssetHolder, recipient))
        } yield diff |+| transferDiff

      case (Right(diff), leaseOp: ContractLeaseV1) =>
        for {
          _ <- checkLeaseIdLength(leaseOp.leaseId)
          _ <- checkLeaseIdNotExist(blockchain, leaseOp.leaseId)

          recipientAddress <- blockchain.resolveAlias(leaseOp.recipient)
          contractPortfolio = diff.portfolios.getOrElse(Contract(contractId), Portfolio.empty) |+| blockchain.contractPortfolio(contractId)
          _ <- Either.cond(
            contractPortfolio.balance - contractPortfolio.lease.out >= leaseOp.amount,
            (),
            GenericError(s"Cannot lease more than own: balance:${contractPortfolio.balance}, already leased: ${contractPortfolio.lease.out}")
          )
        } yield {
          val sender    = Contract(contractId)
          val recipient = Account(recipientAddress)

          val portfolioDiff: Map[AssetHolder, Portfolio] = Map(
            sender    -> Portfolio(0, LeaseBalance(0, leaseOp.amount), Map.empty),
            recipient -> Portfolio(0, LeaseBalance(leaseOp.amount, 0), Map.empty)
          )

          val leaseDetails = LeaseDetails(
            sender,
            leaseOp.recipient,
            height,
            leaseOp.amount,
            isActive = true,
            leaseTxId = Some(executedTx.id())
          )

          val leaseDiff = Diff(
            height = height,
            tx = executedTx,
            portfolios = portfolioDiff,
            leaseMap = Map(LeaseId(leaseOp.leaseId) -> leaseDetails)
          )

          diff |+| leaseDiff
        }

      case (Right(diff), leaseCancelOp: ContractCancelLeaseV1) =>
        val maybeLease = diff.leaseMap.get(LeaseId(leaseCancelOp.leaseId)).orElse(blockchain.leaseDetails(LeaseId(leaseCancelOp.leaseId)))

        for {
          lease            <- maybeLease.toRight(GenericError("Related LeaseTransaction not found"))
          recipientAddress <- blockchain.resolveAlias(lease.recipient)
          _                <- checkLeaseActive(lease)
          _                <- Either.cond(Contract(contractId) == lease.sender, (), GenericError(s"Lease '${leaseCancelOp.leaseId}' was leased by other sender"))

        } yield {
          val sender    = Contract(contractId)
          val recipient = Account(recipientAddress)

          val portfolioDiff: Map[AssetHolder, Portfolio] = Map(
            sender    -> Portfolio(0, LeaseBalance(0, -lease.amount), Map.empty),
            recipient -> Portfolio(0, LeaseBalance(-lease.amount, 0), Map.empty)
          )

          val leaseDetails = lease.copy(isActive = false)

          val leaseCancelDiff = Diff(
            height = height,
            tx = executedTx,
            portfolios = portfolioDiff,
            leaseCancelMap = Map(LeaseId(leaseCancelOp.leaseId) -> leaseDetails)
          )

          diff |+| leaseCancelDiff
        }

      case (Right(diff), paymentOp: ContractPaymentV1) =>
        val validateAssetExistence = paymentOp.assetId match {
          case None =>
            Right(())
          case Some(assetId) =>
            for {
              _ <- checkAssetIdLength(assetId)
              _ <- Either.cond(
                diff.assets.contains(assetId) || blockchain.assetDescription(assetId).isDefined,
                (),
                GenericError(s"Asset '$assetId' does not exist")
              )
            } yield ()
        }

        for {
          _ <- validateAssetExistence
          recipient <- blockchain.contract(ContractId(paymentOp.recipient))
            .toRight(ValidationError.ContractNotFound(paymentOp.recipient))
            .map(_ => ContractId(paymentOp.recipient).toAssetHolder)
          paymentDiff = Diff(height, executedTx, getPortfoliosMap(paymentOp, contractId.toAssetHolder, recipient))
        } yield diff |+| paymentDiff

      case (diffError @ Left(_), _) => diffError
    }

    appliedAssetOpsDiff
  }

  private def checkContractIssueNonces(assetOperations: List[ContractAssetOperation]): Either[ValidationError, Unit] = {
    val issueNoncesList = assetOperations.collect { case i: ContractIssueV1 => i.nonce }

    def checkZeroNonces(issueNonces: List[Byte]): Either[ValidationError, Unit] = {
      Either.cond(!issueNonces.contains(0.toByte), (), AssetIdZeroNonceError)
    }

    Either.cond(issueNoncesList.distinct.size == issueNoncesList.size, (), AssetIdNonceDuplicatesError) -> checkZeroNonces(issueNoncesList) match {
      case success @ (Right(()), Right(()))   => success._1
      case duplicateNonceError @ (Left(_), _) => duplicateNonceError._1
      case zeroNonceError @ (_, Left(_))      => zeroNonceError._2
    }
  }

  private def checkContractLeaseNonces(assetOperations: List[ContractAssetOperation]): Either[ValidationError, Unit] = {
    val leaseNoncesList = assetOperations.collect { case i: ContractLeaseV1 => i.nonce }

    def checkZeroNonces(leaseNonces: List[Byte]): Either[ValidationError, Unit] = {
      Either.cond(!leaseNonces.contains(0.toByte), (), LeaseIdZeroNonceError)
    }

    Either.cond(leaseNoncesList.distinct.size == leaseNoncesList.size, (), LeaseIdNonceDuplicatesError) -> checkZeroNonces(leaseNoncesList) match {
      case success @ (Right(()), Right(()))   => success._1
      case duplicateNonceError @ (Left(_), _) => duplicateNonceError._1
      case zeroNonceError @ (_, Left(_))      => zeroNonceError._2
    }
  }

  private def validateMinerIsSender(miner: PublicKeyAccount, tx: ExecutedContractTransaction): Either[ValidationError, Unit] =
    ().asRight.filterOrElse(
      _ => miner == tx.sender,
      InvalidSender(
        s"ExecutedContractTransaction should be created only by block creator: tx sender is '${tx.sender.address}', but block creator is '${miner.toAddress}'")
    )

  private def checkExecutableTxIsNotExecuted(ect: ExecutedContractTransaction): Either[ValidationError, Unit] = {
    Either.cond(!blockchain.hasExecutedTxFor(ect.tx.id()),
                (),
                ValidationError.GenericError(s"Executable transaction ${ect.tx} has been already executed"))
  }

  private def checkResultsHashIfMiner(tx: ExecutedContractTransaction): Either[ValidationError, Unit] = {
    def innerCheck(resultHash: ByteStr): Either[ValidationError.InvalidResultsHash, Unit] = {
      val assetOps = tx match {
        case executedTxV3: ExecutedContractTransactionV3                         => executedTxV3.assetOperations
        case executedTxV4: ExecutedContractTransactionV4                         => executedTxV4.assetOperations
        case _: ExecutedContractTransactionV5                                    => List.empty
        case _: ExecutedContractTransactionV2 | _: ExecutedContractTransactionV1 => List.empty
      }
      val expectedHash = tx match {
        case tx: ExecutedContractTransactionV5 => ContractTransactionValidation.resultsMapHash(tx.resultsMap, tx.assetOperationsMap)
        case _                                 => ContractTransactionValidation.resultsHash(tx.results, assetOps)
      }
      Either.cond(
        resultHash == expectedHash,
        (),
        ValidationError.InvalidResultsHash(resultHash, expectedHash)
      )
    }
    contractTxExecutor match {
      case MiningExecutor =>
        tx match {
          case _: ExecutedContractTransactionV1  => Right(())
          case tx: ExecutedContractTransactionV2 => innerCheck(tx.resultsHash)
          case tx: ExecutedContractTransactionV3 => innerCheck(tx.resultsHash)
          case _: ExecutedContractTransactionV4  => Right(()) // this check will be in transactionExecutor
          case tx: ExecutedContractTransactionV5 => innerCheck(tx.resultsHash)
        }
      case ValidatingExecutor => Right(())
    }
  }

  private def checkValidationProofsIfNotValidator(tx: ExecutedContractTransaction, minerAddress: Address): Either[ValidationError, Unit] = {
    def innerCheck(validationProofs: List[ValidationProof], resultsHash: ByteStr) =
      for {
        validationPolicy <- blockchain.validationPolicy(tx.tx)
        _ <- validationPolicy match {
          case ValidationPolicy.Any                          => Right(())
          case ValidationPolicy.Majority                     => checkValidationProofMajority(tx, minerAddress)
          case ValidationPolicy.MajorityWithOneOf(addresses) => checkValidationProofMajority(tx, minerAddress, addresses.toSet)
        }
        _ <- validationProofs.traverse(checkValidationProof(resultsHash, _))
      } yield ()

    contractTxExecutor match {
      case MiningExecutor =>
        tx match {
          case _: ExecutedContractTransactionV1  => Right(())
          case tx: ExecutedContractTransactionV2 => innerCheck(tx.validationProofs, tx.resultsHash)
          case tx: ExecutedContractTransactionV3 => innerCheck(tx.validationProofs, tx.resultsHash)
          case tx: ExecutedContractTransactionV4 => innerCheck(tx.validationProofs, tx.resultsHash)
          case tx: ExecutedContractTransactionV5 => innerCheck(tx.validationProofs, tx.resultsHash)
        }
      case ValidatingExecutor => Right(())

    }
  }

  private def checkValidationProofMajority(tx: ExecutedContractTransaction,
                                           minerAddress: Address,
                                           requiredAddresses: Set[Address] = Set.empty): Either[ValidationError, Unit] = {
    def innerCheck(validationProofs: List[ValidationProof], resultsHash: ByteStr): Either[ValidationError.InvalidValidationProofs, Unit] = {
      val defaultValidators = blockchain.lastBlockContractValidators - minerAddress
      val validators: Set[Address] = blockchain.contract(ContractId(tx.tx.contractId))
        .filter(_.isConfidential)
        .map(_.groupParticipants intersect defaultValidators)
        .getOrElse(defaultValidators)
      val proofAddresses = validationProofs.view.map(_.validatorPublicKey.toAddress).toSet
      val filteredCount  = proofAddresses.count(validators.contains)
      val majoritySize   = math.ceil(ValidationPolicy.MajorityRatio * validators.size).toInt

      val requiredAddressesCondition = requiredAddresses.isEmpty || (requiredAddresses intersect proofAddresses).nonEmpty
      val majorityCondition          = filteredCount >= majoritySize

      Either.cond(
        requiredAddressesCondition && majorityCondition,
        (),
        ValidationError.InvalidValidationProofs(filteredCount,
                                                majoritySize,
                                                validators,
                                                resultsHash.some,
                                                requiredAddressesCondition,
                                                requiredAddresses)
      )
    }

    tx match {
      case tx: ExecutedContractTransactionV2 => innerCheck(tx.validationProofs, tx.resultsHash)
      case tx: ExecutedContractTransactionV3 => innerCheck(tx.validationProofs, tx.resultsHash)
      case tx: ExecutedContractTransactionV5 => innerCheck(tx.validationProofs, tx.resultsHash)
      case _                                 => Right(())
    }
  }

  private def checkValidationProof(resultsHash: ByteStr,
                                   validationProof: ValidationProof): Either[ValidationError.InvalidValidatorSignature, Unit] = {
    Either.cond(
      crypto.verify(validationProof.signature.arr, resultsHash.arr, validationProof.validatorPublicKey.publicKey),
      (),
      ValidationError.InvalidValidatorSignature(validationProof.validatorPublicKey, validationProof.signature)
    )
  }
}

object ExecutedContractTransactionDiff {
  private val AssetIdNonceDuplicatesError = GenericError("Attempt to issue multiple assets with the same nonce")
  private val AssetIdZeroNonceError       = GenericError("Attempt to issue asset with nonce = 0")
  private val LeaseIdNonceDuplicatesError = GenericError("Attempt to create multiple contract leases with the same nonce")
  private val LeaseIdZeroNonceError       = GenericError("Attempt to create contract lease with nonce = 0")

  case class TxContractOpsData(issueOps: Seq[ContractIssueV1],
                               reissueOps: Seq[ContractReissueV1],
                               burnOps: Seq[ContractBurnV1],
                               transferOps: Seq[ContractTransferOutV1],
                               leaseOps: Seq[ContractLeaseV1],
                               leaseCancelOps: Seq[ContractCancelLeaseV1],
                               contractPaymentOps: Seq[ContractPaymentV1]) {

    lazy val issuedAssetsIds: Set[ByteStr] = issueOps.map(_.assetId).toSet

  }

  def extractTxContractOpsData(executedTx: ExecutedContractTransactionV3): TxContractOpsData = {
    val issueOpsBuilder       = Seq.newBuilder[ContractIssueV1]
    val reissueOpsBuilder     = Seq.newBuilder[ContractReissueV1]
    val burnOpsBuilder        = Seq.newBuilder[ContractBurnV1]
    val transferOpsBuilder    = Seq.newBuilder[ContractTransferOutV1]
    val leaseOpsBuilder       = Seq.newBuilder[ContractLeaseV1]
    val leaseCancelOpsBuilder = Seq.newBuilder[ContractCancelLeaseV1]
    val paymentOpsBuilder     = Seq.newBuilder[ContractPaymentV1]

    executedTx.assetOperations.foreach {
      case issue: ContractIssueV1 =>
        issueOpsBuilder += issue
      case reissue: ContractReissueV1 =>
        reissueOpsBuilder += reissue
      case burn: ContractBurnV1 =>
        burnOpsBuilder += burn
      case transfer: ContractTransferOutV1 =>
        transferOpsBuilder += transfer
      case lease: ContractLeaseV1 =>
        leaseOpsBuilder += lease
      case leaseCancel: ContractCancelLeaseV1 =>
        leaseCancelOpsBuilder += leaseCancel
      case contractPayment: ContractPaymentV1 =>
        paymentOpsBuilder += contractPayment
    }

    TxContractOpsData(
      issueOpsBuilder.result(),
      reissueOpsBuilder.result(),
      burnOpsBuilder.result,
      transferOpsBuilder.result(),
      leaseOpsBuilder.result(),
      leaseCancelOpsBuilder.result(),
      paymentOpsBuilder.result()
    )
  }

  sealed trait ContractTxExecutorType

  case object MiningExecutor     extends ContractTxExecutorType
  case object ValidatingExecutor extends ContractTxExecutorType
}
