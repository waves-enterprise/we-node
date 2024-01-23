package com.wavesenterprise.state.diffs.docker

import cats.implicits._
import cats.kernel.Monoid
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.acl.Role
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.BlockFeeCalculator.{CurrentBlockFeePart, CurrentBlockValidatorsFeePart}
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.docker.{ContractApiVersion, ContractInfo}
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings.EnabledForNativeTokens
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext.Default
import com.wavesenterprise.state.diffs.{assertBalanceInvariant, assertDiffAndState, assertDiffEither, produce}
import com.wavesenterprise.state.{ByteStr, ContractId, Portfolio}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.{AssetId, GenesisPermitTransaction, GenesisTransaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.{NoShrink, TransactionGen, crypto}
import monix.eval.Coeval
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper.ExtendedGen

import scala.concurrent.duration._

class ExecutedContractTransactionDiffTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with ContractTransactionGen
    with ExecutableTransactionGen
    with NoShrink
    with WithDomain {

  val MaxAssetOperationsCount = 100

  property("check ExecutedContractTransactionV1 diff and state") {
    forAll(preconditionsForV1) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffAndState(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV1) {
          case (blockDiff, state) =>
            val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets shouldBe Map.empty

            executedCreate.fee shouldBe 0
            executedCall.fee shouldBe 0

            val create    = executedCreate.tx.asInstanceOf[CreateContractTransaction with DockerContractTransaction]
            val call      = executedCall.tx.asInstanceOf[CallContractTransaction]
            val createFee = create.fee
            val callFee   = call.fee

            blockDiff.portfolios(executedSigner.toAddress.toAssetHolder).balance shouldBe (createFee + callFee)
            blockDiff.portfolios(create.sender.toAddress.toAssetHolder).balance shouldBe -createFee
            blockDiff.portfolios(call.sender.toAddress.toAssetHolder).balance shouldBe -callFee

            blockDiff.transactions.size shouldBe 4 // create, call and 2 executed
            blockDiff.transactionsMap.get(executedCreate.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(create.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(executedCall.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(call.id()) shouldNot be(None)

            state.addressPortfolio(executedSigner.toAddress).balance shouldBe (createFee + callFee)
            state.addressPortfolio(create.sender.toAddress).balance shouldBe (enoughAmount - createFee)
            state.addressPortfolio(call.sender.toAddress).balance shouldBe (enoughAmount - callFee)

            state.transactionInfo(executedCreate.id()).map(_._2) shouldBe Some(executedCreate)
            state.transactionInfo(create.id()).map(_._2) shouldBe Some(create)
            state.transactionInfo(executedCall.id()).map(_._2) shouldBe Some(executedCall)
            state.transactionInfo(call.id()).map(_._2) shouldBe Some(call)
            state.executedTxFor(create.id()) shouldBe Some(executedCreate)
            state.executedTxFor(call.id()) shouldBe Some(executedCall)

            state.contract(ContractId(create.id())) shouldBe Some(
              ContractInfo(Coeval.pure(create.sender), create.id(), DockerContract(create.image, create.imageHash), 1, active = true)
            )
            state.contractData(create.id(), Default) shouldBe Monoid.combine(ExecutedContractData(executedCreate.results.asMap),
                                                                             ExecutedContractData(executedCall.results.asMap))

        }
    }
  }

  property("check ExecutedContractTransactionV2 diff and state") {
    forAll(preconditionsForV2(minProofs = 1)) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffAndState(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
          case (blockDiff, state) =>
            val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets shouldBe Map.empty

            executedCreate.fee shouldBe 0
            executedCall.fee shouldBe 0

            val executedCreateValidators = executedCreate.validationProofs.map(_.validatorPublicKey.toAddress).toSet
            val executedCallValidators   = executedCall.validationProofs.map(_.validatorPublicKey.toAddress).toSet

            val create               = executedCreate.tx.asInstanceOf[CreateContractTransactionV4]
            val call                 = executedCall.tx.asInstanceOf[CallContractTransaction]
            val createFee            = create.fee
            val callFee              = call.fee
            val createValidatorsSize = executedCreateValidators.size
            val callValidatorsSize   = executedCallValidators.size
            val createValidatorFee   = if (createValidatorsSize > 0) CurrentBlockValidatorsFeePart(createFee) / createValidatorsSize else 0L
            val callValidatorFee     = if (callValidatorsSize > 0) CurrentBlockValidatorsFeePart(callFee) / callValidatorsSize else 0L

            val createSignerFee = createFee - createValidatorFee * executedCreateValidators.size
            val callSignerFee   = callFee - callValidatorFee * executedCallValidators.size

            blockDiff.portfolios(executedSigner.toAddress.toAssetHolder).balance shouldBe (createSignerFee + callSignerFee)
            blockDiff.portfolios(create.sender.toAddress.toAssetHolder).balance shouldBe -createFee
            blockDiff.portfolios(call.sender.toAddress.toAssetHolder).balance shouldBe -callFee

            val onlyCreateValidators = executedCreateValidators -- executedCallValidators
            val onlyCallValidators   = executedCallValidators -- executedCreateValidators
            val bothValidators       = executedCreateValidators.intersect(executedCallValidators)

            onlyCreateValidators.foreach { validator =>
              blockDiff.portfolios(validator.toAssetHolder).balance shouldBe createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              blockDiff.portfolios(validator.toAssetHolder).balance shouldBe callValidatorFee
            }
            bothValidators.foreach { validator =>
              blockDiff.portfolios(validator.toAssetHolder).balance shouldBe createValidatorFee + callValidatorFee
            }

            blockDiff.transactions.size shouldBe 4 // create, call and 2 executed
            blockDiff.transactionsMap.get(executedCreate.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(create.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(executedCall.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(call.id()) shouldNot be(None)

            state.addressPortfolio(executedSigner.toAddress).balance shouldBe (createSignerFee + callSignerFee)
            state.addressPortfolio(create.sender.toAddress).balance shouldBe (enoughAmount - createFee)
            state.addressPortfolio(call.sender.toAddress).balance shouldBe (enoughAmount - callFee)

            onlyCreateValidators.foreach { validator =>
              state.addressPortfolio(validator).balance shouldBe enoughAmount + createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              state.addressPortfolio(validator).balance shouldBe enoughAmount + callValidatorFee
            }
            bothValidators.foreach { validator =>
              state.addressPortfolio(validator).balance shouldBe enoughAmount + createValidatorFee + callValidatorFee
            }

            val genesisBalancesSum = genesisBlock.transactionData.collect {
              case GenesisTransaction(_, amount, _, _) => amount
            }.sum
            val afterOperationBalancesSum = state
              .collectAddressLposPortfolios({
                case (_, portfolio) => portfolio.balance
              })
              .values
              .sum
            genesisBalancesSum shouldBe afterOperationBalancesSum

            state.transactionInfo(executedCreate.id()).map(_._2) shouldBe Some(executedCreate)
            state.transactionInfo(create.id()).map(_._2) shouldBe Some(create)
            state.transactionInfo(executedCall.id()).map(_._2) shouldBe Some(executedCall)
            state.transactionInfo(call.id()).map(_._2) shouldBe Some(call)
            state.executedTxFor(create.id()) shouldBe Some(executedCreate)
            state.executedTxFor(call.id()) shouldBe Some(executedCall)

            state.contract(ContractId(create.id())) shouldBe Some(
              ContractInfo(Coeval.pure(create.sender),
                           create.id(),
                           DockerContract(create.image, create.imageHash),
                           1,
                           active = true,
                           validationPolicy = create.validationPolicy))
            state.contractData(create.id(), Default) shouldBe Monoid.combine(ExecutedContractData(executedCreate.results.asMap),
                                                                             ExecutedContractData(executedCall.results.asMap))
        }
    }
  }

  property("check ExecutedContractTransactionV2 diff and state with NG") {
    forAll(preconditionsForV2(minProofs = 1)) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffAndState(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2WithNG) {
          case (blockDiff, state) =>
            val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)

            val (currentBlockMinerFee, nextBlockMinerFee) = Monoid.combineAll(List(executedCreate, executedCall).map { tx =>
              val txFee           = tx.tx.fee
              val validatorsCount = tx.validationProofs.size
              val perValidatorFee = if (validatorsCount > 0) CurrentBlockValidatorsFeePart(txFee) / validatorsCount else 0L
              val minerFee        = txFee - perValidatorFee * validatorsCount
              val currentBlockFee = CurrentBlockFeePart(minerFee)
              val nextBlockFee    = minerFee - currentBlockFee
              currentBlockFee -> nextBlockFee
            })

            totalPortfolioDiff.balance shouldBe -nextBlockMinerFee
            totalPortfolioDiff.effectiveBalance shouldBe -nextBlockMinerFee
            totalPortfolioDiff.assets shouldBe Map.empty

            executedCreate.fee shouldBe 0
            executedCall.fee shouldBe 0

            val executedCreateValidators = executedCreate.validationProofs.map(_.validatorPublicKey.toAddress).toSet
            val executedCallValidators   = executedCall.validationProofs.map(_.validatorPublicKey.toAddress).toSet

            val create               = executedCreate.tx.asInstanceOf[CreateContractTransactionV4]
            val call                 = executedCall.tx.asInstanceOf[CallContractTransaction]
            val createFee            = create.fee
            val callFee              = call.fee
            val createValidatorsSize = executedCreateValidators.size
            val callValidatorsSize   = executedCallValidators.size
            val createValidatorFee   = if (createValidatorsSize > 0) CurrentBlockValidatorsFeePart(createFee) / createValidatorsSize else 0L
            val callValidatorFee     = if (callValidatorsSize > 0) CurrentBlockValidatorsFeePart(callFee) / callValidatorsSize else 0L

            blockDiff.portfolios(executedSigner.toAddress.toAssetHolder).balance shouldBe currentBlockMinerFee
            blockDiff.portfolios(create.sender.toAddress.toAssetHolder).balance shouldBe -createFee
            blockDiff.portfolios(call.sender.toAddress.toAssetHolder).balance shouldBe -callFee

            val onlyCreateValidators = executedCreateValidators -- executedCallValidators
            val onlyCallValidators   = executedCallValidators -- executedCreateValidators
            val bothValidators       = executedCreateValidators.intersect(executedCallValidators)

            onlyCreateValidators.foreach { validator =>
              blockDiff.portfolios(validator.toAssetHolder).balance shouldBe createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              blockDiff.portfolios(validator.toAssetHolder).balance shouldBe callValidatorFee
            }
            bothValidators.foreach { validator =>
              blockDiff.portfolios(validator.toAssetHolder).balance shouldBe createValidatorFee + callValidatorFee
            }

            blockDiff.transactions.size shouldBe 4 // create, call and 2 executed
            blockDiff.transactionsMap.get(executedCreate.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(create.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(executedCall.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(call.id()) shouldNot be(None)

            state.addressPortfolio(executedSigner.toAddress).balance shouldBe currentBlockMinerFee
            state.addressPortfolio(create.sender.toAddress).balance shouldBe (enoughAmount - createFee)
            state.addressPortfolio(call.sender.toAddress).balance shouldBe (enoughAmount - callFee)

            onlyCreateValidators.foreach { validator =>
              state.addressPortfolio(validator).balance shouldBe enoughAmount + createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              state.addressPortfolio(validator).balance shouldBe enoughAmount + callValidatorFee
            }
            bothValidators.foreach { validator =>
              state.addressPortfolio(validator).balance shouldBe enoughAmount + createValidatorFee + callValidatorFee
            }

            val genesisBalancesSum = genesisBlock.transactionData.collect {
              case GenesisTransaction(_, amount, _, _) => amount
            }.sum
            val afterOperationBalancesSum = state
              .collectAddressLposPortfolios({
                case (_, portfolio) => portfolio.balance
              })
              .values
              .sum
            genesisBalancesSum shouldBe (afterOperationBalancesSum + nextBlockMinerFee)

            state.transactionInfo(executedCreate.id()).map(_._2) shouldBe Some(executedCreate)
            state.transactionInfo(create.id()).map(_._2) shouldBe Some(create)
            state.transactionInfo(executedCall.id()).map(_._2) shouldBe Some(executedCall)
            state.transactionInfo(call.id()).map(_._2) shouldBe Some(call)
            state.executedTxFor(create.id()) shouldBe Some(executedCreate)
            state.executedTxFor(call.id()) shouldBe Some(executedCall)

            state.contract(ContractId(create.id())) shouldBe Some(
              ContractInfo(Coeval.pure(create.sender),
                           create.id(),
                           DockerContract(create.image, create.imageHash),
                           1,
                           active = true,
                           validationPolicy = create.validationPolicy)
            )
            state.contractData(create.id(), Default) shouldBe Monoid.combine(ExecutedContractData(executedCreate.results.asMap),
                                                                             ExecutedContractData(executedCall.results.asMap))
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 without active ContractValidations feature") {
    forAll(preconditionsForV2()) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV1) {
          _ should (produce(BlockchainFeature.ContractValidationsSupport.description) and produce(s"has not been activated yet"))
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 with invalid results hash") {
    forAll(preconditionsForV2(resultsHashTransformer = resultHash => ByteStr(resultHash.arr.reverse))) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
          _ should produce(s"Invalid results hash")
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 with not enough validation proofs") {
    val maxTxProofs               = 20
    val additionalValidatorsCount = math.ceil(maxTxProofs / ValidationPolicy.MajorityRatio).toInt + 1 - maxTxProofs
    val additionalValidatorsTxs = (for {
      startTime <- ntpTimestampGen.map(_ - 30.seconds.toMillis)
      count     <- Gen.choose(additionalValidatorsCount, additionalValidatorsCount + 10)
      accounts  <- Gen.listOfN(count, accountGen)
    } yield {
      accounts.zipWithIndex.map {
        case (account, i) =>
          GenesisPermitTransaction.create(account.toAddress, Role.ContractValidator, startTime + i).explicitGet()
      }
    }).generateSample()

    forAll(
      preconditionsForV2(
        validationPolicyGen = Gen.const(ValidationPolicy.Majority),
        validationProofsTransformer = _.tail,
        genesisTxsForValidatorBuilder = buildGenesisTxsForNotEnoughValidator,
        additionalGenesisTxs = additionalValidatorsTxs,
        maxProofs = maxTxProofs
      )) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
          _ should produce(s"Invalid validation proofs")
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 with invalid validation proof signature") {
    def signatureBreaker(proofs: List[ValidationProof]): List[ValidationProof] = {
      proofs.head.copy(signature = ByteStr(proofs.head.signature.arr.reverse)) :: proofs.tail
    }

    forAll(preconditionsForV2(validationProofsTransformer = signatureBreaker)) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
          _ should (produce(s"Invalid validator") and produce("signature"))
        }
    }
  }

  property("CompositeBlockchain#lastBlockContractValidators works well if validator permission changes in previous NG block") {
    val preconditionsGen: Gen[Seq[Block]] = for {
      timestamp    <- ntpTimestampGen.map(_ - 10.seconds.toMillis)
      permissioner <- accountGen
      additionalTxs = Seq(
        GenesisPermitTransaction.create(permissioner.toAddress, Role.Permissioner, timestamp).explicitGet(),
        GenesisTransaction.create(permissioner.toAddress, 10.0.west, timestamp).explicitGet()
      )
      (genesisBlock, executedSigner, executedCreate, executedCall) <-
        preconditionsForV2(validationPolicyGen = Gen.const(ValidationPolicy.Majority), additionalGenesisTxs = additionalTxs)
      (addValidator, removeValidator) <- validatorPermitsGen(permissioner)
      blockTimestamp       = timestamp + 20.seconds.toMillis
      emptyBlock1          = TestBlock.create(blockTimestamp, genesisBlock.uniqueId, Seq.empty)
      executedCreateBlock  = TestBlock.create(blockTimestamp + 3.seconds.toMillis, emptyBlock1.uniqueId, Seq(executedCreate), executedSigner)
      emptyBlock2          = TestBlock.create(blockTimestamp + 4.seconds.toMillis, executedCreateBlock.uniqueId, Seq.empty)
      addValidatorBlock    = TestBlock.create(blockTimestamp + 5.seconds.toMillis, emptyBlock2.uniqueId, Seq(addValidator))
      emptyBlock3          = TestBlock.create(blockTimestamp + 6.seconds.toMillis, addValidatorBlock.uniqueId, Seq.empty)
      removeValidatorBlock = TestBlock.create(blockTimestamp + 7.seconds.toMillis, emptyBlock3.uniqueId, Seq(removeValidator))
      executedCallBlock    = TestBlock.create(blockTimestamp + 8.seconds.toMillis, removeValidatorBlock.uniqueId, Seq(executedCall), executedSigner)
    } yield Seq(genesisBlock, emptyBlock1, executedCreateBlock, emptyBlock2, addValidatorBlock, emptyBlock3, removeValidatorBlock, executedCallBlock)

    val custom   = DefaultWESettings.blockchain.custom.copy(functionality = fsForV2WithNG)
    val settings = DefaultWESettings.copy(blockchain = DefaultWESettings.blockchain.copy(custom = custom))

    forAll(preconditionsGen) { blocks =>
      withDomain(settings) { d =>
        blocks.foreach { block =>
          d.appendBlock(block, ConsensusPostAction.NoAction)
        }
      }
    }
  }

  property(
    "CallContractTransactionV5 with transfers in and contract's asset operations (Issue, Reissue, Burn and TransferOut) preserves balance invariant") {

    def setup(withoutAssetOperations: Boolean, minContractIssues: Int, contractOperationSize: Int) = {
      def genTransfersInParamsBy(assetLimits: List[(Option[AssetId], Long)]): Seq[(Option[AssetId], Long)] = assetLimits.map {
        case (assetId, maxAmount) =>
          assetId -> Gen.choose(1, maxAmount / (contractOperationSize + 1)).sample.get
      }

      def genValidationParams: Gen[(ValidationPolicy, Int, List[PrivateKeyAccount])] =
        for {
          validationPolicy <- Gen.oneOf(ValidationPolicy.Majority, ValidationPolicy.Any)
          proofsCount      <- Gen.choose(5, 10)
          validators <- Gen
            .listOfN(math.floor(proofsCount / ValidationPolicy.MajorityRatio).toInt, accountGen)
        } yield (validationPolicy, proofsCount, validators)

      for {
        genesisTime                                 <- ntpTimestampGen.map(_ - 1.minute.toMillis)
        contractCreator                             <- accountGen
        contractCaller                              <- accountGen
        executedSigner                              <- accountGen
        (validationPolicy, proofsCount, validators) <- genValidationParams
        creditWestsToContractByCreator              <- positiveLongGen
        creditWestsToContractByCaller               <- positiveLongGen
        create <- createContractV5Gen(
          None,
          (None, createTxFeeGen),
          contractCreator,
          validationPolicy,
          ContractApiVersion.Initial,
          genTransfersInParamsBy(List(none -> creditWestsToContractByCreator)).toList
        )
        executedCreate <- executedTxV3ParamGen(executedSigner, create, proofsCount = proofsCount, specifiedValidators = validators)
        call <- genCallContractWithTransfers(
          contractCaller,
          create.contractId,
          genTransfersInParamsBy(List(none -> creditWestsToContractByCaller)).toList
        )
        contractOperations <- listAssetOperation(call.id(), minContractIssues, contractOperationSize, withoutAssetOperations)
        executedCall <- executedTxV3ParamWithOperationsGen(
          signer = executedSigner,
          tx = call,
          proofsCount = proofsCount,
          specifiedValidators = validators,
          assetOperations = contractOperations
        )
        genesisForCreateAccount = GenesisTransaction.create(contractCreator.toAddress, enoughAmount, genesisTime).explicitGet()
        genesisForCallAccount   = GenesisTransaction.create(contractCaller.toAddress, enoughAmount, genesisTime).explicitGet()
        genesisForValidators    = validators.flatMap(validator => buildGenesisTxsForValidator(validator.toAddress, genesisTime))
        genesisBlock            = block(Seq(genesisForCreateAccount, genesisForCallAccount) ++ genesisForValidators)
      } yield (genesisBlock, executedSigner, executedCreate, executedCall)
    }

    def checkInvariantWithParametrizedTransfers(withoutAssetOperations: Boolean, minContractIssues: Int, contractOperationSize: Int): Unit = {
      forAll(setup(withoutAssetOperations, minContractIssues, contractOperationSize)) {
        case (genesisBlock, executedSigner, executedCreate, executedCall) =>
          assertDiffAndState(Seq(genesisBlock), block(executedSigner, Seq(executedCreate, executedCall)), EnabledForNativeTokens) {
            case (blockDiff, newState) =>
              if (withoutAssetOperations) assertBalanceInvariant(blockDiff)
              else {
                val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
                totalPortfolioDiff.balance shouldBe 0
                totalPortfolioDiff.effectiveBalance shouldBe 0

                val contractId = executedCreate.tx.contractId

                val contractAssetManipulationsMap = executedCall.assetOperations
                  .collect {
                    case ContractAssetOperation.ContractIssueV1(_, _, _, _, _, quantity, _, _, nonce) =>
                      ByteStr(crypto.fastHash(executedCall.tx.id().arr :+ nonce)).some -> quantity
                    case ContractAssetOperation.ContractReissueV1(_, _, assetId, quantity, _) =>
                      assetId.some -> quantity
                    case ContractAssetOperation.ContractBurnV1(_, _, assetId, amount) if assetId.isDefined => // without burning wests
                      assetId -> -amount
                  }
                  .foldLeft(Map.empty[AssetId, Long]) {
                    case (resMap, (optAssetId, assetAmountEffect)) =>
                      resMap + (optAssetId.get -> (resMap.getOrElse(optAssetId.get, 0L) + assetAmountEffect))
                  }

                val assetTransfersMap = executedCall.assetOperations
                  .collect {
                    case op: ContractAssetOperation.ContractTransferOutV1 if op.assetId.isDefined => op
                  }
                  .groupBy(_.assetId)
                  .mapValues(_.map(transferOp => (transferOp.recipient.asInstanceOf[Address], transferOp.amount)))

                contractAssetManipulationsMap.foreach {
                  case (assetId, totalIssued) =>
                    (totalIssued - assetTransfersMap.getOrElse(Some(assetId), List.empty).map(_._2).sum) shouldBe newState
                      .contractPortfolio(ContractId(contractId))
                      .assets(assetId)
                }
              }
          }
      }
    }

    (10 to MaxAssetOperationsCount by 10).foreach { size =>
      checkInvariantWithParametrizedTransfers(withoutAssetOperations = true, minContractIssues = 0, contractOperationSize = size)
    }

    (10 to MaxAssetOperationsCount by 10).foreach { size =>
      checkInvariantWithParametrizedTransfers(withoutAssetOperations = false,
                                              minContractIssues = math.min(size / 5, 10),
                                              contractOperationSize = size)
    }
  }
}
