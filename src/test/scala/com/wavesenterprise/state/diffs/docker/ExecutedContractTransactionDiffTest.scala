package com.wavesenterprise.state.diffs.docker

import cats.implicits.{catsKernelStdCommutativeGroupForTuple2, catsKernelStdGroupForLong}
import cats.kernel.Monoid
import com.wavesenterprise.NoShrink
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.block.Block
import com.wavesenterprise.block.BlockFeeCalculator.{CurrentBlockFeePart, CurrentBlockValidatorsFeePart}
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.docker.{ContractApiVersion, ContractInfo}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.DefaultWESettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext.Default
import com.wavesenterprise.state.diffs.{assertDiffAndState, assertDiffEi, produce}
import com.wavesenterprise.state.{ByteStr, DataEntry, Portfolio}
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.{GenesisPermitTransaction, GenesisTransaction, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import monix.eval.Coeval
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._

import scala.concurrent.duration._

class ExecutedContractTransactionDiffTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with ContractTransactionGen
    with NoShrink
    with WithDomain {

  private val enoughAmount = com.wavesenterprise.state.diffs.ENOUGH_AMT / 100

  val preconditionsForV1: Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV1, ExecutedContractTransactionV1)] = for {
    genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
    executedSigner <- accountGen
    create         <- createContractV2ParamGenWithoutSponsoring
    executedCreate <- executedContractV1ParamGen(executedSigner, create)
    callSigner     <- accountGen
    call           <- callContractV1ParamGen(callSigner, create.contractId)
    executedCall   <- executedContractV1ParamGen(executedSigner, call)
    genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, enoughAmount, genesisTime).explicitGet()
    genesisForCallAccount   = GenesisTransaction.create(call.sender.toAddress, enoughAmount, genesisTime).explicitGet()
    genesisBlock            = TestBlock.create(Seq(genesisForCreateAccount, genesisForCallAccount))
  } yield (genesisBlock, executedSigner, executedCreate, executedCall)

  val fsForV1: FunctionalitySettings = TestFunctionalitySettings.Enabled

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

            val create    = executedCreate.tx.asInstanceOf[CreateContractTransaction]
            val call      = executedCall.tx.asInstanceOf[CallContractTransaction]
            val createFee = create.fee
            val callFee   = call.fee

            blockDiff.portfolios(executedSigner.toAddress).balance shouldBe (createFee + callFee)
            blockDiff.portfolios(create.sender.toAddress).balance shouldBe -createFee
            blockDiff.portfolios(call.sender.toAddress).balance shouldBe -callFee

            blockDiff.transactions.size shouldBe 4 // create, call and 2 executed
            blockDiff.transactionsMap.get(executedCreate.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(create.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(executedCall.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(call.id()) shouldNot be(None)

            state.portfolio(executedSigner.toAddress).balance shouldBe (createFee + callFee)
            state.portfolio(create.sender.toAddress).balance shouldBe (enoughAmount - createFee)
            state.portfolio(call.sender.toAddress).balance shouldBe (enoughAmount - callFee)

            state.transactionInfo(executedCreate.id()).map(_._2) shouldBe Some(executedCreate)
            state.transactionInfo(create.id()).map(_._2) shouldBe Some(create)
            state.transactionInfo(executedCall.id()).map(_._2) shouldBe Some(executedCall)
            state.transactionInfo(call.id()).map(_._2) shouldBe Some(call)
            state.executedTxFor(create.id()) shouldBe Some(executedCreate)
            state.executedTxFor(call.id()) shouldBe Some(executedCall)

            state.contract(create.id()) shouldBe Some(
              ContractInfo(Coeval.pure(create.sender), create.id(), create.image, create.imageHash, 1, active = true))
            state.contractData(create.id(), Default) shouldBe Monoid.combine(ExecutedContractData(executedCreate.results.asMap),
                                                                             ExecutedContractData(executedCall.results.asMap))

        }
    }
  }

  def buildGenesisTxsForValidator(address: Address, time: Long): List[Transaction] = {
    List(
      GenesisTransaction.create(address, enoughAmount, time).explicitGet(),
      GenesisPermitTransaction.create(address, Role.Miner, time).explicitGet(),
      GenesisPermitTransaction.create(address, Role.ContractValidator, time).explicitGet()
    )
  }

  def preconditionsForV2(
      resultsHashTransformer: ByteStr => ByteStr = identity,
      validationPolicyGen: Gen[ValidationPolicy] = Gen.oneOf(ValidationPolicy.Majority, ValidationPolicy.Any),
      apiVersion: ContractApiVersion = ContractApiVersion.Initial,
      validationProofsTransformer: List[ValidationProof] => List[ValidationProof] = identity,
      genesisTxsForValidatorBuilder: (Address, Long) => Seq[Transaction] = buildGenesisTxsForValidator,
      additionalGenesisTxs: Seq[Transaction] = Seq.empty,
      minProofs: Int = 10,
      maxProofs: Int = 30
  ): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV2, ExecutedContractTransactionV2)] =
    for {
      genesisTime      <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner   <- accountGen
      validationPolicy <- validationPolicyGen
      create <- createContractV4ParamGen(Gen.const(None),
                                         (Gen.const(None), createTxFeeGen),
                                         accountGen,
                                         Gen.const(validationPolicy),
                                         Gen.const(apiVersion))
      proofsCount <- Gen.choose(minProofs, maxProofs)
      validators  <- Gen.listOfN(math.floor(proofsCount / ValidationPolicy.MajorityRatio).toInt, accountGen)
      executedCreate <- executedContractV2ParamGen(executedSigner,
                                                   create,
                                                   resultsHashTransformer,
                                                   validationProofsTransformer,
                                                   proofsCount,
                                                   validators)
      callSigner   <- accountGen
      call         <- callContractV1ParamGen(callSigner, create.contractId)
      executedCall <- executedContractV2ParamGen(executedSigner, call, resultsHashTransformer, validationProofsTransformer, proofsCount, validators)
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      genesisForCallAccount   = GenesisTransaction.create(call.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      genesisForValidators    = validators.flatMap(validator => genesisTxsForValidatorBuilder(validator.toAddress, genesisTime))
      genesisBlock            = TestBlock.create(Seq(genesisForCreateAccount, genesisForCallAccount) ++ genesisForValidators ++ additionalGenesisTxs)
    } yield (genesisBlock, executedSigner, executedCreate, executedCall)

  val fsForV2: FunctionalitySettings = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures +
      (BlockchainFeature.ContractValidationsSupport.id -> 0)
  )

  val fsForV2WithNG: FunctionalitySettings = TestFunctionalitySettings.EnabledForAtomics.copy(
    preActivatedFeatures = TestFunctionalitySettings.EnabledForAtomics.preActivatedFeatures ++
      Map(BlockchainFeature.ContractValidationsSupport.id -> 0, BlockchainFeature.NG.id -> 0)
  )

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

            blockDiff.portfolios(executedSigner.toAddress).balance shouldBe (createSignerFee + callSignerFee)
            blockDiff.portfolios(create.sender.toAddress).balance shouldBe -createFee
            blockDiff.portfolios(call.sender.toAddress).balance shouldBe -callFee

            val onlyCreateValidators = executedCreateValidators -- executedCallValidators
            val onlyCallValidators   = executedCallValidators -- executedCreateValidators
            val bothValidators       = executedCreateValidators.intersect(executedCallValidators)

            onlyCreateValidators.foreach { validator =>
              blockDiff.portfolios(validator).balance shouldBe createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              blockDiff.portfolios(validator).balance shouldBe callValidatorFee
            }
            bothValidators.foreach { validator =>
              blockDiff.portfolios(validator).balance shouldBe createValidatorFee + callValidatorFee
            }

            blockDiff.transactions.size shouldBe 4 // create, call and 2 executed
            blockDiff.transactionsMap.get(executedCreate.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(create.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(executedCall.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(call.id()) shouldNot be(None)

            state.portfolio(executedSigner.toAddress).balance shouldBe (createSignerFee + callSignerFee)
            state.portfolio(create.sender.toAddress).balance shouldBe (enoughAmount - createFee)
            state.portfolio(call.sender.toAddress).balance shouldBe (enoughAmount - callFee)

            onlyCreateValidators.foreach { validator =>
              state.portfolio(validator).balance shouldBe enoughAmount + createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              state.portfolio(validator).balance shouldBe enoughAmount + callValidatorFee
            }
            bothValidators.foreach { validator =>
              state.portfolio(validator).balance shouldBe enoughAmount + createValidatorFee + callValidatorFee
            }

            val genesisBalancesSum = genesisBlock.transactionData.collect {
              case GenesisTransaction(_, amount, _, _) => amount
            }.sum
            val afterOperationBalancesSum = state
              .collectLposPortfolios {
                case (_, portfolio) => portfolio.balance
              }
              .values
              .sum
            genesisBalancesSum shouldBe afterOperationBalancesSum

            state.transactionInfo(executedCreate.id()).map(_._2) shouldBe Some(executedCreate)
            state.transactionInfo(create.id()).map(_._2) shouldBe Some(create)
            state.transactionInfo(executedCall.id()).map(_._2) shouldBe Some(executedCall)
            state.transactionInfo(call.id()).map(_._2) shouldBe Some(call)
            state.executedTxFor(create.id()) shouldBe Some(executedCreate)
            state.executedTxFor(call.id()) shouldBe Some(executedCall)

            state.contract(create.id()) shouldBe Some(
              ContractInfo(Coeval.pure(create.sender),
                           create.id(),
                           create.image,
                           create.imageHash,
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

            blockDiff.portfolios(executedSigner.toAddress).balance shouldBe currentBlockMinerFee
            blockDiff.portfolios(create.sender.toAddress).balance shouldBe -createFee
            blockDiff.portfolios(call.sender.toAddress).balance shouldBe -callFee

            val onlyCreateValidators = executedCreateValidators -- executedCallValidators
            val onlyCallValidators   = executedCallValidators -- executedCreateValidators
            val bothValidators       = executedCreateValidators.intersect(executedCallValidators)

            onlyCreateValidators.foreach { validator =>
              blockDiff.portfolios(validator).balance shouldBe createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              blockDiff.portfolios(validator).balance shouldBe callValidatorFee
            }
            bothValidators.foreach { validator =>
              blockDiff.portfolios(validator).balance shouldBe createValidatorFee + callValidatorFee
            }

            blockDiff.transactions.size shouldBe 4 // create, call and 2 executed
            blockDiff.transactionsMap.get(executedCreate.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(create.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(executedCall.id()) shouldNot be(None)
            blockDiff.transactionsMap.get(call.id()) shouldNot be(None)

            state.portfolio(executedSigner.toAddress).balance shouldBe currentBlockMinerFee
            state.portfolio(create.sender.toAddress).balance shouldBe (enoughAmount - createFee)
            state.portfolio(call.sender.toAddress).balance shouldBe (enoughAmount - callFee)

            onlyCreateValidators.foreach { validator =>
              state.portfolio(validator).balance shouldBe enoughAmount + createValidatorFee
            }
            onlyCallValidators.foreach { validator =>
              state.portfolio(validator).balance shouldBe enoughAmount + callValidatorFee
            }
            bothValidators.foreach { validator =>
              state.portfolio(validator).balance shouldBe enoughAmount + createValidatorFee + callValidatorFee
            }

            val genesisBalancesSum = genesisBlock.transactionData.collect {
              case GenesisTransaction(_, amount, _, _) => amount
            }.sum
            val afterOperationBalancesSum = state
              .collectLposPortfolios {
                case (_, portfolio) => portfolio.balance
              }
              .values
              .sum
            genesisBalancesSum shouldBe (afterOperationBalancesSum + nextBlockMinerFee)

            state.transactionInfo(executedCreate.id()).map(_._2) shouldBe Some(executedCreate)
            state.transactionInfo(create.id()).map(_._2) shouldBe Some(create)
            state.transactionInfo(executedCall.id()).map(_._2) shouldBe Some(executedCall)
            state.transactionInfo(call.id()).map(_._2) shouldBe Some(call)
            state.executedTxFor(create.id()) shouldBe Some(executedCreate)
            state.executedTxFor(call.id()) shouldBe Some(executedCall)

            state.contract(create.id()) shouldBe Some(
              ContractInfo(Coeval.pure(create.sender),
                           create.id(),
                           create.image,
                           create.imageHash,
                           1,
                           active = true,
                           validationPolicy = create.validationPolicy))
            state.contractData(create.id(), Default) shouldBe Monoid.combine(ExecutedContractData(executedCreate.results.asMap),
                                                                             ExecutedContractData(executedCall.results.asMap))
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 without active ContractValidations feature") {
    forAll(preconditionsForV2()) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEi(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV1) {
          _ should (produce(BlockchainFeature.ContractValidationsSupport.description) and produce(s"has not been activated yet"))
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 with invalid results hash") {
    forAll(preconditionsForV2(resultsHashTransformer = resultHash => ByteStr(resultHash.arr.reverse))) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEi(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
          _ should produce(s"Invalid results hash")
        }
    }
  }

  property("Cannot use ExecutedContractTransactionV2 with not enough validation proofs") {
    def buildGenesisTxsForValidator(address: Address, time: Long): List[Transaction] =
      GenesisPermitTransaction.create(address, Role.ContractValidator, time).explicitGet() :: Nil

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
        genesisTxsForValidatorBuilder = buildGenesisTxsForValidator,
        additionalGenesisTxs = additionalValidatorsTxs,
        maxProofs = maxTxProofs
      )) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEi(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
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
        assertDiffEi(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedCall)), fsForV2) {
          _ should (produce(s"Invalid validator") and produce("signature"))
        }
    }
  }

  property("CompositeBlockchain#lastBlockContractValidators works well if validator permission changes in previous NG block") {
    def validatorPermitsGen(permissioner: PrivateKeyAccount): Gen[(PermitTransaction, PermitTransaction)] = {
      for {
        time   <- ntpTimestampGen
        target <- accountGen.map(_.toAddress)
        add <- permitTransactionV1Gen(
          accountGen = Gen.const(permissioner),
          targetGen = Gen.const(target),
          permissionOpGen = Gen.const(PermissionOp(OpType.Add, Role.ContractValidator, time, None)),
          timestampGen = Gen.const(time)
        )
        remove <- permitTransactionV1Gen(
          accountGen = Gen.const(permissioner),
          targetGen = Gen.const(target),
          permissionOpGen = Gen.const(PermissionOp(OpType.Remove, Role.ContractValidator, time, None)),
          timestampGen = Gen.const(time)
        )
      } yield (add, remove)
    }

    val preconditionsGen: Gen[Seq[Block]] = for {
      timestamp    <- ntpTimestampGen.map(_ - 10.seconds.toMillis)
      permissioner <- accountGen
      additionalTxs = Seq(
        GenesisPermitTransaction.create(permissioner.toAddress, Role.Permissioner, timestamp).explicitGet(),
        GenesisTransaction.create(permissioner.toAddress, 10.0.west, timestamp).explicitGet()
      )
      (genesisBlock, executedSigner, executedCreate, executedCall) <- preconditionsForV2(validationPolicyGen = Gen.const(ValidationPolicy.Majority),
                                                                                         additionalGenesisTxs = additionalTxs)
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

  implicit class ListExt(list: List[DataEntry[_]]) {
    def asMap: Map[String, DataEntry[_]] = list.map(e => e.key -> e).toMap
  }
}
