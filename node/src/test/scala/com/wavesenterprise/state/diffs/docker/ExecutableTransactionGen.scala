package com.wavesenterprise.state.diffs.docker

import cats.implicits._
import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.block.Block
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFees.{defaultFees => fees}
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.ENOUGH_AMT
import com.wavesenterprise.state.diffs.docker.ExecutableTransactionGen.ExecutedTxV3TestData
import com.wavesenterprise.state.{ByteStr, DataEntry, Sponsorship}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.assets.IssueTransaction
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.apache.commons.codec.digest.DigestUtils
import org.scalacheck.Gen

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

trait ExecutableTransactionGen { self: TransactionGen with ContractTransactionGen =>

  private[docker] val enoughAmount        = ENOUGH_AMT / 100
  private[docker] val CreateFee           = fees.forTxType(CreateContractTransaction.typeId)
  private[docker] val CallFee             = fees.forTxType(CallContractTransaction.typeId)
  private[docker] val UpdateFee           = fees.forTxType(UpdateContractTransaction.typeId)
  private[docker] val DisableFee          = fees.forTxType(DisableContractTransaction.typeId)
  private[docker] val CreateFeeInAsset    = Sponsorship.fromWest(CreateFee)
  private[docker] val CallFeeInAsset      = Sponsorship.fromWest(CallFee)
  private[docker] val UpdateFeeInAsset    = Sponsorship.fromWest(UpdateFee)
  private[docker] val DisableFeeInAsset   = Sponsorship.fromWest(DisableFee)
  private[docker] val AssetTransferAmount = 100 * CreateFeeInAsset

  private val ParamsMinCount   = 0
  private val ParamsMaxCount   = 100
  private val DataEntryMaxSize = 10

  val fs: FunctionalitySettings = TestFunctionalitySettings.Enabled

  val fsForSponsored: FunctionalitySettings = FunctionalitySettings(
    featureCheckBlocksPeriod = 2,
    blocksForFeatureActivation = 1,
    preActivatedFeatures = BlockchainFeature.implemented.filterNot(_ == BlockchainFeature.ContractValidationsSupport.id).map(_ -> 0).toMap
  )

  val fsForV1: FunctionalitySettings = TestFunctionalitySettings.Enabled

  val fsForV2: FunctionalitySettings = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures +
      (BlockchainFeature.ContractValidationsSupport.id -> 0)
  )

  val fsForV2WithNG: FunctionalitySettings = TestFunctionalitySettings.EnabledForAtomics.copy(
    preActivatedFeatures = TestFunctionalitySettings.EnabledForAtomics.preActivatedFeatures ++
      Map(BlockchainFeature.ContractValidationsSupport.id -> 0, BlockchainFeature.NG.id -> 0)
  )

  val fsForV4WithContractValidation: FunctionalitySettings = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures +
      (BlockchainFeature.ContractValidationsSupport.id -> 0)
  )

  implicit class ListExt(list: List[DataEntry[_]]) {
    def asMap: Map[String, DataEntry[_]] = list.map(e => e.key -> e).toMap
  }

  def buildGenesisTxsForValidator(address: Address, time: Long): List[Transaction] = {
    List(
      GenesisTransaction.create(address, enoughAmount, time).explicitGet(),
      GenesisPermitTransaction.create(address, Role.Miner, time).explicitGet(),
      GenesisPermitTransaction.create(address, Role.ContractValidator, time).explicitGet()
    )
  }

  def buildGenesisTxsForNotEnoughValidator(address: Address, time: Long): List[Transaction] =
    GenesisPermitTransaction.create(address, Role.ContractValidator, time).explicitGet() :: Nil

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

  val baseUpdatePreconditions: Gen[(Seq[Block], PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
    createSigner   <- accountGen
    create         <- createContractV2ParamGen(createSigner)
    executedCreate <- executedTxV1ParamGen(createSigner, create)
    update         <- Gen.oneOf(updateContractV1ParamGen(createSigner, create), updateContractV2ParamGenWithoutSponsoring(createSigner, create))
    executedUpdate <- executedForUpdateGen(createSigner, update)
    genesisForCreateAccount = GenesisTransaction.create(createSigner.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
    genesisBlock            = block(createSigner, Seq(genesisForCreateAccount))
    executedTxBlock         = block(createSigner, Seq(executedCreate))
  } yield (Seq(genesisBlock, executedTxBlock), createSigner, executedUpdate)

  val withCallTxV1: Gen[(Seq[Block], CallContractTransactionV1, Block)] = for {
    (blocks, account, update) <- baseUpdatePreconditions
    updateBlock = block(account, Seq(update))
    callTxV1     <- callContractV1ParamGen(account, update.tx.contractId)
    executedCall <- executedTxV1ParamGen(account, callTxV1)
    executedTxBlock = block(account, Seq(executedCall))
  } yield (blocks :+ updateBlock, callTxV1, executedTxBlock)

  val preconditionsForV1: Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV1, ExecutedContractTransactionV1)] = for {
    genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
    executedSigner <- accountGen
    callSigner     <- accountGen
    create         <- createContractV2ParamGenWithoutSponsoring
    call           <- callContractV1ParamGen(callSigner, create.contractId)
    executedCreate <- executedTxV1ParamGen(executedSigner, create)
    executedCall   <- executedTxV1ParamGen(executedSigner, call)
    genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, enoughAmount, genesisTime).explicitGet()
    genesisForCallAccount   = GenesisTransaction.create(call.sender.toAddress, enoughAmount, genesisTime).explicitGet()
    genesisBlock            = block(Seq(genesisForCreateAccount, genesisForCallAccount))
  } yield (genesisBlock, executedSigner, executedCreate, executedCall)

  val withCallTxV2: Gen[(Seq[Block], CallContractTransactionV2, Block)] = for {
    (blocks, account, update) <- baseUpdatePreconditions
    updateBlock = block(account, Seq(update))
    callTxV2     <- callContractV2ParamGen(account, update.tx.contractId, 2)
    executedCall <- executedTxV1ParamGen(account, callTxV2)
    executedTxBlock = block(account, Seq(executedCall))
  } yield (blocks :+ updateBlock, callTxV2, executedTxBlock)

  val withCreateV2: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    genesisTime                                        <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    (contractDeveloper, genesisForCreateAccount)       <- accountGenesisGen(genesisTime)
    (issuer, assetId, sponsorGenesis, sponsorBlock, _) <- issueAndSendSponsorAssetsGen(contractDeveloper, CreateFeeInAsset, genesisTime)
    create                                             <- createContractV2ParamGen(Gen.const(Some(assetId)), Gen.const(CreateFeeInAsset), Gen.const(contractDeveloper))
    executedSigner                                     <- accountGen
    executedCreate                                     <- executedTxV1ParamGen(executedSigner, create)
    genesisBlock = block(Seq(sponsorGenesis, genesisForCreateAccount))
  } yield (Seq(genesisBlock, sponsorBlock), issuer, assetId, contractDeveloper, executedSigner, executedCreate)

  val withUpdateV2: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    (blocks, issuer, assetId, contractDeveloper, executedSigner, executedCreate) <- withCreateV2
    executedCreateBlock = block(executedSigner, Seq(executedCreate))
    update         <- updateContractV2ParamGen(Gen.const(Some(assetId)), Gen.const(UpdateFeeInAsset), contractDeveloper, executedCreate.tx.contractId)
    executedUpdate <- executedTxV1ParamGen(executedSigner, update)
  } yield (blocks :+ executedCreateBlock, issuer, assetId, executedSigner, executedUpdate)

  val withDisableV2: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, DisableContractTransactionV2)] = for {
    (blocks, issuer, assetId, contractDeveloper, executedSigner, executedCreate) <- withCreateV2
    executedCreateBlock = block(executedSigner, Seq(executedCreate))
    disable <- disableContractV2ParamGen(Gen.const(Some(assetId)), Gen.const(DisableFeeInAsset), contractDeveloper, executedCreate.tx.contractId)
  } yield (blocks :+ executedCreateBlock, issuer, assetId, executedSigner, disable)

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
                                         Gen.const(None),
                                         createTxFeeGen,
                                         accountGen,
                                         Gen.const(validationPolicy),
                                         Gen.const(apiVersion))
      proofsCount    <- Gen.choose(minProofs, maxProofs)
      validators     <- Gen.listOfN(math.floor(proofsCount / ValidationPolicy.MajorityRatio).toInt, accountGen)
      executedCreate <- executedTxV2ParamGen(executedSigner, create, resultsHashTransformer, validationProofsTransformer, proofsCount, validators)
      callSigner     <- accountGen
      call           <- callContractV1ParamGen(callSigner, create.contractId)
      executedCall   <- executedTxV2ParamGen(executedSigner, call, resultsHashTransformer, validationProofsTransformer, proofsCount, validators)
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      genesisForCallAccount   = GenesisTransaction.create(call.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      genesisForValidators    = validators.flatMap(validator => genesisTxsForValidatorBuilder(validator.toAddress, genesisTime))
      genesisBlock            = block(Seq(genesisForCreateAccount, genesisForCallAccount) ++ genesisForValidators ++ additionalGenesisTxs)
    } yield (genesisBlock, executedSigner, executedCreate, executedCall)

  def preconditionsV2(
      validationPolicy: ValidationPolicy,
      proofsCount: Int,
  ): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV2, ExecutedContractTransactionV2)] =
    for {
      genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner <- accountGen
      create <- createContractV4ParamGen(Gen.const(None),
                                         Gen.const(None),
                                         createTxFeeGen,
                                         executedSigner,
                                         Gen.const(ValidationPolicy.Any),
                                         contractApiVersionGen)
      executedCreate <- executedTxV2ParamGen(executedSigner, create, identity, identity, proofsCount)
      update <- updateContractV4ParamGen(
        atomicBadgeGen = Gen.const(None),
        optAssetIdGen = Gen.const(None),
        amountGen = updateTxFeeGen,
        signerGen = executedSigner,
        validationPolicyGen = Gen.const(validationPolicy),
        contractApiVersionGen = contractApiVersionGen,
        contractIdGen = Gen.const(create.contractId),
      )
      executedUpdate <- executedTxV2ParamGen(executedSigner, update, identity, identity, proofsCount)
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
      genesisBlock            = block(Seq(genesisForCreateAccount))
    } yield (genesisBlock, executedSigner, executedCreate, executedUpdate)

  val withCallTxV3: Gen[(Seq[Block], CallContractTransactionV3, Block)] = for {
    (blocks, account, update) <- baseUpdatePreconditions
    updateBlock = block(account, Seq(update))
    callTxV3     <- callContractV3ParamGenWithoutSponsoring(account, update.tx.contractId, 2)
    executedCall <- executedTxV1ParamGen(account, callTxV3)
    executedTxBlock = block(account, Seq(executedCall))
  } yield (blocks :+ updateBlock, callTxV3, executedTxBlock)

  val withCallV3: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    (blocks, issuer, assetId, contractDeveloper, executedSigner, executedCreate) <- withCreateV2
    executedCreateBlock = block(executedSigner, Seq(executedCreate))
    call         <- callContractV3ParamGen(Gen.some(assetId), Gen.const(CallFeeInAsset), contractDeveloper, executedCreate.tx.contractId, 1)
    executedCall <- executedTxV1ParamGen(executedSigner, call)
  } yield (blocks :+ executedCreateBlock, issuer, assetId, executedSigner, executedCall)

  def preconditionsForCreateV4AndExecutedV2(
      validationPolicy: ValidationPolicy,
      proofsCount: Int,
      additionalGenesisTxs: Seq[Transaction] = Seq.empty,
  ): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV2)] =
    for {
      genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner <- accountGen
      create <- createContractV4ParamGen(Gen.const(None),
                                         Gen.const(None),
                                         createTxFeeGen,
                                         accountGen,
                                         Gen.const(validationPolicy),
                                         contractApiVersionGen)
      executedCreate <- executedTxV2ParamGen(executedSigner, create, identity, identity, proofsCount)
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
      genesisBlock            = block(Seq(genesisForCreateAccount) ++ additionalGenesisTxs)
    } yield (genesisBlock, executedSigner, executedCreate)

  def preconditionsForCreateV5AndCallV3ExecutedV3(
      minProofs: Int = 10,
      maxProofs: Int = 30,
      maxTransfers: Int = 10): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV3, ExecutedContractTransactionV3)] =
    for {
      genesisTime      <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner   <- accountGen
      validationPolicy <- validationPolicyGen
      create           <- createContractV5ParamGen(None, None, createTxFeeGen, accountGen, validationPolicy, ContractApiVersion.Initial, maxTransfers, None)
      proofsCount      <- Gen.choose(minProofs, maxProofs)
      validators       <- Gen.listOfN(math.floor(proofsCount / ValidationPolicy.MajorityRatio).toInt, accountGen)
      executedCreate   <- executedTxV3ParamGen(executedSigner, create, identity, identity, proofsCount, validators)
      optAssetId       <- genOptAssetId
      call             <- callContractV3ParamGen(Gen.const(optAssetId), Gen.const(CallFeeInAsset))
      executedCall     <- executedTxV3ParamGen(executedSigner, call, proofsCount = proofsCount, specifiedValidators = validators)
    } yield {
      val genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      val genesisForCallAccount   = GenesisTransaction.create(call.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      val genesisBlock            = block(Seq(genesisForCreateAccount, genesisForCallAccount))
      (genesisBlock, executedSigner, executedCreate, executedCall)
    }

  def optionallyIssueAsset(issuer: PrivateKeyAccount, needAsset: Boolean): Gen[(Option[IssueTransaction], Gen[Option[AssetId]])] =
    if (needAsset) for {
      assetIssue <- smartIssueTransactionGen(issuer, None)
    } yield (Some(assetIssue), Gen.option(assetIssue.assetId()))
    else (none[IssueTransaction], Gen.const(none[AssetId]))

  def preconditionsForCreateV5WithTransfersOptWithAsset(
      withAsset: Boolean,
      transferCount: Int = 10): Gen[(Seq[Block], PrivateKeyAccount, Option[IssueTransaction], ExecutedContractTransactionV3)] = {
    for {
      ts                <- timestampGen
      (master, genesis) <- accountGen.map(acc => acc -> GenesisTransaction.create(acc.toAddress, ENOUGH_AMT, ts).explicitGet())
      maybeIssueTxGen = if (withAsset) smartIssueTransactionGen(Gen.const(master), None).map(Some(_)) else Gen.const[Option[IssueTransaction]](None)
      maybeIssueTx <- maybeIssueTxGen
      transferGen = for {
        optAssetId <- maybeIssueTx.fold(Gen.const[Option[ByteStr]](None))(maybeIssueTx => Gen.const(Some(maybeIssueTx.assetId())))
        amount     <- Gen.choose(100000L, 1000000000L)
      } yield (optAssetId, amount)
      transferGen    <- Gen.listOfN(transferCount, transferGen)
      create         <- createContractV5Gen(None, (None, createTxFeeGen), master, ValidationPolicy.Any, ContractApiVersion.Initial, transferGen)
      executedCreate <- executedTxV3ParamGen(master, create)
    } yield {
      val block1      = block(Seq(genesis))
      val emptyBlock2 = block(block1.timestamp + 5, block1.uniqueId, Seq.empty)
      val block2      = maybeIssueTx.map(issueTx => block(Seq(issueTx))).getOrElse(emptyBlock2)
      (Seq(block1, block2), master, maybeIssueTx, executedCreate)
    }
  }

  def preconditionsForCallV5WithTransfersOptWithAsset(
      operationsCount: Int = 30): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV3, ExecutedContractTransactionV3)] = {
    for {
      genesisTime      <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner   <- accountGen
      validationPolicy <- Gen.oneOf(ValidationPolicy.Majority, ValidationPolicy.Any)
      create <- createContractV5Gen(
        None,
        (None, createTxFeeGen),
        accountGen,
        validationPolicy,
        ContractApiVersion.Initial,
        List(None -> (ENOUGH_AMT / 100))
      )
      proofsCount <- Gen.choose(1, 10)
      validators <- Gen
        .listOfN(math.floor(proofsCount / ValidationPolicy.MajorityRatio).toInt, accountGen)
      executedCreate         <- executedTxV3ParamGen(executedSigner, create, proofsCount = proofsCount, specifiedValidators = validators)
      callerContractTransfer <- contractTransferInV1Gen(None, ENOUGH_AMT / 1000)
      call <- callContractV5ParamGen(Gen.const(Option.empty[AssetId]),
                                     callTxFeeGen,
                                     accountGen.sample.get,
                                     create.contractId,
                                     1,
                                     List(callerContractTransfer))
      contractOperations <- listContractAssetOperationV1ParamGen(call.id(), operationsCount)
      executedCall <- executedTxV3ParamWithOperationsGen(
        signer = executedSigner,
        tx = call,
        proofsCount = proofsCount,
        specifiedValidators = validators,
        assetOperations = contractOperations
      )
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      genesisForCallAccount   = GenesisTransaction.create(call.sender.toAddress, enoughAmount, genesisTime).explicitGet()
      genesisForValidators    = validators.flatMap(validator => buildGenesisTxsForValidator(validator.toAddress, genesisTime))
      genesisBlock            = block(Seq(genesisForCreateAccount, genesisForCallAccount) ++ genesisForValidators)
    } yield (genesisBlock, executedSigner, executedCreate, executedCall)
  }

  def createContractV5Gen(atomicBadgeGen: Gen[Option[AtomicBadge]],
                          optFeeAssetWithAmountGen: (Gen[Option[AssetId]], Gen[Long]),
                          signerGen: Gen[PrivateKeyAccount],
                          validationPolicyGen: Gen[ValidationPolicy],
                          contractApiVersionGen: Gen[ContractApiVersion],
                          transfersGen: Gen[List[(Option[AssetId], Long)]]): Gen[CreateContractTransactionV5] =
    for {
      signer             <- signerGen
      image              <- genBoundedString(CreateContractTransactionV5.ImageMinLength, CreateContractTransactionV5.ImageMaxLength)
      imageHash          <- bytes32gen.map(DigestUtils.sha256Hex)
      contractName       <- genBoundedString(1, CreateContractTransactionV5.ContractNameMaxLength)
      optFeeAssetId      <- optFeeAssetWithAmountGen._1
      feeAmount          <- optFeeAssetWithAmountGen._2
      atomicBadge        <- atomicBadgeGen
      validationPolicy   <- validationPolicyGen
      contractApiVersion <- contractApiVersionGen
      timestamp          <- ntpTimestampGen
      payments           <- listContractTransferInV1Gen(transfersGen)
    } yield
      CreateContractTransactionV5
        .selfSigned(
          signer,
          new String(image, UTF_8),
          imageHash,
          new String(contractName, UTF_8),
          List.empty,
          feeAmount,
          timestamp,
          optFeeAssetId,
          atomicBadge,
          validationPolicy,
          contractApiVersion,
          payments
        )
        .explicitGet()

  def createContractV5ParamGen(atomicBadgeGen: Gen[Option[AtomicBadge]],
                               optFeeAssetWithAmountGen: (Gen[Option[AssetId]], Gen[Long]),
                               signerGen: Gen[PrivateKeyAccount],
                               validationPolicyGen: Gen[ValidationPolicy],
                               contractApiVersionGen: Gen[ContractApiVersion],
                               transfersGen: Gen[List[(Option[AssetId], Long)]]): Gen[CreateContractTransactionV5] =
    for {
      signer             <- signerGen
      image              <- genBoundedString(CreateContractTransactionV5.ImageMinLength, CreateContractTransactionV5.ImageMaxLength)
      imageHash          <- bytes32gen.map(DigestUtils.sha256Hex)
      contractName       <- genBoundedString(1, CreateContractTransactionV5.ContractNameMaxLength)
      params             <- Gen.choose(ParamsMinCount, ParamsMaxCount).flatMap(Gen.listOfN(_, dataEntryGen(DataEntryMaxSize)))
      optFeeAssetId      <- optFeeAssetWithAmountGen._1
      feeAmount          <- optFeeAssetWithAmountGen._2
      atomicBadge        <- atomicBadgeGen
      validationPolicy   <- validationPolicyGen
      contractApiVersion <- contractApiVersionGen
      timestamp          <- ntpTimestampGen
      payments           <- listContractTransferInV1Gen(transfersGen)
    } yield
      CreateContractTransactionV5
        .selfSigned(
          signer,
          new String(image, UTF_8),
          imageHash,
          new String(contractName, UTF_8),
          params,
          feeAmount,
          timestamp,
          optFeeAssetId,
          atomicBadge,
          validationPolicy,
          contractApiVersion,
          payments
        )
        .explicitGet()

  def createV5AndCallV5WithExecutedV3AndIssueWithParam(contractAssetOperations: List[ContractAssetOperation]): Gen[ExecutedTxV3TestData] =
    for {
      executedSigner <- accountGen
      genesisTimestamp = 0
      (creatorAccount, creatorGenesisTrx) <- accountGenesisGen(genesisTimestamp)
      create <- createContractV5Gen(
        None,
        (None, CreateFee),
        creatorAccount,
        ValidationPolicy.Any,
        ContractApiVersion.Initial,
        List.empty[(Option[AssetId], Long)]
      )
      executedCreate <- executedTxV3ParamGen(executedSigner, create)
      refillContract <- contractTransferInV1Gen(None, ENOUGH_AMT / 1000)
      call           <- callContractV5ParamGen(None, callTxFeeGen, creatorAccount, create.contractId, 1, List(refillContract))
      executedCall <- executedTxV3ParamWithOperationsGen(
        executedSigner,
        call,
        assetOperations = contractAssetOperations
      )
    } yield {
      val block1 = block(Seq(creatorGenesisTrx))
      val block2 = block(executedSigner, Seq(executedCreate))
      ExecutedTxV3TestData(Seq(block1, block2), executedSigner, List.empty, executedCall)
    }

  def blockWithExecutedCreateV5Gen(signer: PrivateKeyAccount, assetOps: List[ContractAssetOperation]): Gen[Block] =
    for {
      create <- createContractV5Gen(
        None,
        (None, CreateFee),
        signer,
        ValidationPolicy.Any,
        ContractApiVersion.Initial,
        List.empty[(Option[AssetId], Long)]
      )
      executedCreate <- executedTxV3ParamWithOperationsGen(signer, create, assetOperations = assetOps)
    } yield {
      val blockWithCreate = block(signer, Seq(executedCreate))
      blockWithCreate
    }

}

object ExecutableTransactionGen {

  case class ExecutedTxV3TestData(
      minedBlocks: Seq[Block],
      executedTxSignerAccount: PrivateKeyAccount,
      validatorAccounts: List[PrivateKeyAccount],
      executedTx: ExecutedContractTransactionV3
  )
}
