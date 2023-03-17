package com.wavesenterprise.database.snapshot

import com.wavesenterprise.account.{Alias, PrivateKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionsGen, Role}
import com.wavesenterprise.block.Block
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.state.diffs.ENOUGH_AMT
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, ExecutedContractTransactionV1}
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.transaction.transfer.TransferTransaction
import com.wavesenterprise.transaction.validation.DataValidation
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Suite

import scala.concurrent.duration._
import scala.util.Random

trait StatePreconditionsGen extends TransactionGen with ContractTransactionGen with NoShrink { _: Suite =>

  import StatePreconditionsGen._

  protected def setupBlockFilling: SetupBlockFilling = DefaultSetupBlockFilling

  protected val fs: FunctionalitySettings = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = (BlockchainFeature.implemented - BlockchainFeature.AtomicTransactionSupport.id)
      .map(_ -> 0)
      .toMap
  )

  protected def txSenders(): Gen[Seq[PrivateKeyAccount]] = {
    Gen.listOfN(SendersCount, accountGen).suchThat(_.nonEmpty)
  }

  protected def genesis(accounts: Seq[PrivateKeyAccount]): Gen[Block] =
    for {
      genesisTime <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      genesisTxs = accounts.map(account => GenesisTransaction.create(account.toAddress, ENOUGH_AMT / SendersCount, genesisTime).explicitGet())
      genesisPermits = accounts.flatMap(account =>
        Seq(
          GenesisPermitTransaction.create(account.toAddress, Role.Permissioner, genesisTime).explicitGet(),
          GenesisPermitTransaction.create(account.toAddress, Role.Issuer, genesisTime).explicitGet(),
          GenesisPermitTransaction.create(account.toAddress, Role.ContractDeveloper, genesisTime).explicitGet(),
          GenesisPermitTransaction.create(account.toAddress, Role.ConnectionManager, genesisTime).explicitGet()
        ))
      genesisBlock = TestBlock.create(genesisTxs ++ genesisPermits)
    } yield genesisBlock

  case class AssetTxs(issue: IssueTransaction, reissue: ReissueTransaction, burn: BurnTransaction, sponsorshipOpt: Option[SponsorFeeTransaction]) {
    def toSeq: Seq[Transaction] = Seq(issue, reissue, burn) ++ sponsorshipOpt.toSeq
  }

  protected def createAssetTxs(sender: PrivateKeyAccount): Gen[AssetTxs] =
    for {
      rQuantity  <- positiveIntGen
      bQuantity  <- positiveIntGen
      isScripted <- Arbitrary.arbitrary[Boolean]
      scriptOpt = if (isScripted) {
        val script = ScriptCompiler("match tx { case _ => true}", isAssetScript = true).explicitGet()._1
        Some(script)
      } else None
      (issue, reissue, burn, _) <- issueReissueBurnGeneratorP(ENOUGH_AMT, rQuantity, bQuantity, sender, scriptOpt, Gen.const(true), ntpTimestampGen)
      timestamp                 <- ntpTimestampGen
      sponsorshipOpt = if (isScripted) None
      else {
        val sponsorship = SponsorFeeTransactionV1
          .selfSigned(sender, issue.assetId(), isEnabled = true, TransactionGen.SponsorshipFee, timestamp)
          .explicitGet()
        Some(sponsorship)
      }
    } yield AssetTxs(issue, reissue, burn, sponsorshipOpt)

  protected def transferTx(sender: PrivateKeyAccount, assetId: Option[ByteStr] = None): Gen[TransferTransaction] =
    for {
      recipient <- accountGen
      transfer  <- transferGeneratorP(sender, recipient.toAddress, assetId, None)
    } yield transfer

  protected def createAliasTx(sender: PrivateKeyAccount): Gen[CreateAliasTransaction] =
    for {
      alias     <- aliasGen
      timestamp <- ntpTimestampGen
      aliasTx   <- createAliasV2Gen(sender, alias, MinIssueFee, timestamp)
    } yield aliasTx

  protected def createContractTx(sender: PrivateKeyAccount): Gen[ExecutedContractTransactionV1] =
    for {
      create         <- createContractV2ParamGen(sender)
      executedCreate <- executedTxV1ParamGen(TestBlock.defaultSigner, create)
    } yield executedCreate

  protected def callContractTx(contractId: ByteStr, sender: PrivateKeyAccount): Gen[ExecutedContractTransactionV1] =
    for {
      call           <- callContractV1ParamGen(sender, contractId)
      executedCreate <- executedTxV1ParamGen(TestBlock.defaultSigner, call)
    } yield executedCreate

  protected def createDataTx(sender: PrivateKeyAccount): Gen[DataTransaction] =
    for {
      data <- Gen.listOfN(DataValidation.MaxEntryCount, dataEntryGen(10, dataAsciiKeyGen))
      tx   <- dataTransactionV2GenP(sender, data)
    } yield tx

  protected def createPolicyAndRegisterTx(sender: PrivateKeyAccount): Gen[(CreatePolicyTransaction, Seq[RegisterNodeTransaction])] =
    for {
      txWrapper <- createPolicyTransactionV1GenWithRecipients(senderGen = Gen.const(sender))
      registerNodeTxsGen = txWrapper.recipientsPriKey.map(registerNodeTx(sender, _))
      registerTxs <- Gen.sequence[Seq[RegisterNodeTransaction], RegisterNodeTransaction](registerNodeTxsGen)
    } yield (txWrapper.txWrap.tx, registerTxs)

  protected def policyDataHashTx(sender: PrivateKeyAccount, policyId: ByteStr): Gen[PolicyDataHashTransaction] =
    policyDataHashTransactionV1Gen(policyId.arr, Gen.const(sender)).map(_.tx)

  protected def permissionTx(sender: PrivateKeyAccount): Gen[PermitTransaction] =
    permitTransactionV1Gen(
      accountGen = Gen.const(sender),
      permissionOpGen = PermissionsGen.permissionOpAddGen,
      targetGen = accountGen.map(_.toAddress),
      timestampGen = ntpTimestampGen
    )

  protected def registerNodeTx(sender: PrivateKeyAccount, target: PrivateKeyAccount): Gen[RegisterNodeTransaction] =
    registerNodeTransactionGen(Gen.const(sender), Gen.const(target), Gen.const(OpType.Add))

  case class Setup(block: Block, senderSetups: Seq[OneSenderSetup]) {
    def assets: Seq[ByteStr]    = senderSetups.flatMap(_.assets)
    def contracts: Seq[ByteStr] = senderSetups.flatMap(_.contracts)
    def policies: Seq[ByteStr]  = senderSetups.flatMap(_.policies)
    def aliases: Seq[Alias]     = senderSetups.map(_.alias)
  }

  case class OneSenderSetup(sender: PrivateKeyAccount,
                            txs: Seq[Transaction],
                            assets: Seq[ByteStr],
                            contracts: Seq[ByteStr],
                            policies: Seq[ByteStr],
                            alias: Alias)

  protected def createSetupBlock(accounts: Seq[PrivateKeyAccount], genesisBlock: Block): Gen[Setup] =
    for {
      senderSetups <- Gen.sequence[Seq[OneSenderSetup], OneSenderSetup](accounts.map(createSenderSetup))
      timestamp    <- ntpTimestampGen
      blockTxs = senderSetups.flatMap(_.txs)
      block    = TestBlock.create(timestamp, genesisBlock.uniqueId, blockTxs, TestBlock.defaultSigner, 3)
    } yield Setup(block, senderSetups)

  protected def createSenderSetup(sender: PrivateKeyAccount): Gen[OneSenderSetup] =
    for {
      assetsTxs            <- Gen.listOfN(setupBlockFilling.assets, createAssetTxs(sender))
      executedTxs          <- Gen.listOfN(setupBlockFilling.contracts, createContractTx(sender))
      policyAndRegisterTxs <- Gen.listOfN(setupBlockFilling.policies, createPolicyAndRegisterTx(sender))
      dataTx               <- createDataTx(sender)
      aliasTx              <- createAliasTx(sender)
      policyTxs   = policyAndRegisterTxs.map(_._1)
      registerTxs = policyAndRegisterTxs.flatMap(_._2)
      txs         = assetsTxs.flatMap(_.toSeq) ++ executedTxs ++ registerTxs ++ policyTxs ++ Seq(dataTx, aliasTx)
      assets      = assetsTxs.map(_.issue.assetId())
      contracts   = executedTxs.map(_.tx.contractId)
      policies    = policyTxs.map(_.id())
    } yield OneSenderSetup(sender, txs, assets, contracts, policies, aliasTx.alias)

  protected def createUpdatesBlock(setup: Setup, refBlock: Block): Gen[Block] = {
    val Setup(_, senderSetups)                                    = setup
    val OneSenderSetup(sender, _, assets, contracts, policies, _) = Random.shuffle(senderSetups).head
    val transferTxsGenSeq = assets.map { assetId =>
      transferTx(sender, Some(assetId))
    } :+ transferTx(sender)
    val executedTxsGenSeq = contracts.map { contractId =>
      callContractTx(contractId, sender)
    }

    val policiesTxsGenSeq = policies.map { policyId =>
      policyDataHashTx(sender, policyId)
    }
    for {
      transferTxs <- Gen.sequence[Seq[TransferTransaction], TransferTransaction](transferTxsGenSeq)
      executedTxs <- Gen.sequence[Seq[ExecutedContractTransactionV1], ExecutedContractTransactionV1](executedTxsGenSeq)
      policiesTxs <- Gen.sequence[Seq[PolicyDataHashTransaction], PolicyDataHashTransaction](policiesTxsGenSeq)
      permitTxs   <- Gen.nonEmptyListOf(permissionTx(sender))
      dataTx      <- createDataTx(sender)
      timestamp   <- ntpTimestampGen
      txs   = transferTxs ++ executedTxs ++ policiesTxs ++ Seq(dataTx) ++ permitTxs
      block = TestBlock.create(timestamp, refBlock.uniqueId, txs, TestBlock.defaultSigner, 3)
    } yield block
  }

  protected def updatesBlockSeqGen(setup: Setup, count: Int): Gen[Seq[Block]] = {
    def gen(previous: Seq[Block]): Gen[Seq[Block]] = {
      createUpdatesBlock(setup, previous.lastOption.getOrElse(setup.block)).flatMap { block =>
        val result = previous :+ block
        if (result.size == count) result else gen(result)
      }
    }
    gen(Vector.empty)
  }

  case class Preconditions(senders: Seq[PrivateKeyAccount],
                           blocks: Seq[Block],
                           assets: Seq[ByteStr],
                           contracts: Seq[ByteStr],
                           policies: Seq[ByteStr],
                           aliases: Seq[Alias])

  protected def preconditionsGen(blocksCount: Int): Gen[Preconditions] =
    for {
      senders      <- txSenders()
      genesisBlock <- genesis(senders)
      setup        <- createSetupBlock(senders, genesisBlock)
      updatesBlock <- updatesBlockSeqGen(setup, blocksCount)
      blocks = Seq(genesisBlock, setup.block) ++ updatesBlock
    } yield Preconditions(senders, blocks, setup.assets, setup.contracts, setup.policies, setup.aliases)
}

/**
  * Setup block filling for one sender. Each sender creates $assets assets, $contract contracts, $policies policies
  */
case class SetupBlockFilling(assets: Int, contracts: Int, policies: Int)

object StatePreconditionsGen {
  private val SendersCount             = 5
  private val DefaultSetupBlockFilling = SetupBlockFilling(3, 3, 3)
}
