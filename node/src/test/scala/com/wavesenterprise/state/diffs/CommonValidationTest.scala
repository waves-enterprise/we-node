package com.wavesenterprise.state.diffs

import com.google.common.base.Charsets
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.acl.Role
import com.wavesenterprise.acl.TestPermissionValidator.permissionValidatorNoOp
import com.wavesenterprise.database.snapshot.{Backoff, DisabledSnapshot, EnabledSnapshot}
import com.wavesenterprise.db.WithState
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.v1.compiler.Terms._
import com.wavesenterprise.mining.MiningConstraint
import com.wavesenterprise.settings.{
  ConsensusType,
  FunctionalitySettings,
  GenesisSettingsVersion,
  PositiveInt,
  TestBlockchainSettings,
  TestFunctionalitySettings
}
import com.wavesenterprise.state.diffs.CommonValidation.MaxTimePrevBlockOverTransactionDiff
import com.wavesenterprise.transaction.assets.{IssueTransactionV2, SponsorFeeTransactionV1}
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.transaction.{GenesisPermitTransaction, GenesisTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._

class CommonValidationTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with WithState with NoShrink {

  property("disallows double spending") {
    val preconditionsAndPayment: Gen[(GenesisTransaction, TransferTransactionV2)] = for {
      master    <- accountGen
      recipient <- otherAccountGen(candidate = master)
      ts        <- positiveIntGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      transfer <- westTransferGeneratorP(master, recipient.toAddress)
    } yield (genesis, transfer)

    forAll(preconditionsAndPayment) {
      case (genesis, transfer) =>
        assertDiffEither(Seq(TestBlock.create(Seq(genesis, transfer))), TestBlock.create(Seq(transfer))) { blockDiffEi =>
          blockDiffEi should produce("AlreadyInTheState")
        }

        assertDiffEither(Seq(TestBlock.create(Seq(genesis))), TestBlock.create(Seq(transfer, transfer))) { blockDiffEi =>
          blockDiffEi should produce("AlreadyInTheState")
        }
    }
  }

  property("disallows txs when the snapshot height is reached") {
    val preconditionsAndPayment: Gen[(GenesisTransaction, TransferTransactionV2)] = for {
      master    <- accountGen
      recipient <- otherAccountGen(candidate = master)
      ts        <- positiveIntGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      transfer <- westTransferGeneratorP(master, recipient.toAddress)
    } yield (genesis, transfer)

    val snapshotHeight   = PositiveInt(2)
    val snapshotSettings = EnabledSnapshot(snapshotHeight, PositiveInt(10), Backoff(3, 3.seconds), ConsensusType.PoA)

    forAll(preconditionsAndPayment) {
      case (genesis, transfer) =>
        assertDiffEither(preconditions = Seq(TestBlock.create(Seq(genesis))),
                         block = TestBlock.create(Seq(transfer)),
                         snapshotSettings = snapshotSettings) { blockDiffEi =>
          blockDiffEi should produce(s"Snapshot height '${snapshotHeight.value}' is reached. Unable to process transactions")
        }
    }
  }

  property("allows empty block when the snapshot height is reached") {
    val preconditionsAndPayment: Gen[GenesisTransaction] = for {
      master <- accountGen
      ts     <- positiveIntGen
      genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    } yield genesis

    val snapshotHeight   = PositiveInt(2)
    val snapshotSettings = EnabledSnapshot(snapshotHeight, PositiveInt(10), Backoff(3, 3.seconds), ConsensusType.PoA)

    forAll(preconditionsAndPayment) { genesis =>
      assertDiffEither(preconditions = Seq(TestBlock.create(Seq(genesis))), block = TestBlock.create(Seq.empty), snapshotSettings = snapshotSettings) {
        _ shouldBe 'right
      }
    }
  }

  private def sponsoredTransactionsCheckFeeTest(feeInAssets: Boolean, feeAmount: Long)(f: Either[ValidationError, Unit] => Assertion): Unit = {

    val blockchainSettings = TestBlockchainSettings.withFunctionality(
      createFunctionalitySettings(BlockchainFeature.FeeSwitch -> 0, BlockchainFeature.SmartAccounts -> 0, BlockchainFeature.SponsoredFeesSupport -> 0)
    )

    val gen = sponsorAndSetScriptGen(sponsorship = true, smartToken = false, smartAccount = false, feeInAssets, feeAmount)

    forAll(gen) {
      case (genesisBlock, transferTx) =>
        withStateAndHistory(blockchainSettings.custom.functionality) { blockchain =>
          val (preconditionDiff, preconditionFees, _) =
            BlockDiffer
              .fromBlock(
                blockchainSettings,
                DisabledSnapshot,
                blockchain,
                permissionValidatorNoOp(),
                None,
                genesisBlock,
                MiningConstraint.Unlimited,
                MaxTimePrevBlockOverTransactionDiff
              )
              .explicitGet()
          blockchain.append(preconditionDiff, preconditionFees, genesisBlock)

          f {
            FeeCalculator(blockchain, blockchainSettings.custom.functionality, blockchainSettings.fees)
              .validateTxFee(height = 2, transferTx)
          }
        }
    }
  }

  property("checkFee for sponsored transactions sunny") {
    sponsoredTransactionsCheckFeeTest(feeInAssets = true, feeAmount = 1.west * 10)(_ shouldBe 'right)
  }

  property("checkFee for sponsored transactions fails if the fee is not enough") {
    sponsoredTransactionsCheckFeeTest(feeInAssets = true, feeAmount = 1)(_ should produce("does not exceed minimal value of"))
  }

  private def smartAccountCheckFeeTest(feeInAssets: Boolean, feeAmount: Long)(f: Either[ValidationError, Unit] => Assertion): Unit = {
    val blockchainSettings = TestBlockchainSettings.withFunctionality(createFunctionalitySettings(BlockchainFeature.SmartAccounts -> 0))

    val gen = sponsorAndSetScriptGen(sponsorship = false, smartToken = false, smartAccount = true, feeInAssets, feeAmount)
    forAll(gen) {
      case (genesisBlock, transferTx) =>
        withStateAndHistory(blockchainSettings.custom.functionality) { blockchain =>
          val (preconditionDiff, preconditionFees, _) =
            BlockDiffer
              .fromBlock(
                blockchainSettings,
                DisabledSnapshot,
                blockchain,
                permissionValidatorNoOp(),
                None,
                genesisBlock,
                MiningConstraint.Unlimited,
                MaxTimePrevBlockOverTransactionDiff
              )
              .explicitGet()
          blockchain.append(preconditionDiff, preconditionFees, genesisBlock)

          f {
            FeeCalculator(blockchain, blockchainSettings.custom.functionality, blockchainSettings.fees)
              .validateTxFee(height = 2, transferTx)
          }
        }
    }
  }

  property("checkFee for smart accounts sunny") {
    smartAccountCheckFeeTest(feeInAssets = false, feeAmount = 400000)(_ shouldBe 'right)
  }

  private def sponsorAndSetScriptGen(sponsorship: Boolean, smartToken: Boolean, smartAccount: Boolean, feeInAssets: Boolean, feeAmount: Long) =
    for {
      richAcc      <- accountGen
      recipientAcc <- accountGen
      ts = System.currentTimeMillis()
    } yield {
      val script = ScriptV1(TRUE).explicitGet()

      val genesisTx = GenesisTransaction.create(richAcc.toAddress, ENOUGH_AMT, ts).explicitGet()

      val issueTx =
        if (smartToken)
          IssueTransactionV2
            .selfSigned(AddressScheme.getAddressSchema.chainId,
                        richAcc,
                        "test".getBytes(UTF_8),
                        "desc".getBytes(UTF_8),
                        Long.MaxValue,
                        2,
                        reissuable = false,
                        1.west,
                        ts,
                        Some(script))
            .explicitGet()
        else
          IssueTransactionV2
            .selfSigned(currentChainId,
                        richAcc,
                        "test".getBytes(UTF_8),
                        "desc".getBytes(UTF_8),
                        Long.MaxValue,
                        2,
                        reissuable = false,
                        1.west,
                        ts,
                        None)
            .explicitGet()

      val transferWestTx = TransferTransactionV2
        .selfSigned(richAcc, None, None, ts, 10.west, 1.west, recipientAcc.toAddress, Array.emptyByteArray)
        .explicitGet()

      val transferAssetTx = TransferTransactionV2
        .selfSigned(richAcc, Some(issueTx.id()), None, ts, 100, 1.west, recipientAcc.toAddress, Array.emptyByteArray)
        .explicitGet()

      val sponsorTx =
        if (sponsorship)
          Seq(
            SponsorFeeTransactionV1
              .selfSigned(richAcc, issueTx.id(), isEnabled = true, 1.west, ts)
              .explicitGet()
          )
        else Seq.empty

      val setScriptTx =
        if (smartAccount)
          Seq(
            SetScriptTransactionV1
              .selfSigned(
                AddressScheme.getAddressSchema.chainId,
                recipientAcc,
                Some(script),
                "script".getBytes(Charsets.UTF_8),
                Array.empty[Byte],
                1.west,
                ts
              )
              .explicitGet()
          )
        else Seq.empty

      val transferBackTx = TransferTransactionV2
        .selfSigned(recipientAcc,
                    Some(issueTx.id()),
                    if (feeInAssets) Some(issueTx.id()) else None,
                    ts,
                    1,
                    feeAmount,
                    richAcc.toAddress,
                    Array.emptyByteArray)
        .explicitGet()

      (TestBlock.create(Vector[Transaction](genesisTx, issueTx, transferWestTx, transferAssetTx) ++ sponsorTx ++ setScriptTx), transferBackTx)
    }

  private def createFunctionalitySettings(preActivatedFeatures: (BlockchainFeature, Int)*): FunctionalitySettings =
    TestFunctionalitySettings.Enabled
      .copy(
        preActivatedFeatures = preActivatedFeatures.map { case (k, v) => k.id -> v }(collection.breakOut),
        blocksForFeatureActivation = 1,
        featureCheckBlocksPeriod = 2
      )

  private def smartTokensCheckFeeTest(feeInAssets: Boolean, feeAmount: Long)(f: Either[ValidationError, Unit] => Assertion): Unit = {
    val blockchainSettings = TestBlockchainSettings.withFunctionality(
      createFunctionalitySettings(BlockchainFeature.SmartAccounts -> 0, BlockchainFeature.SmartAssets -> 0)
    )

    val gen = sponsorAndSetScriptGen(sponsorship = false, smartToken = true, smartAccount = false, feeInAssets, feeAmount)
    forAll(gen) {
      case (genesisBlock, transferTx) =>
        withStateAndHistory(blockchainSettings.custom.functionality) { blockchain =>
          val (preconditionDiff, preconditionFees, _) =
            BlockDiffer
              .fromBlock(
                blockchainSettings,
                DisabledSnapshot,
                blockchain,
                permissionValidatorNoOp(),
                None,
                genesisBlock,
                MiningConstraint.Unlimited,
                MaxTimePrevBlockOverTransactionDiff
              )
              .explicitGet()
          blockchain.append(preconditionDiff, preconditionFees, genesisBlock)

          f {
            FeeCalculator(blockchain, blockchainSettings.custom.functionality, blockchainSettings.fees)
              .validateTxFee(height = 1, transferTx)
          }
        }
    }
  }

  property("checkFee for smart tokens sunny") {
    smartTokensCheckFeeTest(feeInAssets = false, feeAmount = 1)(_ shouldBe 'right)
  }

  private val genesisSettings = TestBlockchainSettings.Default.custom.genesis.toPlainSettingsUnsafe

  property("disallows sending transactions without sender role if sender role enabled in genesis") {
    val preconditionsAndPayment: Gen[(GenesisTransaction, TransferTransactionV2)] = for {
      master    <- accountGen
      recipient <- otherAccountGen(candidate = master)
      ts        <- positiveIntGen
      genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      transfer <- westTransferGeneratorP(master, recipient.toAddress)
    } yield (genesis, transfer)

    val blockchainSettings = TestBlockchainSettings.withGenesis(
      genesisSettings.copy(
        senderRoleEnabled = true,
        version = GenesisSettingsVersion.ModernVersion
      )
    )

    forAll(preconditionsAndPayment) {
      case (genesis, transfer) =>
        assertDiffEither(
          preconditions = Seq(TestBlock.create(Seq(genesis))),
          block = TestBlock.create(Seq(transfer)),
          blockchainSettings = blockchainSettings,
          withoutPermissionCheck = false,
          snapshotSettings = DisabledSnapshot
        ) { blockDiffEi =>
          blockDiffEi should produce("Has no active 'sender' role")
        }
    }
  }

  property("allows sending transactions with sender role if sender role enabled in genesis") {
    val preconditionsAndPayment: Gen[(GenesisPermitTransaction, GenesisTransaction, TransferTransactionV2)] = for {
      master    <- accountGen
      recipient <- otherAccountGen(candidate = master)
      ts        <- positiveIntGen
      genesisTx1 = GenesisPermitTransaction.create(master.toAddress, Role.Sender, ts + 1).explicitGet()
      genesisTx2 = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      transfer <- westTransferGeneratorP(master, recipient.toAddress)
    } yield (genesisTx1, genesisTx2, transfer)

    val blockchainSettings = TestBlockchainSettings.withGenesis(
      genesisSettings.copy(
        senderRoleEnabled = true,
        version = GenesisSettingsVersion.ModernVersion
      )
    )

    forAll(preconditionsAndPayment) {
      case (genesisTx1, genesisTx2, transfer) =>
        assertDiffEither(Seq(TestBlock.create(Seq(genesisTx1, genesisTx2))),
                         TestBlock.create(Seq(transfer)),
                         blockchainSettings,
                         withoutPermissionCheck = false,
                         DisabledSnapshot) { blockDiffEi =>
          blockDiffEi shouldBe 'right
        }
    }
  }

  property("allows sending transactions without sender role if sender role disabled in genesis") {
    val preconditionsAndPayment: Gen[(GenesisTransaction, TransferTransactionV2)] = for {
      master    <- accountGen
      recipient <- otherAccountGen(candidate = master)
      ts        <- positiveIntGen
      genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      transfer <- westTransferGeneratorP(master, recipient.toAddress)
    } yield (genesis, transfer)

    val blockchainSettings = TestBlockchainSettings.withGenesis(
      genesisSettings.copy(senderRoleEnabled = false)
    )

    forAll(preconditionsAndPayment) {
      case (genesis, transfer) =>
        assertDiffEither(
          preconditions = Seq(TestBlock.create(Seq(genesis))),
          block = TestBlock.create(Seq(transfer)),
          blockchainSettings = blockchainSettings,
          withoutPermissionCheck = false,
          snapshotSettings = DisabledSnapshot
        ) { blockDiffEi =>
          blockDiffEi shouldBe 'right
        }
    }
  }
}
