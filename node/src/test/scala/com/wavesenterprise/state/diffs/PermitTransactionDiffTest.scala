package com.wavesenterprise.state.diffs

import com.google.common.base.Charsets
import com.wavesenterprise.account.{Address, AddressScheme}
import com.wavesenterprise.acl._
import com.wavesenterprise.block.Block
import com.wavesenterprise.database.snapshot.DisabledSnapshot
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{GenesisSettingsVersion, TestBlockchainSettings, TestFunctionalitySettings}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.ValidationError.AddressIsLastOfRole
import com.wavesenterprise.transaction.acl.{PermitTransaction, PermitTransactionV1}
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1}
import com.wavesenterprise.transaction.{GenesisPermitTransaction, GenesisTransaction}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class PermitTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  private val fs = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.ContractValidationsSupport.id -> 0))

  implicit class EitherAsGen[A, B](maybe: Either[A, B]) {
    def asGen: Gen[B] = maybe.fold(_ => Gen.fail, Gen.const)
  }

  property("Permit diff doesn't validate OpType.Remove tx with due date") {
    val preconditions: Gen[(Block, PermitTransaction)] =
      for {
        senderAcc <- accountGen
        ts        <- ntpTimestampGen
        gptx      <- GenesisPermitTransaction.create(senderAcc.toAddress, Role.Permissioner, ts).asGen
        genesisBlock = TestBlock.create(Seq(gptx))
        dueTs    <- ntpTimestampGen
        permOp   <- PermissionsGen.permissionOpGen.map(_.copy(opType = OpType.Remove, dueTimestampOpt = Some(dueTs)))
        permitTx <- permitTransactionV1Gen(accountGen = Gen.const(senderAcc), permissionOpGen = Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), fs) { result =>
          result should produce("Due date should not be defined for PermitTransaction with OpType Remove")
        }
    }
  }

  property("Scripted accounts are only allowed to have the contract_developer role") {
    val preconditions: Gen[(Block, SetScriptTransaction, PermitTransaction)] =
      for {
        scriptedAcc <- accountGen
        ts          <- ntpTimestampGen
        gtx         <- GenesisTransaction.create(scriptedAcc.toAddress, ENOUGH_AMT, ts).asGen
        genesisBlock = TestBlock.create(Seq(gtx))

        fee    <- smallFeeGen
        script <- ScriptCompiler("true", isAssetScript = false).map(_._1).asGen
        setScriptTx <- SetScriptTransactionV1
          .selfSigned(AddressScheme.getAddressSchema.chainId,
                      scriptedAcc,
                      Some(script),
                      "script".getBytes(Charsets.UTF_8),
                      Array.empty[Byte],
                      fee,
                      ts)
          .asGen

        ts <- ntpTimestampGen
        permOp = PermissionOp(OpType.Add, Role.Issuer, ts, None)
        permitTx <-
          permitTransactionV1Gen(Gen.const(scriptedAcc), Gen.const(scriptedAcc.toAddress), Gen.const(permOp), timestampGen = ntpTimestampGen)
      } yield (genesisBlock, setScriptTx, permitTx)

    forAll(preconditions) {
      case (genesisBlock, setScriptTx, permitTx) =>
        assertDiffEither(Seq(genesisBlock, TestBlock.create(Seq(setScriptTx))), TestBlock.create(Seq(permitTx)), fs) { result =>
          result should produce("Scripted accounts are only allowed to have the contract_developer role")
        }
    }
  }

  property("Permit diff validate OpType.Remove tx") {
    val preconditions: Gen[(Block, PermitTransaction)] =
      for {
        senderAcc <- accountGen
        ts        <- ntpTimestampGen
        role      <- PermissionsGen.allowedEmptyRoleWithoutSenderGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        gptx      <- GenesisPermitTransaction.create(senderAcc.toAddress, role, ts).asGen
        genesisBlock = TestBlock.create(Seq(gtx, gptx))
        permOp       = PermissionOp(OpType.Remove, role, ts, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(senderAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), fs) { result =>
          result shouldBe 'right
        }
    }
  }

  property("Permit diff doesn't validate OpType.Remove tx if address is last of role") {
    val preconditions: Gen[(Block, PermitTransaction)] =
      for {
        senderAcc <- accountGen
        ts        <- ntpTimestampGen
        role      <- PermissionsGen.nonEmptyRoleGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        gptx      <- GenesisPermitTransaction.create(senderAcc.toAddress, role, ts).asGen
        genesisBlock = TestBlock.create(Seq(gtx, gptx))
        permOp       = PermissionOp(OpType.Remove, role, ts, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(senderAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), fs) { result =>
          val expectedError = AddressIsLastOfRole(permitTx.target.asInstanceOf[Address], permitTx.permissionOp.role.asInstanceOf[NonEmptyRole])
          result should produce(expectedError.toString)
        }
    }
  }

  property("Permit diff doesn't validate ban tx if address is last of role") {
    val preconditions: Gen[(Block, PermitTransaction, NonEmptyRole)] =
      for {
        senderAcc <- accountGen
        ts        <- ntpTimestampGen
        role      <- PermissionsGen.nonEmptyRoleGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        gptx      <- GenesisPermitTransaction.create(senderAcc.toAddress, role, ts).asGen
        genesisBlock = TestBlock.create(Seq(gtx, gptx))
        permOp       = PermissionOp(OpType.Add, Role.Banned, ts, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(senderAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx, role)

    forAll(preconditions) {
      case (genesisBlock, permitTx, role) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), fs) { result =>
          val expectedError = AddressIsLastOfRole(permitTx.target.asInstanceOf[Address], role)
          result should produce(expectedError.toString)
        }
    }
  }

  property("Disallows permit tx with sender role if sender role disabled in genesis") {
    val preconditions: Gen[(Block, PermitTransactionV1)] =
      for {
        senderAcc <- accountGen
        ts        <- ntpTimestampGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        genesisPermit = GenesisPermitTransaction.create(senderAcc.toAddress, Role.Permissioner, ts + 1).explicitGet()
        genesisBlock  = TestBlock.create(Seq(gtx, genesisPermit))
        permOp        = PermissionOp(OpType.Add, Role.Sender, ts + 2, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(senderAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), fs, withoutPermissionCheck = false) { result =>
          result should produce("The role 'Sender' is not allowed by network parameters")
        }
    }
  }

  property("Allows permit tx with sender role if sender role enabled in genesis") {
    val blockchainSettings = TestBlockchainSettings.withGenesis(
      TestBlockchainSettings.Default.custom.genesis.toPlainSettingsUnsafe
        .copy(senderRoleEnabled = true, version = GenesisSettingsVersion.ModernVersion)
    )

    val preconditions: Gen[(Block, PermitTransactionV1)] =
      for {
        senderAcc <- accountGen
        targerAcc <- accountGen
        ts        <- ntpTimestampGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        genesisPermit1 = GenesisPermitTransaction.create(senderAcc.toAddress, Role.Permissioner, ts + 1).explicitGet()
        genesisPermit2 = GenesisPermitTransaction.create(senderAcc.toAddress, Role.Sender, ts + 2).explicitGet()
        genesisBlock   = TestBlock.create(Seq(gtx, genesisPermit1, genesisPermit2))
        permOp         = PermissionOp(OpType.Add, Role.Sender, ts + 3, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(targerAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(
          preconditions = Seq(genesisBlock),
          block = TestBlock.create(Seq(permitTx)),
          blockchainSettings = blockchainSettings,
          withoutPermissionCheck = false,
          snapshotSettings = DisabledSnapshot
        ) { result =>
          result shouldBe 'right
        }
    }
  }

  property(s"Allows permit tx with contract validator role if only contract validation feature is enabled") {
    val preconditions: Gen[(Block, PermitTransactionV1)] =
      for {
        senderAcc <- accountGen
        targerAcc <- accountGen
        ts        <- ntpTimestampGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        genesisPermit = GenesisPermitTransaction.create(senderAcc.toAddress, Role.Permissioner, ts + 1).explicitGet()
        genesisBlock  = TestBlock.create(Seq(gtx, genesisPermit))
        permOp        = PermissionOp(OpType.Add, Role.ContractValidator, ts + 3, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(targerAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), fs, withoutPermissionCheck = false) { result =>
          result shouldBe 'right
        }
    }
  }

  property(s"Disallows permit tx with contract validator role if contract validation feature is not enabled") {
    val blockchainSettings = TestBlockchainSettings.withFunctionality(fs.copy(preActivatedFeatures = Map.empty))
    val preconditions: Gen[(Block, PermitTransactionV1)] =
      for {
        senderAcc <- accountGen
        targerAcc <- accountGen
        ts        <- ntpTimestampGen
        gtx       <- GenesisTransaction.create(senderAcc.toAddress, ENOUGH_AMT, ts).asGen
        genesisPermit = GenesisPermitTransaction.create(senderAcc.toAddress, Role.Permissioner, ts + 1).explicitGet()
        genesisBlock  = TestBlock.create(Seq(gtx, genesisPermit))
        permOp        = PermissionOp(OpType.Add, Role.ContractValidator, ts + 3, None)
        permitTx <- permitTransactionV1Gen(Gen.const(senderAcc), Gen.const(targerAcc.toAddress), Gen.const(permOp))
      } yield (genesisBlock, permitTx)

    forAll(preconditions) {
      case (genesisBlock, permitTx) =>
        assertDiffEither(Seq(genesisBlock), TestBlock.create(Seq(permitTx)), blockchainSettings, withoutPermissionCheck = false, DisabledSnapshot) {
          result =>
            result should produce(s"Role '${Role.ContractValidator}' will be available after contract validations feature activation")
        }
    }
  }
}
