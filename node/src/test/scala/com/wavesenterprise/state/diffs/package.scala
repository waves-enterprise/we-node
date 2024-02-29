package com.wavesenterprise.state

import cats.Monoid
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.TestPermissionValidator.permissionValidatorNoOp
import com.wavesenterprise.acl.{PermissionValidator, TestPermissionValidator}
import com.wavesenterprise.block.Block
import com.wavesenterprise.database.snapshot.{ConsensualSnapshotSettings, DisabledSnapshot}
import com.wavesenterprise.db.WithState
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.mining.MiningConstraint
import com.wavesenterprise.settings.{BlockchainSettings, FunctionalitySettings, TestBlockchainSettings, WestAmount, TestFunctionalitySettings => TFS}
import com.wavesenterprise.state.Portfolio.Fraction
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.CommonValidation.MaxTimePrevBlockOverTransactionDiff
import com.wavesenterprise.transaction.{Transaction, ValidationError}
import org.scalatest.matchers.should.Matchers

package object diffs extends WithState with Matchers {
  val ENOUGH_AMT: Long = Long.MaxValue / 3

  def assertDiffEither(
      preconditions: Seq[Block],
      block: Block,
      fs: FunctionalitySettings = TFS.Enabled,
      withoutPermissionCheck: Boolean = true,
      snapshotSettings: ConsensualSnapshotSettings = DisabledSnapshot
  )(assertion: Either[ValidationError, Diff] => Unit): Unit = {
    val blockchainSettings = TestBlockchainSettings.withFunctionality(fs)
    assertDiffEither(preconditions, block, blockchainSettings, withoutPermissionCheck, snapshotSettings)(assertion)
  }

  def assertDiffEither2(
      preconditions: Seq[Block],
      block: Block,
      fs: FunctionalitySettings = TFS.Enabled,
      fee: Option[(Byte, WestAmount)],
      withoutPermissionCheck: Boolean = true,
      snapshotSettings: ConsensualSnapshotSettings = DisabledSnapshot
  )(assertion: Either[ValidationError, Diff] => Unit): Unit = {
    val blockchainSettings = TestBlockchainSettings.withFsAndFee(fs, fee)
    assertDiffEither(preconditions, block, blockchainSettings, withoutPermissionCheck, snapshotSettings)(assertion)
  }

  def assertDiffEither(
      preconditions: Seq[Block],
      block: Block,
      blockchainSettings: BlockchainSettings,
      withoutPermissionCheck: Boolean,
      snapshotSettings: ConsensualSnapshotSettings
  )(assertion: Either[ValidationError, Diff] => Unit): Unit = {
    val permissionValidator = {
      if (withoutPermissionCheck)
        TestPermissionValidator.permissionValidatorNoOp()
      else
        PermissionValidator(blockchainSettings.custom.genesis)
    }

    withStateAndHistory(blockchainSettings.custom.functionality) { state =>
      def differ(blockchain: Blockchain, b: Block) =
        BlockDiffer.fromBlock(
          blockchainSettings,
          snapshotSettings,
          blockchain,
          permissionValidator,
          None,
          b,
          MiningConstraint.Unlimited,
          MaxTimePrevBlockOverTransactionDiff
        )

      preconditions.foreach { precondition =>
        val (preconditionDiff, preconditionFees, _) = differ(state, precondition).explicitGet()
        state.append(preconditionDiff, preconditionFees, precondition)
      }
      val totalDiff1 = differ(state, block)
      assertion(totalDiff1.map(_._1))
    }
  }

  private def assertDiffAndState(
      preconditions: Seq[Block],
      block: Block,
      fs: FunctionalitySettings,
      withNg: Boolean,
      withoutPermissionCheck: Boolean,
      blockchainSettings: BlockchainSettings
  )(assertion: (Diff, Blockchain) => Unit): Unit = {
    val permissionValidator = {
      if (withoutPermissionCheck)
        TestPermissionValidator.permissionValidatorNoOp()
      else
        PermissionValidator(blockchainSettings.custom.genesis)
    }
    withStateAndHistory(fs) { state =>
      def differ(blockchain: Blockchain, prevBlock: Option[Block], b: Block) =
        BlockDiffer.fromBlock(
          blockchainSettings,
          DisabledSnapshot,
          blockchain,
          permissionValidator,
          if (withNg) prevBlock else None,
          b,
          MiningConstraint.Unlimited,
          MaxTimePrevBlockOverTransactionDiff
        )

      preconditions.foldLeft[Option[Block]](None) { (prevBlock, curBlock) =>
        val (diff, fees, _) = differ(state, prevBlock, curBlock).explicitGet()
        state.append(diff, fees, curBlock)
        Some(curBlock)
      }
      val (diff, fees, _) = differ(state, preconditions.lastOption, block).explicitGet()
      state.append(diff, fees, block)
      assertion(diff, state)
    }
  }

  def assertNgDiffState(preconditions: Seq[Block], block: Block, fs: FunctionalitySettings = TFS.Enabled)(
      assertion: (Diff, Blockchain) => Unit): Unit = {
    val blockchainSettings = TestBlockchainSettings.withFunctionality(fs)
    assertDiffAndState(preconditions, block, fs, withNg = true, withoutPermissionCheck = true, blockchainSettings)(assertion)
  }

  def assertDiffAndState(preconditions: Seq[Block], block: Block, fs: FunctionalitySettings = TFS.Enabled, withoutPermissionCheck: Boolean = true)(
      assertion: (Diff, Blockchain) => Unit): Unit = {
    val blockchainSettings = TestBlockchainSettings.withFunctionality(fs)
    assertDiffAndState(preconditions, block, fs, withNg = false, withoutPermissionCheck, blockchainSettings)(assertion)
  }

  def assertDiffAndState(fs: FunctionalitySettings)(test: (Seq[Transaction] => Either[ValidationError, Unit]) => Unit): Unit =
    withStateAndHistory(fs) { state =>
      val blockchainSettings = TestBlockchainSettings.withFunctionality(fs)

      def differ(blockchain: Blockchain, b: Block) =
        BlockDiffer.fromBlock(
          blockchainSettings,
          DisabledSnapshot,
          blockchain,
          permissionValidatorNoOp(),
          None,
          b,
          MiningConstraint.Unlimited,
          MaxTimePrevBlockOverTransactionDiff
        )

      test(txs => {
        val block = TestBlock.create(txs)
        differ(state, block).map(diff => state.append(diff._1, diff._2, block))
      })
    }

  def assertBalanceInvariant(diff: Diff): Unit = {
    val portfolioDiff = Monoid.combineAll(diff.portfolios.values)
    portfolioDiff.balance shouldBe 0
    portfolioDiff.effectiveBalance shouldBe 0
    portfolioDiff.assets.values.foreach(_ shouldBe 0)
  }

  def assertBalanceInvariantForSponsorship(blockDiff: Diff, minerAddress: Address, carryFee: Long, minerTransfersSum: Long = 0): Unit = {
    val blockFee         = Fraction(5, 3)(carryFee)
    val minerBalanceDiff = blockDiff.portfolios(minerAddress.toAssetHolder).balance

    val portfolioDiff = Monoid.combineAll(blockDiff.portfolios.values)
    portfolioDiff.balance shouldBe (minerBalanceDiff - blockFee + minerTransfersSum)
    portfolioDiff.effectiveBalance shouldBe (minerBalanceDiff - blockFee + minerTransfersSum)
    portfolioDiff.assets.values.foreach(_ shouldBe 0)
  }

  def assertLeft(preconditions: Seq[Block], block: Block, fs: FunctionalitySettings = TFS.Enabled)(errorMessage: String): Unit =
    assertDiffEither(preconditions, block, fs)(_ should produce(errorMessage))

  def produce(errorMessage: String): ProduceError = new ProduceError(errorMessage)
}
