package com.wavesenterprise.state.diffs

import com.wavesenterprise.acl.TestPermissionValidator.permissionValidatorNoOp
import com.wavesenterprise.db.WithState
import com.wavesenterprise.settings.{TestBlockchainSettings, TestFunctionalitySettings}
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state.diffs.CommonValidation.MaxTimePrevBlockOverTransactionDiff
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class CommonValidationTimeTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink with WithState {

  property("disallows too old transacions") {
    forAll(for {
      prevBlockTs <- ntpTimestampGen
      blockTs     <- ntpTimestampGen
      master      <- accountGen
      height      <- positiveIntGen
      recipient   <- addressGen
      amount      <- positiveLongGen
      fee         <- smallFeeGen
      transfer1 = createWestTransfer(master, recipient, amount, fee, prevBlockTs - CommonValidation.MaxTimePrevBlockOverTransactionDiff.toMillis - 1)
        .explicitGet()
    } yield (prevBlockTs, blockTs, height, transfer1)) {
      case (prevBlockTs, blockTs, height, transfer1) =>
        withStateAndHistory(TestFunctionalitySettings.Enabled) { blockchain: Blockchain =>
          TransactionDiffer(
            TestBlockchainSettings.Default,
            permissionValidatorNoOp(),
            Some(prevBlockTs),
            blockTs,
            height,
            MaxTimePrevBlockOverTransactionDiff
          )(blockchain, transfer1, None) should produce("too old")
        }
    }
  }

  property("disallows transactions from far future") {
    forAll(for {
      prevBlockTs <- timestampGen
      blockTs     <- Gen.choose(prevBlockTs, prevBlockTs + 7 * 24 * 3600 * 1000)
      master      <- accountGen
      height      <- positiveIntGen
      recipient   <- addressGen
      amount      <- positiveLongGen
      fee         <- smallFeeGen
      transfer1 = createWestTransfer(master, recipient, amount, fee, blockTs + CommonValidation.MaxTimeTransactionOverBlockDiff.toMillis + 1)
        .explicitGet()
    } yield (prevBlockTs, blockTs, height, transfer1)) {
      case (prevBlockTs, blockTs, height, transfer1) =>
        withStateAndHistory(TestBlockchainSettings.Default.custom.functionality) { blockchain: Blockchain =>
          TransactionDiffer(TestBlockchainSettings.Default,
                            permissionValidatorNoOp(),
                            Some(prevBlockTs),
                            blockTs,
                            height,
                            MaxTimePrevBlockOverTransactionDiff)(blockchain, transfer1, None) should produce("far future")
        }
    }
  }
}
