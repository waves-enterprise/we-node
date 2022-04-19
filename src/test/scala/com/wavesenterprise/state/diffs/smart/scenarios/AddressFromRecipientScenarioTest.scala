package com.wavesenterprise.state.diffs.smart.scenarios

import com.wavesenterprise.account.{AddressOrAlias, PrivateKeyAccount}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.v1.compiler.Terms.{CONST_BYTEVECTOR, CaseObj}
import com.wavesenterprise.state.diffs.smart.predef._
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffAndState, produce}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.{CreateAliasTransaction, GenesisTransaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class AddressFromRecipientScenarioTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  val preconditionsAndAliasCreations: Gen[(Seq[GenesisTransaction], CreateAliasTransaction, TransferTransactionV2, TransferTransactionV2)] = for {
    master                   <- accountGen
    ts                       <- timestampGen
    other: PrivateKeyAccount <- accountGen
    genesis1: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    genesis2: GenesisTransaction = GenesisTransaction.create(other.toAddress, ENOUGH_AMT, ts).explicitGet()
    alias              <- aliasGen
    fee                <- smallFeeGen
    aliasTx            <- createAliasV2Gen(other, alias, fee, ts)
    transferViaAddress <- transferGeneratorP(master, other.toAddress, None, None)
    transferViaAlias   <- transferGeneratorP(master, AddressOrAlias.fromBytes(alias.bytes.arr, 0).explicitGet()._1, None, None)
  } yield (Seq(genesis1, genesis2), aliasTx, transferViaAddress, transferViaAlias)

  val script = """
    | match tx {
    |  case t : TransferTransaction =>  addressFromRecipient(t.recipient)
    |  case other => throw()
    |  }
    |  """.stripMargin

  property("Script can resolve AddressOrAlias") {
    forAll(preconditionsAndAliasCreations) {
      case (gen, aliasTx, transferViaAddress, transferViaAlias) =>
        assertDiffAndState(Seq(TestBlock.create(gen)), TestBlock.create(Seq(aliasTx))) {
          case (_, state) =>
            val addressBytes = runScript[CaseObj](script, transferViaAddress, state).explicitGet().fields("bytes").asInstanceOf[CONST_BYTEVECTOR]
            addressBytes.bs.toArray.sameElements(transferViaAddress.recipient.bytes.arr) shouldBe true
            val resolvedAddressBytes =
              runScript[CaseObj](script, transferViaAlias, state).explicitGet().fields("bytes").asInstanceOf[CONST_BYTEVECTOR]

            resolvedAddressBytes.bs.toArray.sameElements(transferViaAddress.recipient.bytes.arr) shouldBe true
        }
    }
  }

  property("Script can't resolve alias that doesn't exist") {
    forAll(preconditionsAndAliasCreations) {
      case (gen, _, _, transferViaAlias) =>
        assertDiffAndState(Seq(TestBlock.create(gen)), TestBlock.create(Seq())) {
          case (_, state) =>
            runScript(script, transferViaAlias, state) should produce("AliasDoesNotExist")
        }
    }
  }
}
