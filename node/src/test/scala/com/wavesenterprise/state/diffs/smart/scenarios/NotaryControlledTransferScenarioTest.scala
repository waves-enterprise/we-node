package com.wavesenterprise.state.diffs.smart.scenarios

import java.nio.charset.StandardCharsets

import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.v1.compiler.CompilerV1
import com.wavesenterprise.lang.v1.compiler.Terms.EVALUATED
import com.wavesenterprise.lang.v1.evaluator.EvaluatorV1
import com.wavesenterprise.lang.v1.parser.Parser
import com.wavesenterprise.lang.{WavesGlobal, Testing}
import com.wavesenterprise.state._
import com.wavesenterprise.utils.SmartContractV1Utils.compilerContext
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.state.diffs.smart._
import com.wavesenterprise.transaction.assets.{IssueTransaction, IssueTransactionV2}
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.{DataTransactionV1, GenesisTransaction}
import com.wavesenterprise.utils.SmartContractV1Utils.dummyEvalContext
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class NotaryControlledTransferScenarioTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  val preconditions: Gen[
    (Seq[GenesisTransaction], IssueTransaction, DataTransactionV1, TransferTransaction, DataTransactionV1, DataTransactionV1, TransferTransaction)] =
    for {
      company  <- accountGen
      king     <- accountGen
      notary   <- accountGen
      accountA <- accountGen
      accountB <- accountGen
      ts       <- timestampGen
      genesis1 = GenesisTransaction.create(company.toAddress, ENOUGH_AMT, ts).explicitGet()
      genesis2 = GenesisTransaction.create(king.toAddress, ENOUGH_AMT, ts).explicitGet()
      genesis3 = GenesisTransaction.create(notary.toAddress, ENOUGH_AMT, ts).explicitGet()
      genesis4 = GenesisTransaction.create(accountA.toAddress, ENOUGH_AMT, ts).explicitGet()
      genesis5 = GenesisTransaction.create(accountB.toAddress, ENOUGH_AMT, ts).explicitGet()

      assetScript = s"""
                    |
                    | match tx {
                    |   case ttx: TransferTransaction =>
                    |      let king = Address(base58'${king.address}')
                    |      let company = Address(base58'${company.address}')
                    |      let notary1 = addressFromPublicKey(extract(getBinary(king, "notary1PK")))
                    |      let txIdBase58String = toBase58String(ttx.id)
                    |      let isNotary1Agreed = match getBoolean(notary1,txIdBase58String) {
                    |        case b : Boolean => b
                    |        case _ : Unit => false
                    |      }
                    |      let recipientAddress = addressFromRecipient(ttx.recipient)
                    |      let recipientAgreement = getBoolean(recipientAddress,txIdBase58String)
                    |      let isRecipientAgreed = if(isDefined(recipientAgreement)) then extract(recipientAgreement) else false
                    |      let senderAddress = addressFromPublicKey(ttx.senderPublicKey)
                    |      senderAddress.bytes == company.bytes || (isNotary1Agreed && isRecipientAgreed)
                    |   case other => throw()
                    | }
        """.stripMargin

      untypedScript = Parser(assetScript).get.value

      typedScript = ScriptV1(CompilerV1(compilerContext(V1, isAssetScript = false), untypedScript).explicitGet()._1).explicitGet()

      issueTransaction = IssueTransactionV2
        .selfSigned(
          AddressScheme.getAddressSchema.chainId,
          company,
          "name".getBytes(StandardCharsets.UTF_8),
          "description".getBytes(StandardCharsets.UTF_8),
          100,
          0,
          false,
          1000000,
          ts,
          Some(typedScript)
        )
        .explicitGet()

      assetId = issueTransaction.id()

      kingDataTransaction = DataTransactionV1
        .selfSigned(king, king, List(BinaryDataEntry("notary1PK", ByteStr(notary.publicKey.getEncoded))), ts + 1, 1000)
        .explicitGet()

      transferFromCompanyToA = TransferTransactionV2
        .selfSigned(company, Some(assetId), None, ts + 20, 1, 1000, accountA.toAddress, Array.empty)
        .explicitGet()

      transferFromAToB = TransferTransactionV2
        .selfSigned(accountA, Some(assetId), None, ts + 30, 1, 1000, accountB.toAddress, Array.empty)
        .explicitGet()

      notaryDataTransaction = DataTransactionV1
        .selfSigned(notary, notary, List(BooleanDataEntry(transferFromAToB.id().base58, true)), ts + 4, 1000)
        .explicitGet()

      accountBDataTransaction = DataTransactionV1
        .selfSigned(accountB, accountB, List(BooleanDataEntry(transferFromAToB.id().base58, true)), ts + 5, 1000)
        .explicitGet()
    } yield (Seq(genesis1, genesis2, genesis3, genesis4, genesis5),
             issueTransaction,
             kingDataTransaction,
             transferFromCompanyToA,
             notaryDataTransaction,
             accountBDataTransaction,
             transferFromAToB)

  private def eval(code: String) = {
    val untyped = Parser(code).get.value
    val typed   = CompilerV1(compilerContext(V1, isAssetScript = false), untyped).map(_._1)
    typed.flatMap(EvaluatorV1[EVALUATED](dummyEvalContext(V1), _))
  }

  property("Script toBase58String") {
    val s = "AXiXp5CmwVaq4Tp6h6"
    eval(s"""toBase58String(base58'$s') == \"$s\"""") shouldBe Testing.evaluated(true)
  }

  property("Script toBase64String") {
    val s = "Kl0pIkOM3tRikA=="
    eval(s"""toBase64String(base64'$s') == \"$s\"""") shouldBe Testing.evaluated(true)
  }

  property("addressFromString() returns None when address is too long") {
    val longAddress = "A" * (WavesGlobal.MaxBase58String + 1)
    eval(s"""addressFromString("$longAddress")""") shouldBe Left("base58Decode input exceeds 100")
  }

  property("Scenario") {
    forAll(preconditions) {
      case (genesis, issue, kingDataTransaction, transferFromCompanyToA, notaryDataTransaction, accountBDataTransaction, transferFromAToB) =>
        assertDiffAndState(smartEnabledFS) { append =>
          append(genesis).explicitGet()
          append(Seq(issue, kingDataTransaction, transferFromCompanyToA)).explicitGet()
          append(Seq(transferFromAToB)) should produce("NotAllowedByScript")
          append(Seq(notaryDataTransaction)).explicitGet()
          append(Seq(transferFromAToB)) should produce("NotAllowedByScript") // recipient should accept tx
          append(Seq(accountBDataTransaction)).explicitGet()
          append(Seq(transferFromAToB)).explicitGet()
        }
    }
  }
}
