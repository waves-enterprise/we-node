package com.wavesenterprise.state.diffs.smart.performance

import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.v1.compiler.CompilerV1
import com.wavesenterprise.lang.v1.compiler.Terms._
import com.wavesenterprise.lang.v1.parser.Parser
import com.wavesenterprise.metrics.Instrumented
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.state.diffs.smart._
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.SmartContractV1Utils.compilerContext
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SigVerifyPerformanceTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val AmtOfTxs = 10000

  private def simpleSendGen(from: PrivateKeyAccount, to: PublicKeyAccount, ts: Long): Gen[TransferTransaction] =
    for {
      amt <- smallFeeGen
      fee <- smallFeeGen
    } yield TransferTransactionV2.selfSigned(from, None, None, ts, amt, fee, to.toAddress, Array.emptyByteArray).explicitGet()

  private def scriptedSendGen(from: PrivateKeyAccount, to: PublicKeyAccount, ts: Long): Gen[TransferTransactionV2] =
    for {
      amt <- smallFeeGen
      fee <- smallFeeGen
    } yield TransferTransactionV2.selfSigned(from, None, None, ts, amt, fee, to.toAddress, Array.emptyByteArray).explicitGet()

  private def differentTransfers(typed: EXPR) =
    for {
      master    <- accountGen
      recipient <- accountGen
      ts        <- positiveIntGen
      amt       <- smallFeeGen
      fee       <- smallFeeGen
      genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      setScript <- selfSignedSetScriptTransactionGenP(master, ScriptV1(typed).explicitGet())
      transfer       = simpleSendGen(master, recipient, ts)
      scriptTransfer = scriptedSendGen(master, recipient, ts)
      transfers       <- Gen.listOfN(AmtOfTxs, transfer)
      scriptTransfers <- Gen.listOfN(AmtOfTxs, scriptTransfer)
    } yield (genesis, setScript, transfers, scriptTransfers)

  ignore("parallel native signature verification vs sequential scripted signature verification") {
    val textScript    = "sigVerify(tx.bodyBytes,tx.proofs[0],tx.senderPk)"
    val untypedScript = Parser(textScript).get.value
    val typedScript   = CompilerV1(compilerContext(V1, isAssetScript = false), untypedScript).explicitGet()._1

    forAll(differentTransfers(typedScript)) {
      case (gen, setScript, transfers, scriptTransfers) =>
        def simpleCheck(): Unit = assertDiffAndState(Seq(TestBlock.create(Seq(gen))), TestBlock.create(transfers), smartEnabledFS) { case _ => }
        def scriptedCheck(): Unit =
          assertDiffAndState(Seq(TestBlock.create(Seq(gen, setScript))), TestBlock.create(scriptTransfers), smartEnabledFS) {
            case _ =>
          }

        val simeplCheckTime   = Instrumented.withTime(simpleCheck())._2
        val scriptedCheckTime = Instrumented.withTime(scriptedCheck())._2
        println(s"[parallel] simple check time: $simeplCheckTime ms,\t [seqential] scripted check time: $scriptedCheckTime ms")
    }

  }
}
