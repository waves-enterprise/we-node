package com.wavesenterprise.state.diffs.smart.scenarios

import com.wavesenterprise.api.http.ApiError.ScriptExecutionError
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.WavesGlobal.MaxBase58Bytes
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.v1.compiler.CompilerV1
import com.wavesenterprise.lang.v1.parser.Parser
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.state.diffs.smart.smartEnabledFS
import com.wavesenterprise.transaction.smart.SetScriptTransaction
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.{CreateAliasTransaction, DataTransactionV1, GenesisTransaction, Proofs}
import com.wavesenterprise.utils.SmartContractV1Utils.compilerContext
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class OracleDataTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  val preconditions
      : Gen[(GenesisTransaction, GenesisTransaction, CreateAliasTransaction, SetScriptTransaction, DataTransactionV1, TransferTransactionV2)] =
    for {
      master <- accountGen
      oracle <- accountGen
      alice  <- addressGen
      ts     <- positiveIntGen
      genesis  = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      genesis2 = GenesisTransaction.create(oracle.toAddress, ENOUGH_AMT, ts).explicitGet()
      alias           <- aliasGen
      createAlias     <- createAliasV2Gen(oracle, alias, 400000, System.currentTimeMillis())
      long            <- longEntryGen(dataAsciiKeyGen)
      bool            <- booleanEntryGen(dataAsciiKeyGen).filter(_.key != long.key)
      bin             <- binaryEntryGen(MaxBase58Bytes, dataAsciiKeyGen).filter(e => e.key != long.key && e.key != bool.key)
      str             <- stringEntryGen(500, dataAsciiKeyGen).filter(e => e.key != long.key && e.key != bool.key && e.key != bin.key)
      dataTransaction <- dataTransactionV1GenP(oracle, List(long, bool, bin, str))
      allFieldsRequiredScript = s"""
                                   | match tx {
                                   | case t : DataTransaction =>
                                   |   let txId = match extract(transactionById(t.id)) {
                                   |     case d: DataTransaction => d.bodyBytes == base64'${ByteStr(dataTransaction.bodyBytes.apply()).base64}'
                                   |     case _ => false
                                   |   }
                                   |   let txHeightId = extract(transactionHeightById(t.id)) > 0
                                   |   txId && txHeightId
                                   | case _ : CreateAliasTransaction => true
                                   | case other =>
                                   |   let oracle = Alias("${alias.name}")
                                   |   let long = extract(getInteger(oracle,"${long.key}")) == ${long.value}
                                   |   let bool = extract(getBoolean(oracle,"${bool.key}")) == ${bool.value}
                                   |   let bin = extract(getBinary(oracle,"${bin.key}")) == base58'${bin.value.base58}'
                                   |   let str = extract(getString(oracle,"${str.key}")) == "${str.value}"
                                   |   long && bool && bin && str
                                   |}""".stripMargin
      setScript <- {
        val untypedAllFieldsRequiredScript = Parser(allFieldsRequiredScript).get.value
        val typedAllFieldsRequiredScript   = CompilerV1(compilerContext(V1, isAssetScript = false), untypedAllFieldsRequiredScript).explicitGet()._1
        selfSignedSetScriptTransactionGenP(master, ScriptV1(typedAllFieldsRequiredScript).explicitGet())
      }
      transferFromScripted <- versionedTransferGenP(master, alice, Proofs.empty)

    } yield (genesis, genesis2, createAlias, setScript, dataTransaction, transferFromScripted)

  property("simple oracle value required to transfer") {
    forAll(preconditions) {
      case (genesis, genesis2, createAlias, setScript, dataTransaction, transferFromScripted) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(genesis, genesis2, createAlias, setScript, dataTransaction))),
                           TestBlock.create(Seq(transferFromScripted)),
                           smartEnabledFS) { case _ => () }
        assertDiffEither(Seq(TestBlock.create(Seq(genesis, genesis2, createAlias, setScript))),
                         TestBlock.create(Seq(transferFromScripted)),
                         smartEnabledFS)(totalDiffEi => totalDiffEi shouldBe Left(_: ScriptExecutionError))
    }
  }
}
