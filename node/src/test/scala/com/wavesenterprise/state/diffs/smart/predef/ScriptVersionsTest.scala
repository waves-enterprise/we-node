package com.wavesenterprise.state.diffs.smart.predef

import com.google.common.base.Charsets
import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lang.ScriptVersion.Versions._
import com.wavesenterprise.lang.v1.compiler.CompilerV1
import com.wavesenterprise.lang.v1.compiler.Terms.{EVALUATED, TRUE}
import com.wavesenterprise.lang.v1.parser.Parser
import com.wavesenterprise.lang.{ScriptVersion, Testing}
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.smart.SetScriptTransactionV1
import com.wavesenterprise.transaction.smart.script.ScriptRunner
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.EmptyBlockchain
import com.wavesenterprise.utils.SmartContractV1Utils.compilerContext
import fastparse.Parsed.Success
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import shapeless.Coproduct
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ScriptVersionsTest extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {
  val CHAIN_ID = AddressScheme.getAddressSchema.chainId

  def eval[T <: EVALUATED](script: String,
                           version: ScriptVersion,
                           tx: Transaction = null,
                           blockchain: Blockchain = EmptyBlockchain): Either[String, T] = {
    val Success(expr, _) = Parser(script)
    for {
      compileResult <- CompilerV1(compilerContext(version, isAssetScript = false), expr)
      (typedExpr, _) = compileResult
      s <- ScriptV1(version, typedExpr, checkSize = false)
      r <- ScriptRunner[T](blockchain.height, Coproduct(tx), blockchain, s, isTokenScript = false)._2
    } yield r

  }

  val duplicateNames =
    """
      |match tx {
      |  case tx: TransferTransaction => true
      |  case _ => false
      |}
    """.stripMargin

  val orderTypeBindings = "let t = Buy; t == Buy"

  "ScriptV1" - {
    "forbids duplicate names" in {
      import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}

      val Success(expr, _)      = Parser(duplicateNames)
      val Right((typedExpr, _)) = CompilerV1(compilerContext(V1, isAssetScript = false), expr)
      val settings = TestFunctionalitySettings.Enabled.copy(
        preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.SmartAccountTrading.id -> 3))
      val setup = for {
        master <- accountGen
        ts     <- positiveLongGen
        genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
        script  = ScriptV1(V1, typedExpr, checkSize = false).explicitGet()
        tx = SetScriptTransactionV1
          .selfSigned(CHAIN_ID, master, Some(script), "script".getBytes(Charsets.UTF_8), Array.empty[Byte], 100000, ts + 1)
          .explicitGet()
      } yield (genesis, tx)

      forAll(setup) {
        case (genesis, tx) =>
          assertDiffEi(Seq(block(Seq(genesis))), block(Seq(tx)), settings) { blockDiffEi =>
            blockDiffEi should produce("duplicate variable names")
          }

          assertDiffEi(Seq(block(Seq(genesis)), block(Seq())), block(Seq(tx)), settings) { blockDiffEi =>
            blockDiffEi shouldBe 'right
          }
      }
    }

    "does not have bindings defined in V2" in {
      eval[EVALUATED](orderTypeBindings, V1) should produce("definition of 'Buy' is not found")
    }
  }

  "ScriptV2" - {
    "allows duplicate names" in {
      forAll(transferV2Gen) { tx =>
        eval[EVALUATED](duplicateNames, V2, tx) shouldBe Testing.evaluated(true)
      }
    }

    "has bindings defined in V2" in {
      eval[EVALUATED](orderTypeBindings, V2) shouldBe Testing.evaluated(true)
    }

    "only works after SmartAccountTrading feature activation" in {
      import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}

      val settings = TestFunctionalitySettings.Enabled.copy(preActivatedFeatures = Map(BlockchainFeature.SmartAccountTrading.id -> 3))
      val setup = for {
        master <- accountGen
        ts     <- positiveLongGen
        genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
        script  = ScriptV1(V2, TRUE, checkSize = false).explicitGet()
        tx = SetScriptTransactionV1
          .selfSigned(AddressScheme.getAddressSchema.chainId,
                      master,
                      Some(script),
                      "script".getBytes(Charsets.UTF_8),
                      Array.empty[Byte],
                      100000,
                      ts + 1)
          .explicitGet()
      } yield (genesis, tx)

      forAll(setup) {
        case (genesis, tx) =>
          assertDiffEi(Seq(block(Seq(genesis))), block(Seq(tx)), settings) { blockDiffEi =>
            blockDiffEi should produce("has not been activated yet")
          }

          assertDiffEi(Seq(block(Seq(genesis)), block(Seq())), block(Seq(tx)), settings) { blockDiffEi =>
            blockDiffEi shouldBe 'right
          }
      }
    }
  }
}
