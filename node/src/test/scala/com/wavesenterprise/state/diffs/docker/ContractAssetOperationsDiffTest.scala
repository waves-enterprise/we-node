package com.wavesenterprise.state.diffs.docker

import cats.Monoid
import cats.implicits._
import com.wavesenterprise.block.Block
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings.EnabledForNativeTokens
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffAndState, assertDiffEither, produce}
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, ExecutedContractTransactionV3}
import com.wavesenterprise.{NoShrink, TransactionGen, crypto}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class ContractAssetOperationsDiffTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with ContractTransactionGen
    with ExecutableTransactionGen
    with NoShrink
    with WithDomain {
  import ExecutableTransactionGen._

  property("Cannot reissue/burn non-existing alias") {
    forAll(createV5AndCallV5WithExecutedV3AndIssueWithParam(List(contractReissueV1Gen().sample.get))) {
      case ExecutedTxV3TestData(minedBlocks, signerAccount, _, executedTx) =>
        val nextBlock = block(signerAccount, Seq(executedTx))

        assertDiffEither(minedBlocks, nextBlock, EnabledForNativeTokens) { blockDiffEi =>
          blockDiffEi should produce("doesn't exist")
        }
    }

    forAll(createV5AndCallV5WithExecutedV3AndIssueWithParam(List(contractBurnV1Gen(bytes32gen.map(ByteStr(_).some)).sample.get))) {
      case ExecutedTxV3TestData(minedBlocks, signerAccount, _, executedTx) =>
        val nextBlock = block(signerAccount, Seq(executedTx))

        assertDiffEither(minedBlocks, nextBlock, EnabledForNativeTokens) { blockDiffEi =>
          blockDiffEi should produce("doesn't exist")
        }
    }
  }

  property("Can not issue/resissue > long.max") {
    val setup = for {
      ExecutedTxV3TestData(blocks, txSigner, _, executedTx) <- createV5AndCallV5WithExecutedV3AndIssueWithParam(List.empty)
      issueOperation                                        <- contractIssueV1Gen(isReissuableGen = true, maybeParentTxId = Some(executedTx.tx.id()))
      reissueOperation                                      <- contractReissueV1Gen(issueOperation.assetId, quantityGen = Gen.const(Long.MaxValue))
      executedCall <- executedTxV3ParamWithOperationsGen(
        txSigner,
        executedTx.tx,
        proofsCount = 0,
        assetOperations = issueOperation :: reissueOperation :: Nil
      )
    } yield (blocks, txSigner, executedCall)

    forAll(setup) {
      case (blocks, executedSigner, executedCall) =>
        val nextBlock = block(executedSigner, Seq(executedCall))

        assertDiffEither(blocks, nextBlock, EnabledForNativeTokens) { ei =>
          ei should produce("Asset total value overflow")
        }
    }
  }

  property("Can not issue asset with nonce = 0") {
    val setup = for {
      ExecutedTxV3TestData(blocks, txSigner, _, executedTx) <- createV5AndCallV5WithExecutedV3AndIssueWithParam(List.empty)
      quantity                                              <- Gen.choose(Long.MaxValue / 200, Long.MaxValue / 100)
      issueOperation                                        <- contractIssueV1Gen(nonceGen = 0.toByte, isReissuableGen = true, quantityGen = quantity)
      executedIssueTx <- executedTxV3ParamWithOperationsGen(
        txSigner,
        executedTx.tx,
        proofsCount = 0,
        assetOperations = issueOperation :: Nil
      )
    } yield (blocks, executedIssueTx, txSigner)

    forAll(setup) {
      case (blocks, executedIssueTx, txSigner) =>
        val nextBlock = block(txSigner, Seq(executedIssueTx))

        assertDiffEither(blocks, nextBlock, EnabledForNativeTokens) { ei =>
          ei should produce("Attempt to issue asset with nonce = 0")
        }
    }
  }

  property("Can't issue multiple assets with the same nonce") {
    val setup = for {
      ExecutedTxV3TestData(blocks, txSigner, _, executedTx) <- createV5AndCallV5WithExecutedV3AndIssueWithParam(List.empty)
      quantity                                              <- Gen.choose(Long.MaxValue / 200, Long.MaxValue / 100)
      nonce                                                 <- issueNonceGen
      issueOperation1                                       <- contractIssueV1Gen(nonceGen = nonce, isReissuableGen = true, quantityGen = quantity)
      issueOperation2                                       <- contractIssueV1Gen(nonceGen = nonce, isReissuableGen = true, quantityGen = quantity)
      executedIssueTx <- executedTxV3ParamWithOperationsGen(
        txSigner,
        executedTx.tx,
        proofsCount = 0,
        assetOperations = issueOperation1 :: issueOperation2 :: Nil
      )
    } yield (blocks, executedIssueTx, txSigner)

    forAll(setup) {
      case (blocks, executedIssueTx, txSigner) =>
        val nextBlock = block(txSigner, Seq(executedIssueTx))

        assertDiffEither(blocks, nextBlock, EnabledForNativeTokens) { ei =>
          ei should produce(s"Attempt to issue multiple assets with the same nonce")
        }
    }
  }

  property("Issue+Reissue+Burn operations must not break WEST balance invariant") {
    val setup = for {
      ExecutedTxV3TestData(blocks, txSigner, _, executedTx) <- createV5AndCallV5WithExecutedV3AndIssueWithParam(List.empty)
      quantity                                              <- Gen.choose(Long.MaxValue / 200, Long.MaxValue / 100)
      (issueOperation, issueAssetId) <- contractIssueV1Gen(isReissuableGen = true, quantityGen = quantity, maybeParentTxId = Some(executedTx.tx.id()))
        .map(itx => itx -> ByteStr(crypto.fastHash(executedTx.tx.id().arr :+ itx.nonce)))
      reissueOperation <- contractReissueV1Gen(
        assetGen = issueAssetId,
        quantityGen = quantity / 2
      )
      burnOperation <- contractBurnV1Gen(issueAssetId.some)
      executedCall <- executedTxV3ParamWithOperationsGen(
        txSigner,
        executedTx.tx,
        proofsCount = 0,
        assetOperations = issueOperation :: reissueOperation :: burnOperation :: Nil
      )
      operations = (issueOperation, reissueOperation, burnOperation)
    } yield (blocks, issueAssetId, operations, executedCall, txSigner)

    forAll(setup) {
      case (blocks, issueAssetId, (issueOperation, reissueOperation, burnOperation), executedTx, txSigner) =>
        val nextBlock = block(txSigner, Seq(executedTx))

        assertDiffAndState(blocks, nextBlock, EnabledForNativeTokens) {
          case (blockDiff, newState) =>
            val totalPortfolioDiff = Monoid.combineAll(blockDiff.portfolios.values)

            val totalAssetVolume = issueOperation.quantity + reissueOperation.quantity - burnOperation.amount

            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets shouldBe Map(issueAssetId -> totalAssetVolume)

            newState.contractBalance(executedTx.tx.contractId, Some(issueAssetId)) shouldBe totalAssetVolume
        }
    }
  }

  property("Differ must throw exception when trying to reissue or burn asset not by issuer contract") {

    import com.wavesenterprise.utils.EitherUtils._

    val assetId = ByteStr(Array.fill[Byte](32)(0))
    val issueOp = ContractAssetOperation.ContractIssueV1(
      assetId,
      name = "issue",
      description = "",
      quantity = 100000L,
      decimals = 0,
      isReissuable = true,
      nonce = 1
    )

    val reissueOp = ContractAssetOperation.ContractReissueV1(
      assetId,
      50000L,
      isReissuable = true
    )

    val burnOp = ContractAssetOperation.ContractBurnV1(
      Some(assetId),
      50000L
    )

    def setup(initOp: ContractAssetOperation, violatingOp: ContractAssetOperation): Gen[(List[Block], Block)] =
      for {
        accountA    <- accountGen
        genesisTime <- ntpTimestampGen.map(_ - 10.minute.toMillis)
        genesisTxs = List(
          GenesisTransaction.create(accountA.toAddress, ENOUGH_AMT, genesisTime).explicitGet,
        )
        genesisBlock = block(accountA, genesisTxs)
        issueBlock       <- blockWithExecutedCreateV5Gen(accountA, List(initOp))
        violatingOpBlock <- blockWithExecutedCreateV5Gen(accountA, List(violatingOp)) // trying to do reissue/burn from not issuer contract
        initialBlocks = List(genesisBlock, issueBlock)
      } yield (initialBlocks, violatingOpBlock)

    setup(issueOp, reissueOp).sample match {
      case Some((initialBlocks, violatingReissueBlock)) =>
        val emptyAssertion = (_: Diff, _: Blockchain) => ()
        val contractId     = violatingReissueBlock.transactionData.head.asInstanceOf[ExecutedContractTransactionV3].tx.id()

        val error = intercept[java.lang.Exception] {
          assertDiffAndState(initialBlocks, violatingReissueBlock, EnabledForNativeTokens)(emptyAssertion)
        }

        error.getMessage should startWith(
          s"TransactionValidationError(GenericError(Asset '${assetId.base58}' was not issued by '${Contract(contractId)}'"
        )
    }

    setup(issueOp, burnOp).sample match {
      case Some((initialBlocks, violatingBurnBlock)) =>
        val emptyAssertion = (_: Diff, _: Blockchain) => ()
        val contractId     = violatingBurnBlock.transactionData.head.asInstanceOf[ExecutedContractTransactionV3].tx.id()

        val error = intercept[java.lang.Exception] {
          assertDiffAndState(initialBlocks, violatingBurnBlock, EnabledForNativeTokens)(emptyAssertion)
        }

        val errorMessage =
          s"contract '${contractId}' balance validation errors: [negative asset balance: [asset: '${assetId.base58}' -> balance: '-${burnOp.amount}']]"

        error.getMessage should include(errorMessage)
    }
  }

}
