package com.wavesenterprise.mining

import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalamock.scalatest.PathMockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.wavesenterprise.transaction.Transaction
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class OneDimensionalMiningConstraintSuite
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with PathMockFactory
    with TransactionGen
    with NoShrink {
  "OneDimensionalMiningConstraint" - {
    "should be empty if the limit is 0, but not overfilled" in {
      val tank = createConstConstraint(0, 1)
      tank.isFull shouldBe true
      tank.isOverfilled shouldBe false
    }

    "put(transaction)" - tests { (maxTxs, txs) =>
      val constraint = createConstConstraint(maxTxs, transactionSize = 1)
      txs.foldLeft(constraint)(_.put(stub[Blockchain], _))
    }
  }

  private def tests(toConstraint: (Int, List[Transaction]) => MiningConstraint): Unit = {
    val dontReachLimitGen: Gen[MiningConstraint] = for {
      maxTxs   <- Gen.chooseNum(1, Int.MaxValue)
      txNumber <- Gen.chooseNum(0, maxTxs - 1)
      txs      <- Gen.listOfN(math.min(txNumber, 15), randomTransactionGen)
    } yield toConstraint(maxTxs, txs)

    "multiple items don't reach the limit" in forAll(dontReachLimitGen) { updatedConstraint =>
      updatedConstraint.isFull shouldBe false
      updatedConstraint.isOverfilled shouldBe false
    }

    val reachSoftLimitGen: Gen[MiningConstraint] = for {
      maxTxs <- Gen.chooseNum(1, 10)
      txs    <- Gen.listOfN(maxTxs, randomTransactionGen)
    } yield toConstraint(maxTxs, txs)

    "multiple items reach the limit softly" in forAll(reachSoftLimitGen) { updatedConstraint =>
      updatedConstraint.isFull shouldBe true
      updatedConstraint.isOverfilled shouldBe false
    }

    val reachHardLimitGen: Gen[MiningConstraint] = for {
      maxTxs   <- Gen.chooseNum(1, 10)
      txNumber <- Gen.chooseNum(maxTxs + 1, maxTxs + 10)
      txs      <- Gen.listOfN(txNumber, randomTransactionGen)
    } yield toConstraint(maxTxs, txs)

    "multiple items reach the limit with gap" in forAll(reachHardLimitGen) { updatedConstraint =>
      updatedConstraint.isFull shouldBe true
      updatedConstraint.isOverfilled shouldBe true
    }
  }
}
