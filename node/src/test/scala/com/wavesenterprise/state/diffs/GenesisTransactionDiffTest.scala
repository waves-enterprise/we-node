package com.wavesenterprise.state.diffs

import cats._
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.state._
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class GenesisTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  def nelMax[T](g: Gen[T], max: Int = 10): Gen[List[T]] = Gen.choose(1, max).flatMap(Gen.listOfN(_, g))

  property("fails if height != 1") {
    forAll(genesisGen, positiveIntGen suchThat (_ > 1)) { (gtx, h) =>
      GenesisTransactionDiff(h)(gtx) should produce("GenesisTransaction cannot appear in non-initial block")
    }
  }

  property("Diff establishes WEST invariant") {
    forAll(nelMax(genesisGen)) { gtxs =>
      assertDiffAndState(Seq.empty, TestBlock.create(gtxs)) { (blockDiff, _) =>
        val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
        totalPortfolioDiff.balance shouldBe gtxs.map(_.amount).sum
        totalPortfolioDiff.effectiveBalance shouldBe gtxs.map(_.amount).sum
        totalPortfolioDiff.assets shouldBe Map.empty

        gtxs.foreach { gtx =>
          blockDiff.portfolios(gtx.recipient.toAssetHolder).balance shouldBe gtx.amount
        }
      }
    }
  }
}
