package com.wavesenterprise.state.diffs

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state.{BooleanDataEntry, DataEntry, Diff, IntegerDataEntry}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.{DataTransactionV1, GenesisTransaction}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ReadAccountDataBySlice extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  val fs = TestFunctionalitySettings.Enabled.copy(preActivatedFeatures = Map(BlockchainFeature.DataTransaction.id -> 0))

  val baseSetup: Gen[(GenesisTransaction, PrivateKeyAccount, Long)] = for {
    master <- accountGen
    ts     <- positiveLongGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
  } yield (genesis, master, ts)

  def data(sender: PrivateKeyAccount, data: List[DataEntry[_]], fee: Long, timestamp: Long): DataTransactionV1 =
    DataTransactionV1.selfSigned(sender, sender, data, timestamp, fee).explicitGet()

  property("read account data by slice") {
    val setup = for {
      (genesis, master, ts) <- baseSetup

      keys1   <- Gen.listOfN(10, validAliasStringGen)
      values1 <- Gen.listOfN(10, positiveLongGen)
      items1 = keys1.zip(values1).map(IntegerDataEntry.tupled)
      fee1 <- smallFeeGen
      dataTx1 = data(master, items1, fee1, ts + 10000)

      keys2   <- Gen.listOfN(10, validAliasStringGen)
      values2 <- Gen.listOfN(10, Arbitrary.arbitrary[Boolean])
      items2 = keys2.zip(values2).map(BooleanDataEntry.tupled)
      fee2 <- smallFeeGen
      dataTx2 = data(master, items2, fee2, ts + 20000)

    } yield (genesis, Seq(items1, items2), Seq(dataTx1, dataTx2))

    forAll(setup) {
      case (genesisTx, itemsList, txs) =>
        val sender  = txs.head.sender
        val genesis = block(Seq(genesisTx))
        val blocks  = txs.map(tx => block(Seq(tx)))

        val items1 = itemsList.head
        assertDiffAndState(Seq(genesis), blocks.head, fs) {
          case (totalDiff, state) =>
            val testBlock = TestBlock.create(blocks.head.timestamp + 10, blocks.head.uniqueId, Seq.empty)
            state.append(Diff.empty, 0L, testBlock)
            assertBalanceInvariant(totalDiff)
            state.addressBalance(sender.toAddress) shouldBe (ENOUGH_AMT - txs.head.fee)
            state.accountDataSlice(sender.toAddress, 0, Integer.MAX_VALUE).data.values should contain theSameElementsAs items1
            val fistFiveValues   = state.accountDataSlice(sender.toAddress, 0, 5).data.values
            val secondFiveValues = state.accountDataSlice(sender.toAddress, 5, 10).data.values
            fistFiveValues.size shouldBe 5
            secondFiveValues.size shouldBe 5
            (fistFiveValues ++ secondFiveValues) should contain theSameElementsAs items1
        }

        val items2 = itemsList(1)
        assertDiffAndState(Seq(genesis, blocks.head), blocks(1), fs) {
          case (totalDiff, state) =>
            val testBlock = TestBlock.create(blocks(1).timestamp + 10, blocks(1).uniqueId, Seq.empty)
            state.append(Diff.empty, 0L, testBlock)
            assertBalanceInvariant(totalDiff)
            state.addressBalance(sender.toAddress) shouldBe (ENOUGH_AMT - txs.take(2).map(_.fee).sum)

            state.accountDataSlice(sender.toAddress, 0, Integer.MAX_VALUE).data.values should contain theSameElementsAs (items1 ++ items2)
        }
    }
  }
}
