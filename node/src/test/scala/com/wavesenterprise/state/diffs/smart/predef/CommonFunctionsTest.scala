package com.wavesenterprise.state.diffs.smart.predef

import com.wavesenterprise.account.{Address, Alias}
import com.wavesenterprise.lang.Testing._
import com.wavesenterprise.lang.v1.compiler.Terms.CONST_BYTEVECTOR
import com.wavesenterprise.lang.v1.evaluator.ctx.impl._
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.{DataTransactionV1, Proofs}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatest.Assertions
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import shapeless.Coproduct
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class CommonFunctionsTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  property("extract should transaction transfer assetId if exists") {
    forAll(transferV2Gen) { transfer =>
      val result = runScript(
        """
            |match tx {
            | case ttx : TransferTransaction  =>  extract(ttx.assetId)
            | case other => throw()
            | }
            |""".stripMargin,
        Coproduct(transfer)
      )
      transfer.assetId match {
        case Some(v) => result.explicitGet().asInstanceOf[CONST_BYTEVECTOR].bs.toArray sameElements v.arr
        case None    => result should produce("extract() called on unit")
      }
    }
  }

  property("isDefined should return true if transfer assetId exists") {
    forAll(transferV2Gen) { transfer =>
      val result = runScript(
        """
                                          |match tx {
                                          | case ttx : TransferTransaction  =>  isDefined(ttx.assetId)
                                          | case other => throw()
                                          | }
                                          |""".stripMargin,
        Coproduct(transfer)
      )
      result shouldEqual evaluated(transfer.assetId.isDefined)
    }
  }

  property("Some/None/extract/isDefined") {
    val some3 = "if true then 3 else unit"
    val none  = "if false then 3 else unit"
    runScript(some3) shouldBe evaluated(3L)
    runScript(none) shouldBe evaluated(unit)
    runScript(s"isDefined($some3)") shouldBe evaluated(true)
    runScript(s"isDefined($none)") shouldBe evaluated(false)
    runScript(s"extract($some3)") shouldBe evaluated(3L)
    runScript(s"extract($none)") should produce("extract() called on unit")
  }

  property("size()") {
    val arr = Array(1: Byte, 2: Byte, 3: Byte)
    runScript("size(base58'')".stripMargin) shouldBe evaluated(0L)
    runScript(s"size(base58'${ByteStr(arr).base58}')".stripMargin) shouldBe evaluated(3L)
  }

  property("getTransfer should extract MassTransfer transfers") {
    import scodec.bits.ByteVector
    forAll(massTransferV1WithAddressesOnlyGen.retryUntil(_.transfers.nonEmpty)) { massTransfer =>
      val resultAmount = runScript(
        """
          |match tx {
          | case mttx : MassTransferTransaction  =>  mttx.transfers[0].amount
          | case other => throw()
          | }
          |""".stripMargin,
        Coproduct(massTransfer)
      )
      resultAmount shouldBe evaluated(massTransfer.transfers.head.amount)
      val resultAddress = runScript(
        """
                                                    |match tx {
                                                    | case mttx : MassTransferTransaction  =>
                                                    |       match mttx.transfers[0].recipient {
                                                    |           case address : Address => address.bytes
                                                    |           case other => throw()
                                                    |       }
                                                    | case other => throw()
                                                    | }
                                                    |""".stripMargin,
        Coproduct(massTransfer)
      )
      resultAddress shouldBe evaluated(ByteVector(massTransfer.transfers(0).recipient.bytes.arr))
      val resultLen = runScript(
        """
                                         |match tx {
                                         | case mttx : MassTransferTransaction  =>  size(mttx.transfers)
                                         | case other => throw()
                                         | }
                                         |""".stripMargin,
        Coproduct(massTransfer)
      )
      resultLen shouldBe evaluated(massTransfer.transfers.size.toLong)
    }
  }

  property("+ should check overflow") {
    runScript("2 + 3") shouldBe evaluated(5L)
    runScript(s"1 + ${Long.MaxValue}") should produce("long overflow")
  }

  property("general shadowing verification") {
    forAll(Gen.oneOf(transferV2Gen, issueGen, massTransferV1Gen(maxTransfersCount = 10))) { tx =>
      val result = runScript(
        s"""
            |match tx {
            | case tx : TransferTransaction  => tx.id == base58'${tx.id().base58}'
            | case tx : IssueTransaction => tx.fee == ${tx.fee}
            | case tx : MassTransferTransaction => tx.timestamp == ${tx.timestamp}
            | case other => throw()
            | }
            |""".stripMargin,
        Coproduct(tx)
      )
      result shouldBe evaluated(true)
    }
  }

  property("negative shadowing verification") {
    forAll(Gen.oneOf(transferV2Gen, issueGen, massTransferV1Gen(maxTransfersCount = 10))) { tx =>
      try {
        runScript(
          s"""
               |let t = 100
               |match tx {
               | case t: TransferTransaction  => t.id == base58'${tx.id().base58}'
               | case t: IssueTransaction => t.fee == ${tx.fee}
               | case t: MassTransferTransaction => t.timestamp == ${tx.timestamp}
               | case other => throw()
               | }
               |""".stripMargin,
          Coproduct(tx)
        )
      } catch {
        case ex: MatchError =>
          Assertions.assert(ex.getMessage().contains("Compilation failed: Value 't' already defined in the scope"))
        case _: Throwable => Assertions.fail("Some unexpected error")
      }
    }
  }

  property("shadowing of empty ref") {
    try {
      runScript(
        s"""
               |match p {
               | case tx: TransferTransaction  => true
               | case other => throw()
               | }
               |""".stripMargin
      )
    } catch {
      case ex: MatchError => Assertions.assert(ex.getMessage().contains("Compilation failed: A definition of 'p' is not found"))
      case _: Throwable   => Assertions.fail("Some unexpected error")
    }
  }

  property("shadowing of inner pattern matching") {
    forAll(Gen.oneOf(transferV2Gen, issueGen)) { tx =>
      val result =
        runScript(
          s"""
               |match tx {
               | case tx: TransferTransaction | IssueTransaction => {
               |  match tx {
               |    case tx: TransferTransaction  => tx.id == base58'${tx.id().base58}'
               |    case tx: IssueTransaction => tx.fee == ${tx.fee}
               |  }
               |  }
               | case other => throw()
               |}
               |""".stripMargin,
          Coproduct(tx)
        )
      result shouldBe evaluated(true)
    }
  }

  property("shadowing of external variable") {
    // TODO: script can be simplified after NODE-837 fix
    try {
      runScript(
        s"""
           |match {
           |  let aaa = 1
           |  tx
           |} {
           |     case tx: TransferTransaction  => true
           |     case other => throw()
           | }
           |""".stripMargin
      )

    } catch {
      case ex: MatchError => Assertions.assert(ex.getMessage().contains("Compilation failed: Value 'tx' already defined in the scope"))
      case _: Throwable   => Assertions.fail("Some unexpected error")
    }
  }

  property("data constructors") {
    forAll(transferV2Gen, longEntryGen(dataAsciiKeyGen)) { (t, entry) =>
      val compareClause = t.recipient match {
        case addr: Address => s"tx.recipient == Address(base58'${addr.address}')"
        case alias: Alias  => s"""tx.recipient == Alias("${alias.name}")"""
      }
      val transferResult = runScript(
        s"""
           |match tx {
           |  case tx: TransferTransaction =>
           |    let goodEq = $compareClause
           |    let badAddressEq = tx.recipient == Address(base58'Mbembangwana')
           |    let badAddressNe = tx.recipient != Address(base58'3AfZaKieM5')
           |    let badAliasEq = tx.recipient == Alias("Ramakafana")
           |    let badAliasNe = tx.recipient != Alias("Nuripitia")
           |    goodEq && !badAddressEq && badAddressNe && !badAliasEq && badAliasNe
           |  case _ => throw()
           |}
           |""".stripMargin,
        Coproduct(t)
      )
      transferResult shouldBe evaluated(true)

      val dataTx = DataTransactionV1.create(t.sender, t.sender, List(entry), t.timestamp, 100000L, Proofs(Seq.empty)).explicitGet()
      val dataResult = runScript(
        s"""
           |match tx {
           |  case tx: DataTransaction =>
           |    let intEq = tx.data[0] == DataEntry("${entry.key}", ${entry.value})
           |    let intNe = tx.data[0] != DataEntry("${entry.key}", ${entry.value})
           |    let boolEq = tx.data[0] == DataEntry("${entry.key}", true)
           |    let boolNe = tx.data[0] != DataEntry("${entry.key}", true)
           |    let binEq = tx.data[0] == DataEntry("${entry.key}", base64'WROOooommmmm')
           |    let binNe = tx.data[0] != DataEntry("${entry.key}", base64'FlapFlap')
           |    let strEq = tx.data[0] == DataEntry("${entry.key}", "${entry.value}")
           |    let strNe = tx.data[0] != DataEntry("${entry.key}", "Zam")
           |    intEq && !intNe && !boolEq && boolNe && !binEq && binNe && !strEq && strNe
           |  case _ => throw()
           |}
         """.stripMargin,
        Coproduct(dataTx)
      )
      dataResult shouldBe evaluated(true)
    }
  }

  property("data constructors bad syntax") {
    val realAddr = "3My3KZgFQ3CrVHgz6vGRt8687sH4oAA1qp8"
    val cases = Seq(
      (s"""Address(\"$realAddr\")""", "Compilation failed: Non-matching types"),
      ("Address(base58'GzumLunBoK', 4)", "Function 'Address' requires 1 arguments, but 2 are provided"),
      ("Address()", "Function 'Address' requires 1 arguments, but 0 are provided"),
      (s"Addr(base58'$realAddr')", "Can't find a function 'Addr'")
    )
    for ((clause, err) <- cases) {
      try {
        runScript(
          s"""
             |match tx {
             |  case tx: TransferTransaction =>
             |    let dza = $clause
             |    throw()
             |  case _ => throw()
             |}
             |""".stripMargin
        )
      } catch {
        case ex: MatchError => Assertions.assert(ex.getMessage().contains(err))
        case e: Throwable   => Assertions.fail("Unexpected error", e)
      }
    }
  }
}
