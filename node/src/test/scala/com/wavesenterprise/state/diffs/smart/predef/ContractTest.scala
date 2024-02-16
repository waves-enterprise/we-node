package com.wavesenterprise.state.diffs.smart.predef

import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.lang.v1.compiler.Terms.{TRUE, FALSE}
import com.wavesenterprise.transaction.AssetId
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.state.diffs.docker.ExecutableTransactionGen
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import shapeless.Coproduct

class ContractTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with ContractTransactionGen
    with ExecutableTransactionGen
    with TransactionGen
    with NoShrink {

  val preconditions: Gen[(CreateContractTransactionV5, CallContractTransactionV4, DisableContractTransactionV3, UpdateContractTransactionV3)] =
    for {
      master <- accountGen
      create <- createContractV5Gen(None,
                                    (None, createTxFeeGen),
                                    master,
                                    ValidationPolicy.Any,
                                    ContractApiVersion.Initial,
                                    List.empty[(Option[AssetId], Long)])
      call    <- callContractV4ParamGen(None)
      disable <- disableContractV3ParamGen(None)
      update  <- updateContractV3ParamGen(None)
    } yield (create, call, disable, update)

  property("CreateContract Transaction works when used by scripted accounts") {
    forAll(preconditions) {
      case (createTx, _, _, _) =>
        val result = runScript(
          """
              |match tx {
              | case c: CreateContractTransaction => false
              | case _ => true
              | }
              |""".stripMargin,
          Coproduct(createTx)
        )
        result shouldEqual Right(FALSE)
    }
  }

  property("CallContract Transaction works when used by scripted accounts") {
    forAll(preconditions) {
      case (_, callTx, _, _) =>
        val result = runScript(
          s"""
              |let contractId = base58'${callTx.contractId.toString()}'
              |let contractVersion = ${callTx.contractVersion}
              |
              |match tx {
              | case c: CallContractTransaction => c.contractId == contractId && c.contractVersion == contractVersion
              | case _ => false
              | }
              |""".stripMargin,
          Coproduct(callTx)
        )
        result shouldEqual Right(TRUE)
    }
  }

  property("DisableContract Transaction works when used by scripted accounts") {
    forAll(preconditions) {
      case (_, _, disableTx, _) =>
        val result = runScript(
          s"""
              |let contractId = base58'${disableTx.contractId.toString()}'
              |
              |match tx {
              | case d: DisableContractTransaction => d.contractId == contractId
              | case _ => false
              | }
              |""".stripMargin,
          Coproduct(disableTx)
        )
        result shouldEqual Right(TRUE)
    }
  }

  property("UpdateContract Transaction works when used by scripted accounts") {
    forAll(preconditions) {
      case (_, _, _, updateTx) =>
        val result = runScript(
          s"""
              |let contractId = base58'${updateTx.contractId.toString()}'
              |
              |match tx {
              | case u: UpdateContractTransaction => u.contractId == contractId
              | case _ => false
              | }
              |""".stripMargin,
          Coproduct(updateTx)
        )
        result shouldEqual Right(TRUE)
    }
  }
}
