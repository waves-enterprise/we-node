package com.wavesenterprise.state.diffs

import com.wavesenterprise.acl.{OpType, PermissionsGen}
import com.wavesenterprise.state.ParticipantRegistration
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import cats.implicits._
import com.wavesenterprise.state.Diff._

import scala.util.Random
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ParticipantRegistrationCombinationTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with NoShrink
    with MockFactory {
  def participantRegistrationGen(opTypeGen: Gen[OpType] = PermissionsGen.permissionOpTypeGen): Gen[ParticipantRegistration] = {
    for {
      address <- addressGen
      pubKey  <- accountGen
      opType  <- opTypeGen
      parReg = ParticipantRegistration(address, pubKey, opType)
    } yield parReg
  }

  property("add + delete = nothing") {
    val addWithRemove = for {
      add <- participantRegistrationGen(Gen.const(OpType.Add))
      remove = add.copy(opType = OpType.Remove)
    } yield (add, remove)

    forAll(addWithRemove) {
      case (add, remove) =>
        Seq(add).combine(Seq(remove)) shouldBe empty
    }
  }

  property("add + add = one add") {
    val addWithAdd = for {
      add <- participantRegistrationGen(Gen.const(OpType.Add))
      secondAdd = add.copy()
    } yield (add, secondAdd)

    forAll(addWithAdd) {
      case (add, secondAdd) =>
        Seq(add).combine(Seq(secondAdd)) should contain theSameElementsAs Seq(add)
    }
  }

  property("remove + remove = one remove") {
    val removeWithRemove = for {
      remove <- participantRegistrationGen(Gen.const(OpType.Remove))
      secondRemove = remove.copy()
    } yield (remove, secondRemove)

    forAll(removeWithRemove) {
      case (remove, secondRemove) =>
        Seq(remove).combine(Seq(secondRemove)) should contain theSameElementsAs Seq(remove)
    }
  }

  property("combine(add N participants; remove some of N; some random participantRegistration) should return right result") {
    val (minElems, maxElems) = (1, 20)
    val threeListsOfParRegGen = for {
      totalCount          <- Gen.choose(minElems, maxElems)
      parRegs             <- Gen.listOfN(totalCount, participantRegistrationGen(Gen.const(OpType.Add)))
      secondTotalCount    <- Gen.choose(minElems, maxElems)
      secondParRegs       <- Gen.listOfN(secondTotalCount, participantRegistrationGen())
      removedParRegsCount <- Gen.choose(minElems, maxElems)
      removedParRegs = Random.shuffle(parRegs).take(removedParRegsCount).map(_.copy(opType = OpType.Remove))
    } yield (parRegs, removedParRegs, secondParRegs)

    forAll(threeListsOfParRegGen) {
      case (oldParRegs, removedParRegs, newParRegs) =>
        val oldParRegsSeq: Seq[ParticipantRegistration] = oldParRegs
        val combineRes                                  = oldParRegsSeq.combine(removedParRegs ++ newParRegs)
        combineRes.size shouldBe (oldParRegs.size - removedParRegs.size + newParRegs.size)
    }
  }

  property("foldLeft is same as combine with empty seq") {
    forAll(Gen.listOf(participantRegistrationGen())) { regSeq: Seq[ParticipantRegistration] =>
      val afterFoldLeft = regSeq.foldLeft(Seq.empty[ParticipantRegistration]) {
        case (cleanedRegs, reg) =>
          cleanedRegs.combine(Seq(reg))
      }

      regSeq.combine(Seq.empty[ParticipantRegistration]) should contain theSameElementsAs afterFoldLeft
    }
  }

  property("same elements in one sequence combine with empty sequence should return one element as result") {
    val addWithAdd = for {
      add <- participantRegistrationGen(Gen.const(OpType.Add))
      secondAdd = add.copy()
    } yield (add, secondAdd)

    forAll(addWithAdd) {
      case (add, secondAdd) =>
        Seq(add, secondAdd).combine(Seq.empty) should contain theSameElementsAs Seq(add)
    }
  }

  property("different elements with same address in one sequence combine with empty sequence should return same sequence") {
    val addWithRemove = for {
      add <- participantRegistrationGen(Gen.const(OpType.Add))
      remove = add.copy(opType = OpType.Remove)
    } yield (add, remove)

    forAll(addWithRemove) {
      case (add, remove) =>
        recombine(Seq(add, remove)) should contain theSameElementsAs Seq.empty[ParticipantRegistration]
        Seq(add, remove).combine(Seq.empty) shouldNot contain theSameElementsAs Seq(add, remove)
    }
  }

  property("empty sequence combine with same elements in one sequence should return one element as result") {
    val addWithAdd = for {
      add <- participantRegistrationGen(Gen.const(OpType.Add))
      secondAdd = add.copy()
    } yield (add, secondAdd)

    forAll(addWithAdd) {
      case (add, secondAdd) =>
        recombine(Seq(add, secondAdd)) should contain theSameElementsAs Seq(add)
        Seq.empty[ParticipantRegistration].combine(Seq(add, secondAdd)) should contain theSameElementsAs Seq(add)
    }
  }

  property("empty sequence combine with different elements with same address in one sequence should return same sequence") {
    val addWithRemove = for {
      add <- participantRegistrationGen(Gen.const(OpType.Add))
      remove = add.copy(opType = OpType.Remove)
    } yield (add, remove)

    forAll(addWithRemove) {
      case (add, remove) =>
        recombine(Seq(add, remove)) should contain theSameElementsAs Seq.empty[ParticipantRegistration]
        Seq.empty[ParticipantRegistration].combine(Seq(add, remove)) shouldNot contain theSameElementsAs Seq(add, remove)
    }
  }

}
