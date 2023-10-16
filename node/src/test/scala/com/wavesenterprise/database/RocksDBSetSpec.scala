package com.wavesenterprise.database

import com.google.common.primitives.Shorts
import com.wavesenterprise.WithDB
import com.wavesenterprise.database.rocksdb.{MainDBColumnFamily, MainReadOnlyDB, MainReadWriteDB}
import monix.execution.atomic.AtomicShort
import org.scalacheck.Gen
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.testcontainers.shaded.com.google.common.primitives.Ints

import scala.collection.mutable
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class RocksDBSetSpec extends AnyPropSpec with ScalaCheckPropertyChecks with WithDB with Matchers {
  private type MainRocksDBSet[T] = RocksDBSet[T, MainDBColumnFamily, MainReadOnlyDB, MainReadWriteDB]

  private sealed trait Operation[T] {
    def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion
  }

  private object Operation {

    case class Add[T](value: T) extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet += value
        rocksDBSet.add(value)

        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    case class AddMany[T](values: Set[T]) extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet ++= values
        rocksDBSet.add(values)

        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    case class Remove[T](value: T) extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet -= value
        rocksDBSet.remove(value)

        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    case class RemoveMany[T](values: Set[T]) extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet --= values
        rocksDBSet.remove(values)

        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    case class AddAndRemoveDisjoint[T](valuesToAdd: Set[T], valuesToRemove: Set[T]) extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet ++= valuesToAdd
        referenceSet --= valuesToRemove

        rocksDBSet.addAndRemoveDisjoint(valuesToAdd, valuesToRemove)

        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    case class Contains[T](value: T) extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet.contains(value) shouldBe rocksDBSet.contains(value)
      }
    }

    case class Size[T]() extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet.size shouldBe rocksDBSet.size
      }
    }

    case class IsEmpty[T]() extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet.isEmpty shouldBe rocksDBSet.isEmpty
      }
    }

    case class NonEmpty[T]() extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet.nonEmpty shouldBe rocksDBSet.nonEmpty
      }
    }

    case class Members[T]() extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    case class Clear[T]() extends Operation[T] {
      def apply(referenceSet: mutable.Set[T], rocksDBSet: MainRocksDBSet[T]): Assertion = {
        referenceSet.clear()
        rocksDBSet.clear()
        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }

    def gen[T](valueGen: Gen[T]): Gen[Operation[T]] =
      Gen.oneOf(
        addGen(valueGen),
        addManyGen(valueGen),
        removeGen(valueGen),
        removeManyGen(valueGen),
        addAndRemoveDisjoint(valueGen),
        containsGen(valueGen),
        sizeGen[T],
        isEmptyGen[T],
        nonEmptyGen[T],
        membersGen[T],
        clearGen[T]
      )

    def addGen[T](valueGen: Gen[T]): Gen[Add[T]] =
      valueGen.flatMap(value => Gen.const(Add(value)))

    def addManyGen[T](valueGen: Gen[T]): Gen[AddMany[T]] =
      Gen.listOf(valueGen).flatMap(values => Gen.const(AddMany(values.toSet)))

    def removeGen[T](valueGen: Gen[T]): Gen[Remove[T]] =
      valueGen.flatMap(value => Gen.const(Remove(value)))

    def removeManyGen[T](valueGen: Gen[T]): Gen[RemoveMany[T]] =
      Gen.listOf(valueGen).flatMap(values => Gen.const(RemoveMany(values.toSet)))

    def addAndRemoveDisjoint[T](valueGen: Gen[T]): Gen[AddAndRemoveDisjoint[T]] = {
      for {
        toAdd    <- Gen.listOf(valueGen).map(_.toSet)
        toRemove <- Gen.listOf(valueGen).map(_.toSet)
      } yield {
        val intersect = toAdd intersect toRemove
        AddAndRemoveDisjoint(toAdd -- intersect, toRemove -- intersect)
      }
    }

    def containsGen[T](valueGen: Gen[T]): Gen[Contains[T]] =
      valueGen.flatMap(value => Gen.const(Contains(value)))

    def sizeGen[T]: Gen[Size[T]] = Gen.const(Size[T]())

    def isEmptyGen[T]: Gen[IsEmpty[T]] = Gen.const(IsEmpty[T]())

    def nonEmptyGen[T]: Gen[NonEmpty[T]] = Gen.const(NonEmpty[T]())

    def membersGen[T]: Gen[Members[T]] = Gen.const(Members[T]())

    def clearGen[T]: Gen[Clear[T]] = Gen.const(Clear[T]())
  }

  private val prefixCounter = AtomicShort(0)

  property("RocksDBSet state equivalent to the scala set after several operations") {
    forAll(operationsGen) { operations =>
      withClue(s"Operations seq [${operations.mkString(", ")}]") {
        val uniquePrefix = prefixCounter.incrementAndGet()

        val referenceSet = mutable.Set.empty[Int]
        val rocksDBSet: RocksDBSet[Int, MainDBColumnFamily, MainReadOnlyDB, MainReadWriteDB] =
          new RocksDBSet(s"$uniquePrefix-test-set", Shorts.toByteArray(uniquePrefix), storage, Ints.toByteArray, Ints.fromByteArray)

        operations.foreach { operation =>
          withClue(s"Incorrect operation – $operation:") {
            operation.apply(referenceSet, rocksDBSet)
          }
        }

        referenceSet.toSet shouldBe rocksDBSet.members
      }
    }
  }

  private def valueGen: Gen[Int] = Gen.chooseNum(0, 10)

  private def operationsGen: Gen[List[Operation[Int]]] =
    for {
      count <- Gen.chooseNum(10, 1000)
      operationGen = Operation.gen(valueGen)
      result <- Gen.listOfN(count, operationGen)
    } yield result
}
