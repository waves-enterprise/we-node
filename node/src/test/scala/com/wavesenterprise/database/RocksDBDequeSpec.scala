package com.wavesenterprise.database

import com.google.common.primitives.Shorts
import com.wavesenterprise.WithDB
import monix.execution.atomic.AtomicShort
import org.scalacheck.Gen
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.testcontainers.shaded.com.google.common.primitives.Ints
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

import java.util
import scala.collection.convert.ImplicitConversions._

class RocksDBDequeSpec extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with WithDB
    with Matchers {

  private sealed trait Operation[T] {
    def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion
  }

  private object Operation {

    case class AddFirst[T](value: T) extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.addFirst(value)
        rocksDBDeque.addFirst(value)

        referenceDeque.toList shouldBe rocksDBDeque.toList
      }
    }

    case class AddLast[T](value: T) extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.addLast(value)
        rocksDBDeque.addLast(value)

        referenceDeque.toList shouldBe rocksDBDeque.toList
      }
    }

    case class PeekFirst[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        val refHead     = Option(referenceDeque.peekFirst())
        val rocksDBHead = rocksDBDeque.head

        refHead shouldBe rocksDBHead
      }
    }

    case class PeekLast[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        val refTail     = Option(referenceDeque.peekLast())
        val rocksDBTail = rocksDBDeque.last

        refTail shouldBe rocksDBTail
      }
    }

    case class PollFirst[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.pollFirst()
        rocksDBDeque.pollFirst

        referenceDeque.toList shouldBe rocksDBDeque.toList
      }
    }

    case class PollLast[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.pollLast()
        rocksDBDeque.pollLast

        referenceDeque.toList shouldBe rocksDBDeque.toList
      }
    }

    case class Contains[T](value: T) extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.contains(value) shouldBe rocksDBDeque.contains(value)
      }
    }

    case class Size[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.size shouldBe rocksDBDeque.size
      }
    }

    case class IsEmpty[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.isEmpty shouldBe rocksDBDeque.isEmpty
      }
    }

    case class NonEmpty[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.nonEmpty shouldBe rocksDBDeque.nonEmpty
      }
    }

    case class ToList[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.toList shouldBe rocksDBDeque.toList
      }
    }

    case class Clear[T]() extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.clear()
        rocksDBDeque.clear()

        referenceDeque.toList shouldBe rocksDBDeque.toList
        referenceDeque.size shouldBe rocksDBDeque.size
      }
    }

    case class Slice[T](from: Int, until: Int) extends Operation[T] {
      def apply(referenceDeque: util.ArrayDeque[T], rocksDBDeque: RocksDBDeque[T]): Assertion = {
        referenceDeque.slice(from, until) shouldBe rocksDBDeque.slice(from, until)
      }
    }

    def gen[T](valueGen: Gen[T], indexGen: Gen[Int]): Gen[Operation[T]] =
      Gen.oneOf(
        addFirstGen(valueGen),
        addLastGen(valueGen),
        peekFirstGen[T],
        peekLastGen[T],
        pollFirstGen[T],
        pollLastGen[T],
        containsGen(valueGen),
        sizeGen[T],
        isEmptyGen[T],
        nonEmptyGen[T],
        toListGen[T],
        clearGen[T],
        sliceGen[T](indexGen)
      )

    def addFirstGen[T](valueGen: Gen[T]): Gen[AddFirst[T]] =
      valueGen.flatMap(value => Gen.const(AddFirst(value)))

    def addLastGen[T](valueGen: Gen[T]): Gen[AddLast[T]] =
      valueGen.flatMap(value => Gen.const(AddLast(value)))

    def peekFirstGen[T]: Gen[PeekFirst[T]] =
      Gen.const(PeekFirst[T]())

    def peekLastGen[T]: Gen[PeekLast[T]] =
      Gen.const(PeekLast[T]())

    def pollFirstGen[T]: Gen[PollFirst[T]] =
      Gen.const(PollFirst[T]())

    def pollLastGen[T]: Gen[PollLast[T]] =
      Gen.const(PollLast[T]())

    def containsGen[T](valueGen: Gen[T]): Gen[Contains[T]] =
      valueGen.flatMap(value => Gen.const(Contains(value)))

    def sizeGen[T]: Gen[Size[T]] = Gen.const(Size[T]())

    def isEmptyGen[T]: Gen[IsEmpty[T]] = Gen.const(IsEmpty[T]())

    def nonEmptyGen[T]: Gen[NonEmpty[T]] = Gen.const(NonEmpty[T]())

    def toListGen[T]: Gen[ToList[T]] = Gen.const(ToList[T]())

    def clearGen[T]: Gen[Clear[T]] = Gen.const(Clear[T]())

    def sliceGen[T](indexGen: Gen[Int]): Gen[Slice[T]] = {
      for {
        from  <- indexGen
        until <- indexGen
      } yield Slice[T](from, until)
    }
  }

  private val prefixCounter = AtomicShort(0)

  property("rocksDBDeque state equivalent to the ArrayDeque after several operations") {
    forAll(operationsGen) { operations =>
      withClue(s"Operations seq [${operations.mkString(", ")}]") {
        val uniquePrefix = prefixCounter.incrementAndGet()

        val rocksDBDeque = new RocksDBDeque(
          s"$uniquePrefix-test-deque",
          Shorts.toByteArray(uniquePrefix),
          storage,
          Ints.toByteArray,
          Ints.fromByteArray
        )

        val referenceDeque = new util.ArrayDeque[Int]()

        operations.foreach { operation =>
          withClue(s"Incorrect operation – $operation:") {
            operation.apply(referenceDeque, rocksDBDeque)
          }
        }

        referenceDeque.toList shouldBe rocksDBDeque.toList
      }
    }
  }

  private def valueGen: Gen[Int] = Gen.chooseNum(0, 10)
  private def indexGen: Gen[Int] = Gen.chooseNum(-10, 1000)

  private def operationsGen: Gen[List[Operation[Int]]] =
    for {
      count <- Gen.chooseNum(150, 1000)
      operationGen = Operation.gen(valueGen, indexGen)
      result <- Gen.listOfN(count, operationGen)
    } yield result
}
