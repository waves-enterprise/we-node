package com.wavesenterprise.utils

import scala.reflect.ClassTag

/**
  * Deterministic pseudorandom number generator.
  * [[https://en.wikipedia.org/wiki/Linear_congruential_generator]]
  */
class LcgRandom(seed: Array[Byte]) {
  private[this] val a = 1103515245
  private[this] val c = 12345
  private[this] val m = Int.MaxValue

  private[this] var currentSeed: Int = nextSeed(BigInt(seed))

  def nextInt(): Int = {
    currentSeed = nextSeed(BigInt(currentSeed))
    currentSeed
  }

  def nextInt(n: Int): Int = {
    var tmp = nextInt()
    if (tmp < 0) tmp = tmp * -1
    tmp % n
  }

  def shuffle[T: ClassTag](sequence: Seq[T]): Seq[T] = {
    val arr = sequence.toArray

    def swap(i1: Int, i2: Int): Unit = {
      val tmp = arr(i1)
      arr(i1) = arr(i2)
      arr(i2) = tmp
    }

    for (n <- arr.length to 2 by -1) {
      val k = nextInt(n)
      swap(n - 1, k)
    }

    arr
  }

  private def nextSeed(currentValue: BigInt): Int = {
    ((currentValue * a + c) % m).toInt
  }
}
