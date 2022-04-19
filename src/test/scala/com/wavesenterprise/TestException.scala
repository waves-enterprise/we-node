package com.wavesenterprise

class TestException(message: String) extends RuntimeException(message) {
  override def getStackTrace: Array[StackTraceElement] = Array.empty
}

object TestException {
  def apply(message: String): TestException = {
    new TestException(message)
  }

  def apply(): TestException = {
    new TestException("Expected test exception")
  }
}
