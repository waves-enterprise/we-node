package com.wavesenterprise

import monix.eval.Task

package object docker {

  def deferEither[A](either: => Either[Throwable, A]): Task[A] = {
    Task.defer {
      either.fold(Task.raiseError, Task.eval(_))
    }
  }
}
