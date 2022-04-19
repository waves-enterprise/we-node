package com.wavesenterprise.utils

package object chaining {
  implicit class Tap[A](private val a: A) extends AnyVal {
    def tap(g: A => Unit): A = {
      g(a)
      a
    }
  }

  implicit class Pipe[A, B](private val a: A) extends AnyVal {
    def pipe(f: A => B): B =
      f(a)
  }
}
