package com.wavesenterprise.utils

import cats.Show

object StringUtils {
  def dashes(s: String): String =
    s.replace("\n", "\n--")

  implicit def betterOptionShow[A: Show]: Show[Option[A]] = Show.show[Option[A]] {
    case Some(value) => implicitly[Show[A]].show(value)
    case None        => "None"
  }
}
