package com.wavesenterprise.utils

object Console {

  def readPasswordFromConsoleWithVerify(format: String): Either[String, Array[Char]] =
    for {
      firstAttempt  <- readPasswordFromConsole(format)
      secondAttempt <- readPasswordFromConsole("Verify password: ")
      password <- {
        if (firstAttempt sameElements secondAttempt) {
          Right(secondAttempt)
        } else {
          println("Passwords don't match. Try again.")
          readPasswordFromConsoleWithVerify(format)
        }
      }
    } yield password

  def readPasswordFromConsole(format: String): Either[String, Array[Char]] =
    Option(System.console)
      .map(_.readPassword(format))
      .orElse {
        Option(this.readLine(format))
          .map(_.toCharArray)
      }
      .toRight("Input console is not available")

  private def readLine(format: String): String =
    if (System.console != null) {
      System.console.readLine(format)
    } else {
      print(String.format(format))
      scala.io.StdIn.readLine()
    }
}
