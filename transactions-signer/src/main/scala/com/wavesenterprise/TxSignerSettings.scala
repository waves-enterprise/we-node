package com.wavesenterprise

import java.io.File

import ch.qos.logback.classic.Level

case class TxSignerSettings(
    inputTransactionsFile: File,
    keystoreFile: File,
    keystorePassword: String,
    cryptoType: CryptoType,
    chainByte: Byte,
    outputTransactionsFile: File,
    loggingLevel: Level
) {
  def validate(): Either[String, Unit] = {
    for {
      _ <- validateInputPath()
      _ <- validateKeystorePath()
      _ <- validateOuputPath()
    } yield Right((): Unit)
  }

  private def validateInputPath(): Either[String, Unit] = {
    Either.cond(
      inputTransactionsFile.exists() && inputTransactionsFile.isFile,
      (),
      s"input file '${inputTransactionsFile.getAbsolutePath}' is a directory or doesn't exists"
    )
  }

  private def validateOuputPath(): Either[String, Unit] = {
    Either.cond(!outputTransactionsFile.exists(), (), s"output file '${outputTransactionsFile.getAbsolutePath}' exists")
  }

  private def validateKeystorePath(): Either[String, Unit] = {
    Either.cond(
      {
        cryptoType match {
          case CryptoType.Waves   => keystoreFile.exists() && keystoreFile.isFile
          case CryptoType.Unknown => false
        }
      },
      (),
      s"bad crypto type '${cryptoType.entryName}', or unmatched keystore '${keystoreFile.getAbsolutePath}' for it - should be file for '${CryptoType.Waves.entryName}'"
    )
  }
}

object TxSignerSettings {
  val empty: TxSignerSettings = {
    TxSignerSettings(
      new File(""),
      new File(""),
      "",
      CryptoType.Unknown,
      0.toByte,
      new File(""),
      Level.INFO
    )
  }
}
