package com.wavesenterprise

import com.typesafe.config.Config
import com.wavesenterprise.network.message.MessageCode
import net.ceedubs.ficus.readers.ValueReader

import scala.collection.JavaConverters._

package object generator {

  implicit val messageCodeSetReader: ValueReader[Set[MessageCode]] = { (cfg: Config, path: String) =>
    cfg
      .getLongList(path)
      .asScala
      .map(_.toByte)
      .map { x =>
        if (x.isValidByte) x
        else throw new IllegalArgumentException(s"$path has an invalid value: '$x' expected to be a byte")
      }
      .toSet
  }

  def exitWithError(errorMessage: String): Nothing = {
    println(errorMessage)
    sys.exit(1)
  }

}
