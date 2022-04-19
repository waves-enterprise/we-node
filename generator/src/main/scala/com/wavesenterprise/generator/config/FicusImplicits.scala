package com.wavesenterprise.generator.config

import com.google.common.base.CaseFormat
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.wavesenterprise.acl.{OpType, Role}
import com.wavesenterprise.settings.WEConfigReaders._
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.{TransactionParser, TransactionParsers}
import net.ceedubs.ficus.readers.{CollectionReaders, StringReader, ValueReader}
import play.api.libs.json.Json

trait FicusImplicits {

  implicit val distributionsReader: ValueReader[Map[TransactionParser, Double]] = {
    val converter                                = CaseFormat.LOWER_HYPHEN.converterTo(CaseFormat.UPPER_CAMEL)
    def toTxType(key: String): TransactionParser = TransactionParsers.by(converter.convert(key)).get

    CollectionReaders.mapValueReader[Double].map { xs =>
      xs.map {
        case (k, v) => {
          println(s"$k - $v")
          toTxType(k) -> v
        }
      }
    }
  }

  implicit val dataEntryReader: ValueReader[DataEntry[_]] = (config: Config, path: String) =>
    Json.parse(config.getConfig(path).root().render(ConfigRenderOptions.concise())).as[DataEntry[_]]

  implicit val roleReader: ValueReader[Role] =
    StringReader.stringValueReader.map(s => Role.fromStr(s).right.get)

  implicit val opTypeReader: ValueReader[OpType] =
    StringReader.stringValueReader.map(s => OpType.fromStr(s).right.get)
}
