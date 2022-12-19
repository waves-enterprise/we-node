package com.wavesenterprise

import java.io.File
import java.net.{InetSocketAddress, URI}

import com.typesafe.config.{ConfigMemorySize, _}
import com.wavesenterprise.state.ByteStr
import net.ceedubs.ficus.readers.namemappers.HyphenNameMapper
import net.ceedubs.ficus.readers.{NameMapper, ValueReader}
import pureconfig.{ConfigObjectSource, ConfigSource}

import scala.collection.JavaConverters._

package object settings {
  implicit val hyphenCase: NameMapper = HyphenNameMapper

  implicit val fileReader: ValueReader[File]        = (cfg, path) => new File(cfg.getString(path))
  implicit val byteStrReader: ValueReader[ByteStr]  = (cfg, path) => ByteStr.decodeBase58(cfg.getString(path)).get
  implicit val shortValueReader: ValueReader[Short] = (cfg, path) => cfg.getLong(path).toShort
  implicit val preactivatedFeaturesReader: ValueReader[Map[Short, Int]] = (config: Config, path: String) =>
    if (!config.hasPathOrNull(path) || config.getIsNull(path)) {
      Map.empty
    } else {
      config.getValue(path).valueType() match {
        case ConfigValueType.OBJECT =>
          val paf = config.getConfig(path)
          (for {
            featureId <- paf.root().keySet().asScala
          } yield featureId.toShort -> paf.getInt(featureId)).toMap
        case ConfigValueType.STRING if config.getString(path).isEmpty =>
          Map.empty
        case other =>
          throw new ConfigException.WrongType(config.getValue(path).origin(), path, ConfigValueType.OBJECT.name(), other.name())
      }

    }

  implicit val inetSocketAddressReader: ValueReader[InetSocketAddress] = { (config: Config, path: String) =>
    val uri = new URI(s"my://${config.getString(path)}")
    new InetSocketAddress(uri.getHost, uri.getPort)
  }

  implicit val configMemorySizeReader = new ValueReader[ConfigMemorySize] {
    override def read(config: Config, path: String): ConfigMemorySize = config.getMemorySize(path)
  }

  def buildSourceBasedOnDefault(customSource: ConfigObjectSource): ConfigObjectSource = {
    ConfigSource.defaultOverrides
      .withFallback(customSource)
      .withFallback(ConfigSource.defaultApplication)
      .withFallback(ConfigSource.defaultReference)
  }
}
