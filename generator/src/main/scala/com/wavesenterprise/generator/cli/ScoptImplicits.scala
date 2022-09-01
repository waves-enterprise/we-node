package com.wavesenterprise.generator.cli

import com.wavesenterprise.state._
import play.api.libs.json._
import scopt.Read

import scala.concurrent.duration.FiniteDuration

trait ScoptImplicits {
  implicit def scoptOptionReads[T](implicit r: Read[T]): Read[Option[T]] = Read.stringRead.map {
    case "null" => None
    case x      => Option(r.reads(x))
  }

  implicit val finiteDurationRead: Read[FiniteDuration] = Read.durationRead.map { x =>
    if (x.isFinite()) FiniteDuration(x.length, x.unit)
    else throw new IllegalArgumentException(s"Duration '$x' expected to be finite")
  }

  implicit val jsonAsListDataEntry: Read[List[DataEntry[_]]] = Read.reads(json => Json.parse(json).as[List[DataEntry[_]]])
}
