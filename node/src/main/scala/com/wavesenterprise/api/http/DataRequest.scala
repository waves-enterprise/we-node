package com.wavesenterprise.api.http

import com.wavesenterprise.state.DataEntry

trait DataRequest extends UnsignedTxRequest {
  def version: Byte
  def senderPublicKey: Option[String]
  def author: String
  def data: List[DataEntry[_]]
  def fee: Long
}
