package com.wavesenterprise.api.http

import play.api.libs.json.JsObject

private case class TxJsonWithMeta(txJson: JsObject, typeId: Byte, version: Byte)
