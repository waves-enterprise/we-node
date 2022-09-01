package com.wavesenterprise.api.http

import play.api.libs.json.{JsPath, JsonValidationError}

private case class ParseErrors(errors: Seq[(JsPath, Seq[JsonValidationError])])
