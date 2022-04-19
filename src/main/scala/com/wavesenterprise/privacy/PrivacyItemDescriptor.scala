package com.wavesenterprise.privacy

case class PrivacyItemDescriptor(dataType: PrivacyDataType) {
  def bytes: Array[Byte] = Array(dataType.value)
}

object PrivacyItemDescriptor {

  def fromBytesUnsafe(bytes: Array[Byte]): PrivacyItemDescriptor = {
    val dataType = PrivacyDataType.withValue {
      bytes.headOption.getOrElse(throw new IllegalArgumentException(s"Failed to parse PrivacyItemDescriptor from empty array"))
    }

    PrivacyItemDescriptor(dataType)
  }
}
