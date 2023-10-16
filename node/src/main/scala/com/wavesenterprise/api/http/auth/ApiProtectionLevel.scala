package com.wavesenterprise.api.http.auth

sealed trait ApiProtectionLevel

object ApiProtectionLevel {
  case object WithoutProtection                     extends ApiProtectionLevel
  case object ApiKeyProtection                      extends ApiProtectionLevel
  case object PrivacyApiKeyProtection               extends ApiProtectionLevel
  case object ConfidentialContractsApiKeyProtection extends ApiProtectionLevel
}
