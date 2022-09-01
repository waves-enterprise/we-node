package com.wavesenterprise.database

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.crypto.PublicKey
import com.wavesenterprise.utils.{Base58, Base64}
import org.apache.commons.codec.digest.DigestUtils

import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509Certificate}

trait WithTestCert {
  // echo -n | openssl s_client -connect google.com:443 -servername google.com | openssl x509
  protected val testCertStr: String =
    """MIINsTCCDJmgAwIBAgIQRVLl9A6X+OIKAAAAAS+P2zANBgkqhkiG9w0BAQsFADBG
      |MQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExM
      |QzETMBEGA1UEAxMKR1RTIENBIDFDMzAeFw0yMjAxMTcwMjIxMDRaFw0yMjA0MTEw
      |MjIxMDNaMBcxFTATBgNVBAMMDCouZ29vZ2xlLmNvbTBZMBMGByqGSM49AgEGCCqG
      |SM49AwEHA0IABLUiFxtQHhutJbou0ckFMvCH/v5ily9fKcpUv0j7f65iI02ScP9v
      |gHUfPanKBr/sXXxYLTyH8LEPRn1631y8fkSjgguTMIILjzAOBgNVHQ8BAf8EBAMC
      |B4AwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
      |y/uwlapgrcUE9uaLt6L4MjYpaPQwHwYDVR0jBBgwFoAUinR/r4XN7pXNPZzQ4kYU
      |83E1HScwagYIKwYBBQUHAQEEXjBcMCcGCCsGAQUFBzABhhtodHRwOi8vb2NzcC5w
      |a2kuZ29vZy9ndHMxYzMwMQYIKwYBBQUHMAKGJWh0dHA6Ly9wa2kuZ29vZy9yZXBv
      |L2NlcnRzL2d0czFjMy5kZXIwgglCBgNVHREEggk5MIIJNYIMKi5nb29nbGUuY29t
      |ghYqLmFwcGVuZ2luZS5nb29nbGUuY29tggkqLmJkbi5kZXaCEiouY2xvdWQuZ29v
      |Z2xlLmNvbYIYKi5jcm93ZHNvdXJjZS5nb29nbGUuY29tghgqLmRhdGFjb21wdXRl
      |Lmdvb2dsZS5jb22CCyouZ29vZ2xlLmNhggsqLmdvb2dsZS5jbIIOKi5nb29nbGUu
      |Y28uaW6CDiouZ29vZ2xlLmNvLmpwgg4qLmdvb2dsZS5jby51a4IPKi5nb29nbGUu
      |Y29tLmFygg8qLmdvb2dsZS5jb20uYXWCDyouZ29vZ2xlLmNvbS5icoIPKi5nb29n
      |bGUuY29tLmNvgg8qLmdvb2dsZS5jb20ubXiCDyouZ29vZ2xlLmNvbS50coIPKi5n
      |b29nbGUuY29tLnZuggsqLmdvb2dsZS5kZYILKi5nb29nbGUuZXOCCyouZ29vZ2xl
      |LmZyggsqLmdvb2dsZS5odYILKi5nb29nbGUuaXSCCyouZ29vZ2xlLm5sggsqLmdv
      |b2dsZS5wbIILKi5nb29nbGUucHSCEiouZ29vZ2xlYWRhcGlzLmNvbYIPKi5nb29n
      |bGVhcGlzLmNughEqLmdvb2dsZXZpZGVvLmNvbYIMKi5nc3RhdGljLmNughAqLmdz
      |dGF0aWMtY24uY29tgg9nb29nbGVjbmFwcHMuY26CESouZ29vZ2xlY25hcHBzLmNu
      |ghFnb29nbGVhcHBzLWNuLmNvbYITKi5nb29nbGVhcHBzLWNuLmNvbYIMZ2tlY25h
      |cHBzLmNugg4qLmdrZWNuYXBwcy5jboISZ29vZ2xlZG93bmxvYWRzLmNughQqLmdv
      |b2dsZWRvd25sb2Fkcy5jboIQcmVjYXB0Y2hhLm5ldC5jboISKi5yZWNhcHRjaGEu
      |bmV0LmNuggt3aWRldmluZS5jboINKi53aWRldmluZS5jboIRYW1wcHJvamVjdC5v
      |cmcuY26CEyouYW1wcHJvamVjdC5vcmcuY26CEWFtcHByb2plY3QubmV0LmNughMq
      |LmFtcHByb2plY3QubmV0LmNughdnb29nbGUtYW5hbHl0aWNzLWNuLmNvbYIZKi5n
      |b29nbGUtYW5hbHl0aWNzLWNuLmNvbYIXZ29vZ2xlYWRzZXJ2aWNlcy1jbi5jb22C
      |GSouZ29vZ2xlYWRzZXJ2aWNlcy1jbi5jb22CEWdvb2dsZXZhZHMtY24uY29tghMq
      |Lmdvb2dsZXZhZHMtY24uY29tghFnb29nbGVhcGlzLWNuLmNvbYITKi5nb29nbGVh
      |cGlzLWNuLmNvbYIVZ29vZ2xlb3B0aW1pemUtY24uY29tghcqLmdvb2dsZW9wdGlt
      |aXplLWNuLmNvbYISZG91YmxlY2xpY2stY24ubmV0ghQqLmRvdWJsZWNsaWNrLWNu
      |Lm5ldIIYKi5mbHMuZG91YmxlY2xpY2stY24ubmV0ghYqLmcuZG91YmxlY2xpY2st
      |Y24ubmV0gg5kb3VibGVjbGljay5jboIQKi5kb3VibGVjbGljay5jboIUKi5mbHMu
      |ZG91YmxlY2xpY2suY26CEiouZy5kb3VibGVjbGljay5jboIRZGFydHNlYXJjaC1j
      |bi5uZXSCEyouZGFydHNlYXJjaC1jbi5uZXSCHWdvb2dsZXRyYXZlbGFkc2Vydmlj
      |ZXMtY24uY29tgh8qLmdvb2dsZXRyYXZlbGFkc2VydmljZXMtY24uY29tghhnb29n
      |bGV0YWdzZXJ2aWNlcy1jbi5jb22CGiouZ29vZ2xldGFnc2VydmljZXMtY24uY29t
      |ghdnb29nbGV0YWdtYW5hZ2VyLWNuLmNvbYIZKi5nb29nbGV0YWdtYW5hZ2VyLWNu
      |LmNvbYIYZ29vZ2xlc3luZGljYXRpb24tY24uY29tghoqLmdvb2dsZXN5bmRpY2F0
      |aW9uLWNuLmNvbYIkKi5zYWZlZnJhbWUuZ29vZ2xlc3luZGljYXRpb24tY24uY29t
      |ghZhcHAtbWVhc3VyZW1lbnQtY24uY29tghgqLmFwcC1tZWFzdXJlbWVudC1jbi5j
      |b22CC2d2dDEtY24uY29tgg0qLmd2dDEtY24uY29tggtndnQyLWNuLmNvbYINKi5n
      |dnQyLWNuLmNvbYILMm1kbi1jbi5uZXSCDSouMm1kbi1jbi5uZXSCFGdvb2dsZWZs
      |aWdodHMtY24ubmV0ghYqLmdvb2dsZWZsaWdodHMtY24ubmV0ggxhZG1vYi1jbi5j
      |b22CDiouYWRtb2ItY24uY29tgg0qLmdzdGF0aWMuY29tghQqLm1ldHJpYy5nc3Rh
      |dGljLmNvbYIKKi5ndnQxLmNvbYIRKi5nY3BjZG4uZ3Z0MS5jb22CCiouZ3Z0Mi5j
      |b22CDiouZ2NwLmd2dDIuY29tghAqLnVybC5nb29nbGUuY29tghYqLnlvdXR1YmUt
      |bm9jb29raWUuY29tggsqLnl0aW1nLmNvbYILYW5kcm9pZC5jb22CDSouYW5kcm9p
      |ZC5jb22CEyouZmxhc2guYW5kcm9pZC5jb22CBGcuY26CBiouZy5jboIEZy5jb4IG
      |Ki5nLmNvggZnb28uZ2yCCnd3dy5nb28uZ2yCFGdvb2dsZS1hbmFseXRpY3MuY29t
      |ghYqLmdvb2dsZS1hbmFseXRpY3MuY29tggpnb29nbGUuY29tghJnb29nbGVjb21t
      |ZXJjZS5jb22CFCouZ29vZ2xlY29tbWVyY2UuY29tgghnZ3BodC5jboIKKi5nZ3Bo
      |dC5jboIKdXJjaGluLmNvbYIMKi51cmNoaW4uY29tggh5b3V0dS5iZYILeW91dHVi
      |ZS5jb22CDSoueW91dHViZS5jb22CFHlvdXR1YmVlZHVjYXRpb24uY29tghYqLnlv
      |dXR1YmVlZHVjYXRpb24uY29tgg95b3V0dWJla2lkcy5jb22CESoueW91dHViZWtp
      |ZHMuY29tggV5dC5iZYIHKi55dC5iZYIaYW5kcm9pZC5jbGllbnRzLmdvb2dsZS5j
      |b22CG2RldmVsb3Blci5hbmRyb2lkLmdvb2dsZS5jboIcZGV2ZWxvcGVycy5hbmRy
      |b2lkLmdvb2dsZS5jboIYc291cmNlLmFuZHJvaWQuZ29vZ2xlLmNuMCEGA1UdIAQa
      |MBgwCAYGZ4EMAQIBMAwGCisGAQQB1nkCBQMwPAYDVR0fBDUwMzAxoC+gLYYraHR0
      |cDovL2NybHMucGtpLmdvb2cvZ3RzMWMzL1FxRnhiaTlNNDhjLmNybDCCAQUGCisG
      |AQQB1nkCBAIEgfYEgfMA8QB2ACl5vvCeOTkh8FZzn2Old+W+V32cYAr4+U1dJlwl
      |XceEAAABfmYNh6oAAAQDAEcwRQIhAPpURnP+UJ3tJYKfP9hfLpvcaMG4JZXZksGa
      |P1XFF6KUAiAN2AouDWPqB8zApzQVCE2PrwBjbTAM1iQ8vVJ+hEuKogB3AEHIyrHf
      |IkZKEMahOglCh15OMYsbA+vrS8do8JBilgb2AAABfmYNh9kAAAQDAEgwRgIhAKi/
      |apNG/qlWEPldvLnh2rERRDt2bkomAYgIQhU5Ev+BAiEA7QkrSzB2L6rWWVZvSgr1
      |rU9J97tsAs9YZQc8qgu0VYQwDQYJKoZIhvcNAQELBQADggEBAClOMhtw4RAylEuu
      |lCfIdMpR1IhFl+lUtNPhkDagKhQx6DkxGzjJ8GkU//7lKTFv9FfHOoLfkNRlhnB5
      |1vnFBeSiapS2RKk0aJtqzOIzZo22xutpixe3yVYxNvS6e5KVlRB1RxUr1s20ocVe
      |fPSPh1a+wen/V9Z2c2Mh4zziZAtltDCxncJFjjWNmKrL028IIugCeJ90kMYQc/Ym
      |OHBxcxHkcYUROoT50MrhtyjgIwieNGfkzQfq7mmsdtR9PqsLWiRQF1zUwikxwG3W
      |bT1qcyehImUJHlFZyhDnEtg8Vub5jzWK2ACNAxg7I3sVRZT55qpEVXfrWbqSyfLP
      |jSuv9c4=""".stripMargin.replace("\n", "")

  // echo -n | openssl s_client -connect wikipedia.org:443 -servername wikipedia.org | openssl x509
  protected val anotherTestCertStr =
    """MIIIIjCCB6egAwIBAgIQAn2UGyks2y7a+ZMRGFN0PjAKBggqhkjOPQQDAzBWMQsw
      |CQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMTAwLgYDVQQDEydEaWdp
      |Q2VydCBUTFMgSHlicmlkIEVDQyBTSEEzODQgMjAyMCBDQTEwHhcNMjExMDE5MDAw
      |MDAwWhcNMjIxMTE3MjM1OTU5WjB5MQswCQYDVQQGEwJVUzETMBEGA1UECBMKQ2Fs
      |aWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEjMCEGA1UEChMaV2lraW1l
      |ZGlhIEZvdW5kYXRpb24sIEluYy4xGDAWBgNVBAMMDyoud2lraXBlZGlhLm9yZzBZ
      |MBMGByqGSM49AgEGCCqGSM49AwEHA0IABOhQLNDSTqKxkqq2cw/PoLRX5cLAfK5u
      |VZFKppRn+qX4sD9GrCNStEg7ZGT76s3p5PuPEKf06CO6lSlu78pyu4OjggYyMIIG
      |LjAfBgNVHSMEGDAWgBQKvAgpF4ylOW16Ds4zxy6z7fvDejAdBgNVHQ4EFgQUiSWO
      |8YX5f1zRhxSYSl70HDIdUUQwggLFBgNVHREEggK8MIICuIIPKi53aWtpcGVkaWEu
      |b3Jngg13aWtpbWVkaWEub3Jngg1tZWRpYXdpa2kub3Jngg13aWtpYm9va3Mub3Jn
      |ggx3aWtpZGF0YS5vcmeCDHdpa2luZXdzLm9yZ4INd2lraXF1b3RlLm9yZ4IOd2lr
      |aXNvdXJjZS5vcmeCD3dpa2l2ZXJzaXR5Lm9yZ4IOd2lraXZveWFnZS5vcmeCDndp
      |a3Rpb25hcnkub3Jnghd3aWtpbWVkaWFmb3VuZGF0aW9uLm9yZ4IGdy53aWtpghJ3
      |bWZ1c2VyY29udGVudC5vcmeCESoubS53aWtpcGVkaWEub3Jngg8qLndpa2ltZWRp
      |YS5vcmeCESoubS53aWtpbWVkaWEub3JnghYqLnBsYW5ldC53aWtpbWVkaWEub3Jn
      |gg8qLm1lZGlhd2lraS5vcmeCESoubS5tZWRpYXdpa2kub3Jngg8qLndpa2lib29r
      |cy5vcmeCESoubS53aWtpYm9va3Mub3Jngg4qLndpa2lkYXRhLm9yZ4IQKi5tLndp
      |a2lkYXRhLm9yZ4IOKi53aWtpbmV3cy5vcmeCECoubS53aWtpbmV3cy5vcmeCDyou
      |d2lraXF1b3RlLm9yZ4IRKi5tLndpa2lxdW90ZS5vcmeCECoud2lraXNvdXJjZS5v
      |cmeCEioubS53aWtpc291cmNlLm9yZ4IRKi53aWtpdmVyc2l0eS5vcmeCEyoubS53
      |aWtpdmVyc2l0eS5vcmeCECoud2lraXZveWFnZS5vcmeCEioubS53aWtpdm95YWdl
      |Lm9yZ4IQKi53aWt0aW9uYXJ5Lm9yZ4ISKi5tLndpa3Rpb25hcnkub3JnghkqLndp
      |a2ltZWRpYWZvdW5kYXRpb24ub3JnghQqLndtZnVzZXJjb250ZW50Lm9yZ4INd2lr
      |aXBlZGlhLm9yZzAOBgNVHQ8BAf8EBAMCB4AwHQYDVR0lBBYwFAYIKwYBBQUHAwEG
      |CCsGAQUFBwMCMIGbBgNVHR8EgZMwgZAwRqBEoEKGQGh0dHA6Ly9jcmwzLmRpZ2lj
      |ZXJ0LmNvbS9EaWdpQ2VydFRMU0h5YnJpZEVDQ1NIQTM4NDIwMjBDQTEtMS5jcmww
      |RqBEoEKGQGh0dHA6Ly9jcmw0LmRpZ2ljZXJ0LmNvbS9EaWdpQ2VydFRMU0h5YnJp
      |ZEVDQ1NIQTM4NDIwMjBDQTEtMS5jcmwwPgYDVR0gBDcwNTAzBgZngQwBAgIwKTAn
      |BggrBgEFBQcCARYbaHR0cDovL3d3dy5kaWdpY2VydC5jb20vQ1BTMIGFBggrBgEF
      |BQcBAQR5MHcwJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmRpZ2ljZXJ0LmNvbTBP
      |BggrBgEFBQcwAoZDaHR0cDovL2NhY2VydHMuZGlnaWNlcnQuY29tL0RpZ2lDZXJ0
      |VExTSHlicmlkRUNDU0hBMzg0MjAyMENBMS0xLmNydDAMBgNVHRMBAf8EAjAAMIIB
      |fgYKKwYBBAHWeQIEAgSCAW4EggFqAWgAdgApeb7wnjk5IfBWc59jpXflvld9nGAK
      |+PlNXSZcJV3HhAAAAXyZXkr+AAAEAwBHMEUCIBMrn9S/XWaDxzU6Dotz5+rqQdIP
      |f+Al24AihByk56H+AiEAg1Wt1B0DIyxOGm7m/XOPGq+QTv4eUARkxE/aQrkhwQIA
      |dgBRo7D1/QF5nFZtuDd4jwykeswbJ8v3nohCmg3+1IsF5QAAAXyZXkr2AAAEAwBH
      |MEUCIQDs7bEtpj68Xa4PSNVwW6TpYzImE4bG1BZmaJxkZ2AuzAIgFp1eFXmg65XQ
      |qlARQOgoNJDY6EECtaiw+0nuKNoWMf8AdgBByMqx3yJGShDGoToJQodeTjGLGwPr
      |60vHaPCQYpYG9gAAAXyZXkqEAAAEAwBHMEUCIQD+E/o+Cy8crHEG0VJkg7O5JBnW
      |o+UioA8TUARs4MdtjgIgBOK8Av5czpI1cerOG8awiiXjVXl/Mf2NKeSUD3bqHy4w
      |CgYIKoZIzj0EAwMDaQAwZgIxAM2q6BYYC17eJLxEdvOj5R6lAxlSfv5XLAv+4q+0
      |Zz5qggg2zgFgMItbo05QJx/eAgIxAI77ICh+tc/fH5CZCYOwd3Bv4JQ/nVl9ymwh
      |aS5p0swP6axTxJPCnNaDluVzOXJ9nA==""".stripMargin.replace("\n", "")

  protected val testCertBytes = Base64.decode(testCertStr).getOrElse[Array[Byte]](throw new RuntimeException("Invalid certificate string"))
  protected val anotherTestCertBytes =
    Base64.decode(anotherTestCertStr).getOrElse[Array[Byte]](throw new RuntimeException("Invalid certificate string"))

  protected val testCert = CertificateFactory
    .getInstance("X.509")
    .generateCertificate(new ByteArrayInputStream(testCertBytes))
    .asInstanceOf[X509Certificate]
  protected val testPka = PublicKeyAccount(PublicKey(testCert.getPublicKey.getEncoded))
  protected val anotherTestCert = CertificateFactory
    .getInstance("X.509")
    .generateCertificate(new ByteArrayInputStream(anotherTestCertBytes))
    .asInstanceOf[X509Certificate]
  protected val anotherTestPka                 = PublicKeyAccount(PublicKey(anotherTestCert.getPublicKey.getEncoded))
  protected val testCertDn                     = testCert.getSubjectX500Principal.getName
  protected val testCertDnHash                 = DigestUtils.sha1Hex(testCertDn)
  protected val testCertPublicKeyBase58        = Base58.encode(testCert.getPublicKey.getEncoded)
  protected val testCertFingerprint            = Base64.encode(DigestUtils.sha1(testCert.getEncoded))
  protected val anotherTestCertDn              = anotherTestCert.getSubjectX500Principal.getName
  protected val anotherTestCertDnHash          = DigestUtils.sha1Hex(anotherTestCertDn)
  protected val anotherTestCertPublicKeyBase58 = Base58.encode(anotherTestCert.getPublicKey.getEncoded)
  protected val anotherTestCertFingerprint     = Base64.encode(DigestUtils.sha1(anotherTestCert.getEncoded))
}
