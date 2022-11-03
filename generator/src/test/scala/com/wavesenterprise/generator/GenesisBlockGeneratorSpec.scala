package com.wavesenterprise.generator

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.acl.Role
import com.wavesenterprise.generator.utils.{Gen => WGen}
import com.wavesenterprise.settings.{BlockchainSettings, GenesisSettings}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.NTP
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import pureconfig.ConfigSource
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class GenesisBlockGeneratorSpec extends AnyFreeSpec with Matchers with TransactionGen with BeforeAndAfterAll {
  private val filteredRoles = WGen.roles.filterNot(role => role == Role.Permissioner || role == Role.Sender)
  private val time          = NTP(Seq("pool.ntp.org"))(monix.execution.Scheduler.global)

  def blockchainSettingsGen(
      accountsCount: Int,
      version: Byte = 1,
      timestamp: Long = GenesisSettings.minTimestamp,
      senderRoleEnabled: Boolean = false,
      addSenderRole: Boolean = false,
      addPermissionerRole: Boolean = true,
      customInitialBalance: Option[Long] = None,
      pki: Boolean = false
  ): Gen[BlockchainSettings] = {
    val senderRoles       = if (addSenderRole) List(Role.Sender) else Nil
    val permissionerRoles = if (addPermissionerRole) List(Role.Permissioner) else Nil
    val addressWithDataGen = for {
      amount     <- Gen.choose(5, 40).map(_ * 1000000000000L)
      rolesCount <- Gen.choose(1, 4)
      roles      <- Gen.listOfN(rolesCount, Gen.oneOf(filteredRoles)).map(_.distinct ++ senderRoles ++ permissionerRoles)
      address    <- accountGen
    } yield (address, amount, roles)

    for {
      addressesWithData <- Gen.listOfN(accountsCount, addressWithDataGen)
    } yield {
      val initialBalance = addressesWithData.map {
        case (_, amount, _) => amount
      }.sum

      val transactions = addressesWithData
        .map {
          case (account, amount, _) => s"""{recipient: "${account.toAddress.stringRepr}", amount: $amount}"""
        }
        .mkString("[", ",\n", "]")

      val networkParticipants = addressesWithData
        .map {
          case (account, _, roles) => s"""{public-key: "${account.publicKeyBase58}", roles: ${roles.map(_.prefixS).mkString("[", ",\n", "]")}}"""
        }
        .mkString("[", ",\n", "]")

      val pkiSection =
        s"""
           |      pki {
           |          trusted-root-fingerprints = ["4clQ5u8i+ExWRXKLkiBg19Wno+g="]
           |          certificates = [
           |            "MIIEiDCCA3CgAwIBAgIRAOAB17VV4pghCgAAAAErgYgwDQYJKoZIhvcNAQELBQAwRjELMAkGA1UEBhMCVVMxIjAgBgNVBAoTGUdvb2dsZSBUcnVzdCBTZXJ2aWNlcyBMTEMxEzARBgNVBAMTCkdUUyBDQSAxQzMwHhcNMjExMjI3MDgxMTMyWhcNMjIwMzIxMDgxMTMxWjAZMRcwFQYDVQQDEw53d3cuZ29vZ2xlLmNvbTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABNVPJxFSuX5RQ1tH9BNyYPvUzh/e8DHLyWUJ5KLgXvN2lRJb2nUB0i3AG81+498uvOIr6j5ymyUhP7dMoHwea8ejggJnMIICYzAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUmmFgAVkvDnnInXJEcmXaciWmo6swHwYDVR0jBBgwFoAUinR/r4XN7pXNPZzQ4kYU83E1HScwagYIKwYBBQUHAQEEXjBcMCcGCCsGAQUFBzABhhtodHRwOi8vb2NzcC5wa2kuZ29vZy9ndHMxYzMwMQYIKwYBBQUHMAKGJWh0dHA6Ly9wa2kuZ29vZy9yZXBvL2NlcnRzL2d0czFjMy5kZXIwGQYDVR0RBBIwEIIOd3d3Lmdvb2dsZS5jb20wIQYDVR0gBBowGDAIBgZngQwBAgEwDAYKKwYBBAHWeQIFAzA8BgNVHR8ENTAzMDGgL6AthitodHRwOi8vY3Jscy5wa2kuZ29vZy9ndHMxYzMvemRBVHQwRXhfRmsuY3JsMIIBBAYKKwYBBAHWeQIEAgSB9QSB8gDwAHUARqVV63X6kSAwtaKJafTzfREsQXS+/Um4havy/HD+bUcAAAF9+yjHMwAABAMARjBEAiAUTUAZrwL+YuBytcEzxsNykrP2ElJMyILRQhqx6WsLXQIgWhGyoFzwrnTa+H8Ta/DZwSGb3Mc/n46HIWwa3uH4nJEAdwBByMqx3yJGShDGoToJQodeTjGLGwPr60vHaPCQYpYG9gAAAX37KMcxAAAEAwBIMEYCIQCKromvTmjkyatCVBAR2fa0iMNvW/oCRMoU1aVs8S0g7wIhAPYt0skRap5a+rNjuiowULBFAAVx4DgaTup3WXVYf07iMA0GCSqGSIb3DQEBCwUAA4IBAQAmrqegFWr6RkYNAz5YOXV+kiUfwdwdBoluD3VUfsHMkX3FNN4LFYSTJ0XxwWRuvs4ZnyfRbmz5SWtKyO6gJb9LJWQvHBJC/b4n/gXvqvAwsXuGvTLRZB+pBmKxfmtAlYx2zW014BU8g2Fn8yallACYBhVmVeryJWCiqUV1nUTw75NXNcVqOMwtWiS5WrVw6EXyzqygC+ULEZwkMTaqTJ+FAv6xdyri016CvC+tNar2GLhtJoUbOYx29WaHEdlfhLxw1rs/+umDMvWjW7nCmwt9e6Oxh4/RMX6KTrCzqJEBe0AvVTJcUtTp+1bCqzk//CvPm1JwQIKScdi0z2trU/Si",
           |            "MIIFljCCA36gAwIBAgINAgO8U1lrNMcY9QFQZjANBgkqhkiG9w0BAQsFADBHMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExMQzEUMBIGA1UEAxMLR1RTIFJvb3QgUjEwHhcNMjAwODEzMDAwMDQyWhcNMjcwOTMwMDAwMDQyWjBGMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExMQzETMBEGA1UEAxMKR1RTIENBIDFDMzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAPWI3+dijB43+DdCkH9sh9D7ZYIl/ejLa6T/belaI+KZ9hzpkgOZE3wJCor6QtZeViSqejOEH9Hpabu5dOxXTGZok3c3VVP+ORBNtzS7XyV3NzsXlOo85Z3VvMO0Q+sup0fvsEQRY9i0QYXdQTBIkxu/t/bgRQIh4JZCF8/ZK2VWNAcmBA2o/X3KLu/qSHw3TT8An4Pf73WELnlXXPxXbhqW//yMmqaZviXZf5YsBvcRKgKAgOtjGDxQSYflispfGStZloEAoPtR28p3CwvJlk/vcEnHXG0g/Zm0tOLKLnf9LdwLtmsTDIwZKxeWmLnwi/agJ7u2441Rj72ux5uxiZ0CAwEAAaOCAYAwggF8MA4GA1UdDwEB/wQEAwIBhjAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwEgYDVR0TAQH/BAgwBgEB/wIBADAdBgNVHQ4EFgQUinR/r4XN7pXNPZzQ4kYU83E1HScwHwYDVR0jBBgwFoAU5K8rJnEaK0gnhS9SZizv8IkTcT4waAYIKwYBBQUHAQEEXDBaMCYGCCsGAQUFBzABhhpodHRwOi8vb2NzcC5wa2kuZ29vZy9ndHNyMTAwBggrBgEFBQcwAoYkaHR0cDovL3BraS5nb29nL3JlcG8vY2VydHMvZ3RzcjEuZGVyMDQGA1UdHwQtMCswKaAnoCWGI2h0dHA6Ly9jcmwucGtpLmdvb2cvZ3RzcjEvZ3RzcjEuY3JsMFcGA1UdIARQME4wOAYKKwYBBAHWeQIFAzAqMCgGCCsGAQUFBwIBFhxodHRwczovL3BraS5nb29nL3JlcG9zaXRvcnkvMAgGBmeBDAECATAIBgZngQwBAgIwDQYJKoZIhvcNAQELBQADggIBAIl9rCBcDDy+mqhXlRu0rvqrpXJxtDaV/d9AEQNMwkYUuxQkq/BQcSLbrcRuf8/xam/IgxvYzolfh2yHuKkMo5uhYpSTld9brmYZCwKWnvy15xBpPnrLRklfRuFBsdeYTWU0AIAaP0+fbH9JAIFTQaSSIYKCGvGjRFsqUBITTcFTNvNCCK9U+o53UxtkOCcXCb1YyRt8OS1b887U7ZfbFAO/CVMkH8IMBHmYJvJh8VNS/UKMG2YrPxWhu//2m+OBmgEGcYk1KCTd4b3rGS3hSMs9WYNRtHTGnXzGsYZbr8w0xNPM1IERlQCh9BIiAfq0g3GvjLeMcySsN1PCAJA/Ef5c7TaUEDu9Ka7ixzpiO2xj2YC/WXGsYye5TBeg2vZzFb8q3o/zpWwygTMD0IZRcZk0upONXbVRWPeyk+gB9lm+cZv9TSjOz23HFtz30dZGm6fKa+l3D/2gthsjgx0QGtkJAITgRNOidSOzNIb2ILCkXhAd4FJGAJ2xDx8hcFH1mt0G/FX0Kw4zd8NLQsLxdxP8c4CU6x+7Nz/OAipmsHMdMqUybDKwjuDEI/9bfU1lcKwrmz3O2+BtjjKAvpafkmO8l7tdufThcV4q5O8DIrGKZTqPwJNl1IXNDw9bg1kWRxYtnCQ6yICmJhSFm/Y3m6xv+cXDBlHz4n/FsRC6UfTd"
           |          ]
           |          crls = []
           |      }
           |""".stripMargin

      ConfigSource
        .string {
          s"""node.blockchain {
               |  consensus.type = PoS
               |  type = CUSTOM
               |  owner-address = 3N3XLqz3DQSeUbFeTQh6m6W6Eg1FBdTVPt8
               |  custom {
               |    address-scheme-character = T
               |    functionality {
               |      feature-check-blocks-period = 100
               |      blocks-for-feature-activation = 90
               |      pre-activated-features = {
               |        2 = 0
               |        3 = 0
               |        4 = 0
               |        5 = 0
               |        6 = 0
               |        7 = 0
               |      }
               |    }
               |    genesis {
               |      version: $version
               |      sender-role-enabled: $senderRoleEnabled
               |      average-block-delay: 10000ms
               |      initial-base-target: 200000
               |      block-timestamp: $timestamp
               |      initial-balance: ${customInitialBalance.getOrElse(initialBalance)}
               |      genesis-public-key-base-58: "HQLZLbtQsEPyHLSjyptHuhqhiGoPz3nVYz2j891PRym9ZuiGC6hwKHRktC9Ws8fLic7pvPpZ2DzWZ7UvXMyeKM5"
               |      signature: "5WepTySe3ab5k6WprLJUaFdDZLu71ecLirGN7WnfunDeixeZ8TBJcoYtnPtaksEBPQpRByZrNkKSp9xZrouPhc9K"
               |      transactions = $transactions
               |      network-participants = $networkParticipants
               |      ${if (pki) pkiSection else ""}
               |    }
               |  }
               |  fees {
               |    base {
               |      issue = 100000000
               |      transfer = 100000
               |      reissue = 100000000
               |      burn = 500000
               |      exchange = 500000
               |      lease = 500000
               |      lease-cancel = 500000
               |      create-alias = 0
               |      mass-transfer = 500000
               |      data = 500000
               |      set-script = 15000000
               |      sponsor-fee = 100000000
               |      set-asset-script = 100000000
               |      permit = 0
               |      disable-contract = 15000000
               |      call-contract = 15000000
               |      create-contract = 100000000
               |      update-contract = 0
               |      register-node = 500000
               |      create-policy = 100000000
               |      update-policy = 100000000
               |      policy-data-hash = 15000000
               |    }
               |
               |    additional {
               |      mass-transfer = 100000
               |      data = 100000
               |    }
               |  }
               |}""".stripMargin
        }
        .at("node.blockchain")
        .loadOrThrow[BlockchainSettings]
    }
  }

  "Genesis V1" - {
    "More than 127 txs should fail" in {
      val blockchainSettings = blockchainSettingsGen(128).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      val genesisSettings = blockchainSettings.custom.genesis.toPlainSettingsUnsafe
      val expectedTxsCount = genesisSettings.transactions.size +
        genesisSettings.networkParticipants.size +
        genesisSettings.networkParticipants.flatMap(_.roles).size

      genesisInfo shouldBe Left(ValidationError.GenericError(s"Too much transactions for genesis block: found '$expectedTxsCount', max count '127'"))
    }

    "sender-role-enabled=false should pass" in {
      val blockchainSettings = blockchainSettingsGen(3).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      genesisInfo shouldBe 'right
    }

    "sender-role-enabled=true should fail" in {
      the[IllegalArgumentException] thrownBy blockchainSettingsGen(3, senderRoleEnabled = true).sample.get should have message "requirement failed: Sender role is only supported in genesis version 2"
    }

    "Not a single Permissioner role should fail" in {
      val blockchainSettings = blockchainSettingsGen(64, addPermissionerRole = false).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      genesisInfo shouldBe Left(ValidationError.GenericError("[At least one network participant must have the 'permissioner' role]"))
    }

    "Wrong initial balance should fail" in {
      val blockchainSettings = blockchainSettingsGen(10, customInitialBalance = Some(1000000L)).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      genesisInfo shouldBe Left(ValidationError.GenericError("[The value of 'initial-balance' has to be equal to the sum of transactions amounts]"))
    }
  }

  "Genesis V2" - {
    "More than 127 txs should pass" in {
      val blockchainSettings = blockchainSettingsGen(128, version = 2).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      genesisInfo shouldBe 'right
    }

    "sender-role-enabled=true without a Sender role should fail" in {
      val blockchainSettings = blockchainSettingsGen(128, version = 2, senderRoleEnabled = true).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      genesisInfo shouldBe Left(
        ValidationError.GenericError("[Role 'sender' is required for every network participant which has any additional role]"))
    }

    "sender-role-enabled=true with at least one Sender role should pass" in {
      val blockchainSettings = blockchainSettingsGen(128, version = 2, senderRoleEnabled = true, addSenderRole = true).sample.get

      val genesisInfo = GenesisBlockGenerator.getGenesisInfo(
        blockchainSettings,
        currentTime = time.getTimestamp()
      )

      genesisInfo shouldBe 'right
    }
  }

  "Genesis with PKI" - {
    "sender-role-enabled=false should pass" in {
      val blockchainSettings = blockchainSettingsGen(3, pki = true).sample.get
      val pkiSettings        = blockchainSettings.custom.genesis.pki.get

      pkiSettings.trustedRootFingerprints.last shouldBe "4clQ5u8i+ExWRXKLkiBg19Wno+g="
      pkiSettings.certificates should not be empty
    }
  }
}
