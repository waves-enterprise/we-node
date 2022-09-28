Generators
-------------------

[Set of utilities](https://docs.wavesenterprise.com/en/latest/description/generators.html#generators-description) for working with blockchain. 


## How to build artifacts:

##### Building console app:
`sbt generator/assembly`

## Console app usage:


### 1. [AccountsGeneratorApp](https://docs.wavesenterprise.com/en/latest/description/generators.html#accountsgeneratorapp)
##### Generate account:
`java -cp "generators-<version>.jar" com.wavesenterprise.generator.AccountsGeneratorApp accounts.conf`

##### Config example:
```hocon
accounts-generator {
  crypto.type = WAVES
  chain-id = V
  amount = 1
  wallet = "~/node/keystore.dat"
  wallet-password = "/FILL/"
  reload-node-wallet {
    enabled = false
    url = "http://localhost:6862/utils/reload-wallet"
  }
}
```

### 2. [ApiKeyHashGenerator](https://docs.wavesenterprise.com/en/latest/description/generators.html#apikeyhash)

##### Generate API key hash:
`java -cp "generators-<version>.jar" com.wavesenterprise.generator.ApiKeyHashGenerator api-key-hash.conf`

##### API key file example:
```hocon
apikeyhash-generator {
  crypto.type = WAVES
  api-key = "some string for api-key"
}
```

### 3. [GenesisBlockGenerator](https://docs.wavesenterprise.com/en/latest/description/generators.html#genesisblockgenerator)
##### Generate genesis block:
`java -cp "corporate-generators-<version>.jar" com.wavesenterprise.generator.GenesisBlockGenerator node.conf`
