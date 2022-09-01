Transactions signer
-----------------------------

Command-line app for signing transactions with waves cryptography

## How to build artifacts:

##### Building console app:
`sbt transactionsSigner/assembly`

## Console app usage:

##### Signing transaction with waves cryptography:
`java -cp "corporate-transactions-signer-<version>.jar" \
com.wavesenterprise.TxSignerApplication -i transactions.txt -c waves -o ./output -k .../keystore.0000/ -b V -p "password"`

##### Transactions file example:
```json
[{
  "sender" : "3P1H3KKqyGqXzSuPMw88WYB1g3hfAxFoFLt",
  "fee" : 10000000,
  "contractId" : "SSvwcbfTphMxmwmJbWk2Y8QtHspdWs74nMo6ypMDacC",
  "type" : 104,
  "params" : [{"key":"b","type":"integer","value":10},{"key":"a","type":"integer","value":15}],
  "version" : 1
}]
```
