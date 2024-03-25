# Waves Enterprise node

[![Telegram](https://badgen.net/badge/icon/Waves%20Enterprise%20Group?icon=telegram&label=Telegram)](https://t.me/wavesenterprisegroup)
[![docker pulls](https://badgen.net/docker/pulls/wavesenterprise/node)](https://hub.docker.com/repository/docker/wavesenterprise/node)

---

Waves Enterprise blockchain platform is a ready-to-use distributed ledger system which allows to build public and private blockchain networks for usage in corporate and public sectors.

See the [documentation](https://docs.wavesenterprise.com).

## Requirements
* Java 11
* [sbt](https://www.scala-sbt.org)
* [Docker engine](https://docs.docker.com/engine/install)

## Building

### Create fat jar
```
sbt "node/assembly"
```

Our jar file should now be built and available at `./node/target/node-*.jar`

### Build docker image
```
docker build --tag wavesenterprise/node:v1.14.0 .
```

## Usage

After building the image, you can start the network using [docker compose](node/src/docker/docker-compose.yml).

### Configuration

You can use the following configuration templates as a basis:
* [Accounts generator](configs/accounts-example.conf)
* [API key hash generator](configs/api-key-hash-example.conf)
* [Mainnet node](configs/mainnet.conf)
* [Custom network node](configs/node-example.conf)

### Environment variables

#### Core variables
```
# Set Node owner's key password
#   Default: false
WE_NODE_OWNER_PASSWORD_EMPTY=True/False
WE_NODE_OWNER_PASSWORD="123456"

# Base logging level
#   Possible levels: TRACE, DEBUG, INFO, WARN, ERROR and OFF
#   Default: DEBUG
LOG_LEVEL="DEBUG"

# Clean Node's state on startup
# Removes data-directory found in node's config file. By default it's '/node/data'
#   Default: false
CLEAN_STATE=True/False

# Hostname will be used as a config file name. /opt/configs/hostname.conf
#   Default: false
CONFIGNAME_AS_HOSTNAME=True/False
```

#### Java environment variables
```
# JVM options
#   Default: "-XX:+AlwaysPreTouch -XX:+PerfDisableSharedMem"
# Optional
JAVA_OPTS="-Xms750m -Xmx750m"

# Path to JAR file with Node
#   Default: "we.jar"
JAR_FILE="we.jar"

# Additional JAR arguments
# Optional
JAR_ARGS="arg"

# Specifies type of memory allocation manager
#   Possible values: DEFAULT, JEMALLOC
#   Default: DEFAULT
MALLOC_TYPE="JEMALLOC"
```

#### In case of using [Vault](https://www.vaultproject.io)
```
# Namespace path
NAMESPACE="we"

# URL to Vault
VAULT_URL="https://..."

# Base path
VAULT_PATH="wavesnodes"

# Role ID
VAULT_ROLE_ID="roleid"

# Role's secret ID
VAULT_SECRET_ID="123456"

# Use common fallback config from common/ directory in vault
#   Default: false
WITH_COMMON_CONFIG=True/False
```

#### Minimal supported peers' version: 1.8.4 (Can be found in `HandshakeHandler.minSupportedVersion`)

---
## Acknowledgments
[![YourKit logo](https://www.yourkit.com/images/yklogo.png)](https://www.yourkit.com/)

YourKit supports open source projects with innovative and intelligent tools
for monitoring and profiling Java and .NET applications.
YourKit is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a>,
<a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>,
and <a href="https://www.yourkit.com/youmonitor/">YourKit YouMonitor</a>.
