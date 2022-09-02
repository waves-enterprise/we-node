# Waves Enterprise node

[![Telegram](https://badgen.net/badge/icon/Waves%20Enterprise%20Group?icon=telegram&label=Telegram)](https://t.me/wavesenterprisegroup)

---

Waves Enterprise blockchain platform is a ready-to-use distributed ledger system which allows to build public and private blockchain networks for usage in corporate and public sectors.

See the [documentation](https://docs.wavesenterprise.com)

## Requirements
* Java 11
* [sbt](https://www.scala-sbt.org)
* [Docker engine](https://docs.docker.com/engine/install)
* [Public Documentation](https://docs.wavesenterprise.com)

## Building

### Create fat jar
```
sbt "node/assembly"
```

Our jar file should now be built and available at `./node/target/node-*.jar`

### Build docker image
```
docker build --tag wavesenterprise/node:v1.11.0 .
```

## Usage

After building the image, you can start the network using [docker compose](https://github.com/waves-enterprise/WE-releases/releases/download/v1.8.4/docker-compose.yml).

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
