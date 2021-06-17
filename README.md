# Function Stream

Function stream is a distributed event-driven application development platform based on Apache Pulsar. The goal is to facilitate the development of streaming applications for users. Funcion Stream providing more convenient for the development of distributed event-driven applications based on the existing features provided by the pulsar function.

## Install

### Install Apache Pulsar

* Download the modified version pulsar server package from [here](https://functionstream.oss-cn-hongkong.aliyuncs.com/apache-pulsar-2.8.0-SNAPSHOT.zip?versionId=CAEQHxiBgMCAq4vwzhciIGQxMGMxMGI4NjQ0ODRmMzE4NGI0YWE3MjlhNmYyNTc4). 
* You can skip this step if just want to start the pulsar proxy with default config. Configure `conf/proxy.conf` as below:
```
# The ZooKeeper quorum connection string (as a comma-separated list)
zookeeperServers=localhost

# Configuration store connection string (as a comma-separated list)
configurationStoreServers=localhost

# if Service Discovery is Disabled this url should point to the discovery service provider.
brokerServiceURL=localhost

# The port to use for server binary Protobuf requests
servicePort=3001

# Port that discovery service listen on
webServicePort=3000
```
* Start pulsar standalone with the command: `bin/pulsar standalone -nss`
* Start pulsar proxy with the command: `bin/pulsar proxy`

### Install Front-end

* Install dependencies

```sh
yarn install
```

* Compiles and hot-reloads for development

```sh
yarn run serve
```

* Compiles and minifies for production

```sh
yarn run build
```

## Access Function Stream

* Access Pulsar manager UI at http://${frontend-url}/
