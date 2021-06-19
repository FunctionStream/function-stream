# Function Stream

Function stream is a distributed event-driven application development platform based on Apache Pulsar. The goal is to facilitate the development of streaming applications for users. Function Stream providing more convenience for the development of distributed event-driven applications based on the existing features provided by the pulsar function.

Users can manage Pulsar Functions through this platform, including creating, editing, and deleting functions, and also provides management of function image files. Function Stream will make many extensions on the basis of the Pulsar Function to make it more convenient for users to develop distributed event-streaming application. In future plans, the online code editing feature and workflow editing feature will also be added.

This project is still in its early stages, if you are interested in this project and want to contribute to it or suggest ideas, feel free to join us.

You can join [our slack workspace](https://join.slack.com/t/functionstreamgroup/shared_invite/zt-rkqk9pcy-ARS3Y~wb_7z7lojZII5m4g ) to discuss things about the project together.

## Install

### Install Apache Pulsar

Function Stream uses [Apache Pulsar](https://pulsar.apache.org/) as the underlying messaging and processing engine, relying heavily on pulsar's features. Before we set up the function stream, we need to install apache pulsar.

* Download the modified version pulsar server package from [here](https://functionstream.oss-cn-hongkong.aliyuncs.com/apache-pulsar-2.8.1-SNAPSHOT.zip?versionId=CAEQHxiBgMDFwbmL0RciIDM5NTBlYmNhNGQzMzQ1ODM4Yjk5MjA0NmE5ODZiNGNl). 
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
* Start pulsar standalone with the command: `bin/pulsar standalone -nss`.
* Start pulsar proxy with the command: `bin/pulsar proxy`.

### Setup front end

> Note:  [Yarn](https://yarnpkg.com/) package management is recommended, the exact same version loaded with the demo site of this project (yarn.lock) . but you can also use npm.

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

* Access Function Stream at http://localhost:8000/.
