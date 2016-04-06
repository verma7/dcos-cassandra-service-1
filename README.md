DCOS Cassandra Service Guide
======================

[Overview](#overview)
- [Benefits](#benefits)
- [Features](#features)
- [Related Services](#related-services)

[Getting Started](#getting-started)
- [Quick Start](#quick-start)
- [Install and Customize](#install-and-customize)
  - [Default install configuration](#default-install-configuration)
  - [Custom install configuration](#custom-install-configuration)
- [Multiple Cassandra Cluster Installation](#multiple-cassandra-cluster-installation)
- [Uninstall](#uninstall)

[Configuring](#configuring)
- [Changing Configuration at Runtime](#changing-configuration-at-runtime)
  - [Configuration Deployment Strategy](#configuration-deployment-strategy)
  - [Configuration Update Plans](#configuration-update-plans)
  - [Configuration Update REST API](#configuration-update-rest-api)
- [Configuration Options](#configuration-options)
  - [Service Configuration](#service-configuration)
  - [Node Configuration](#node-configuration)
  - [Cassandra Application Configuration](#Cassandra-application-configuration)
  - [Communication Configuration](#communication-configuration)
  - [Hinted Handoff Configuration](#hinted-handoff-configuration)
  - [Snitch Configuration](#snitch-configuration)

[Connecting Clients](#connecting-clients) <!-- note this section may disappear -->

[Managing](#managing) <!-- note these sections may disappear -->
- [Add a Node](#add-a-node)
- [Cleanup](#cleanup)
- [Backup and Restore](#backup-and-restore)
  - [Backup](#backup)
    - [Backup Using the CLI](#backup-using-the-cli)
    - [Backup using the API](#backup-using-the-api)
  - [Restore](#restore)
    - [Restore Using the DCOS CLI](#Restore Using the DCOS CLI)
    - [Restore Using the API](#restore-using-the-api)

[Upgrading Software](#upgrading-software) <!-- note this section may disappear -->

[Troubleshooting](#troubleshooting)<!-- note there is not much here -->

[Limitations](#limitations)

[API Reference](#api-reference)
- [Connection Information](#connection-information)
- [Node Operations](#node-operations)
  - [Add Node](#add-node)
  - [Remove Node](#remove-node)

[Development](#development)

## Overview

DCOS Cassandra is an automated service that makes it easy to deploy and manage on Mesosphere DCOS, eliminating nearly all of the complexity traditional associated with managing a Cassandra cluster. Apache Cassandra is distributed database management system designed to handle large amounts of data across many servers, providing horizonal scalablity and high availability with no single point of failure, with a simple query language (CQL). For more information on Apache Cassandra, see the Apache Cassandra [documentation] (http://docs.datastax.com/en/cassandra/2.2/pdf/cassandra22.pdf). DCOS Cassandra gives you direct access to the Cassandra API so that existing applications can interoperate. You can configure and install DCOS Cassandra in moments. Multiple Cassandra clusters can be installed on DCOS and managed independently, so you can offer Cassandra as a managed service to your organization.

### Benefits

DCOS Cassandra offers the following benefits:

- Easy installation
- Multiple Cassandra clusters
- Elastic Cluster Scaling
- Replication for high availability
- Integrated monitoring


### Features

DCOS Cassandra provides the following features:

- Single command installation for rapid provisioning
- Persistent Storage volumes for enhanced data durability
- Runtime configuration and software updates for high availablity
- Health checks and metrics for monitoring
- Backup and restore for disaster recovery

### Related Services

- [DCOS Spark](https://docs.mesosphere.com/manage-service/spark)

## Getting Started

### Quick Start

- Step 1. Install a Cassandra cluster from the DCOS CLI.

```bash
$ dcos package install cassandra
```

Step 2. [SSH into an agent node](https://docs.mesosphere.com/administration/sshcluster/), and then launch a docker container:

```
$ docker run --net=host -it mohitsoni/alpine-cqlsh:2.2.5 /bin/sh
```

Step 3. Run /tmp/create.sh from inside the docker container to create a `demo` keyspace with a `map` table:

```
$ /tmp/create.sh
```

Step 4. Insert Fortune 1000 companies into the table:

``` bash
$ cqlsh -f /tmp/insert.cql <!-- need to specify the host before -f -- how to do this? -->
```

Step 6. Execute following query to show the data:

```bash
USE demo; SELECT * from demo.map;
```

### Install and Customize

#### Default install configuration

To start a basic test cluster, run the following command on the DCOS CLI:

``` bash
$ dcos package install cassandra
```

This command creates a new Cassandra cluster. Two clusters cannot share the same name, so installing additional clusters beyond the default cluster requires [customizing the `framework-name` at install time](#custom-install-configuration) for each additional instance.

All `dcos cassandra` CLI commands have a `--framework-name` argument that allows the user to specify which Cassandra instance to query. If you do not specify a framework name, the CLI assumes the default value, `cassandra`. The default value for `--framework-name` can be customized via the DCOS CLI configuration.

``` bash
$ dcos config set cassandra.framework_name new_default_name
```

The default cluster, `cassandra`, is intended for testing and development. It is not suitable for production use without additional customization. In particular, the default installation will create a cluster with three nodes with a default cassandra application configuration.

#### Custom install configuration

Customize the defaults by creating a JSON file. Then pass it to `dcos package install` using the `--options` parameter.

Sample JSON options file named `sample-cassandra.json`:

``` json
{
    "node": {
        "nodes": 10,
        "seeds": 3
    }
}
```

The command below creates a cluster using `sample-cassandra.json`:

``` bash
$ dcos package install --options=sample-cassandra.json cassandra
```

This cluster will have 10 nodes and 3 seeds instead of the default values of 3 nodes and 2 seeds.
See [Configuration Options](#configuration-options) for a list of fields that can be customized via an options JSON file when the Cassandra cluster is created.

### Multiple Cassandra Cluster Installation

Installing multiple Cassandra clusters is identical to installing a Cassandra cluster with a custom configuration as described above. Use a JSON options file to specify a unique `framework-name` for each installation:

``` json
$ cat cassandra1.json
{
 "service": {
   "name": "cassandra1"
 }
}

$ dcos package install cassandra --options=cassandra1.json
```

In order to avoid port conflicts, by default you cannot collocate more than one Cassandra instance on the same node. 

####Installation Plan
When the DCOS Cassandra service is initially installed it will generate an installation plan as shown below. 

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "InProgress"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "InProgress"
        }
    ], 
    "status": "InProgress"
}
```

#####Viewing the Installation Plan
The plan can be viewed from the API via the REST endpoint. A curl example is provided below.

``` bash
curl http:/<dcos_url>/service/cassandra/v1/plan
```

#####Plan Errors
If there are any errors that prevent installation, these errors are dispayed in the errors list. The presence of errors indicates that the installation can not progress.
#####Reconciliation Phase
The first phase of the installation plan is the reconciliation phase. This phase ensures that the DCOS Cassandra service maintains the correct status for the Cassandra nodes that it has deployed. For large deployments, reconciliation will occur periodically, and the in-progress phase will switch from Deploy to Reconciliation. This is not cause for concern.
#####Deploy Phase
The second phase of the installation is the deploy phase. This phase will deploy the request number of Cassandra nodes. Each block in the phase represents an individual Cassandra node. In the plan shown above the first node, node-0, has been deployed, the second node, node-1, is in the process of being deployed, and the third node, node-2, is pending deployment based on the completion of node-1.
#####Pausing Installation
In order to pause installation a REST API request, as shown below, can be issued. The installation will pause after completing installation of the current node and wait for user input.

``` bash
curl -X PUT http:/<dcos_url>/service/cassandra/v1/plan?cmd=interrupt
```

#####Resuming Installation
If installation has been paused. The REST API request below will resume installation at the next pending node.

``` bash
curl -X PUT http://<dcos_surl>/service/cassandra/v1/plan?cmd=proceed
```

### Uninstall

Uninstalling a cluster is also straightforward. Replace `cassandra` with the name of the Cassandra instance to be uninstalled.

``` bash
$ dcos package uninstall --app-id=cassandra
```

Then, use the [framerwork cleaner script](https://github.com/mesosphere/framework-cleaner) to remove your Cassandra instance from Zookeeper and to destroy all data associated with it. The script require several arguments, the values for which are derived from your framework name:

`framework-role` is `<framework-name>-role`.
`framework-principle` is `<framework-name>-principal.
`zk_path` is `<framework-name>`.

## Configuring

### Changing Configuration at Runtime

You can customize your cluster in-place when it is up and running.

The Cassandra scheduler runs as a Marathon process and can be reconfigured by changing values within Marathon. These are the general steps to follow:

1. View your Marathon dashboard at `http://<dcos_url>/marathon`
2. In the list of `Applications`, click the name of the Cassandra framework to be updated.
3. Within the Cassandra instance details view, click the `Configuration` tab, then click the `Edit` button.
4. In the dialog that appears, expand the `Environment Variables` section and update any field(s) to their desired value(s). For example, to increase the number of nodes, edit the value for `NODES`. Click `Change and deploy configuration` to apply any changes and cleanly reload the Cassandra scheduler. The Cassandra cluster itself will persist across the change.

#### Configuration Deployment Strategy

Configuration updates are rolled out through execution of Update Plans. You can configure the way these plans are executed.

##### Configuration Update Plans 
This configuration update strategy is analogous to the installation procedure above. If the configuration update is accepted, there will be no errors in the generated plan, and a rolling restart will be performed on all nodes to apply the updated configuration. However, the default strategy can be overridden by a user provided strategy.

### Configuration Update REST API

Make the REST request below to view the current plan:

``` bash
curl -v http://<dcos_url>/service/cassandra/v1/plan
```

Response will look similar to this:

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": true, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": true, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Pending"
        }
    ], 
    "status": "InProgress"
}
```

If you want to interrupt a configuration update that is in progress, enter the `interrupt` command.

And, now if you query the plan again, here's how the response will look (notice `status: "Waiting"`):

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": “Complete"
                }, 
                {
                    "has_decision_point": false”, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "InProgress"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Waiting"
        }
    ], 
    "status": "Waiting"
}
```

**Note:** The interrupt command can’t stop a block that is `InProgress`, but it will stop the change on the subsequent blocks.

Enter the `continue` command to resume the update process.

After you execute the continue operation, the plan will look like this:

```
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": “Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Pending"
        }
    ], 
    "status": "InProgress"
}
```

### Configuration Options

The following describes the most commonly used features of the Cassandra framework and how to configure them via the DCOS CLI and in Marathon. There are two methods of configuring a Cassandra cluster. The configuration may be specified using a JSON file during installation via the DCOS command line (See the [Installation section](#installation)) or via modification to the Service Scheduler’s Marathon environment at runtime (See the [Configuration Update section](#configuration-update)). Note that some configuration options may only be specified at installation time, but these generally relate only to the service’s registration and authentication with the DCOS scheduler.

#### Service Configuration

The service configuration object contains properties that MUST be specified during installation and CANNOT  be modified after installation is in progress. This configuration object is similar across all DCOS Infinity frameworks. Service configuration example:

``` json
{
	"service": {
		"name": "cassandra2",
		"role": "cassandra_role",
		"principal": "cassandra_principal",
		"secret" : "/path/to/secret_file",
		"apiPort": 9000,
		"adminPort": 9001
	}
}
```

| Property | Type | Description         |
|----------| ------ | ----------------- |
| name     | string | The name of the Cassandra cluster. |
| role     | string | The authentication and resource role of the Cassandra cluster. |
| principal| string | The authentication principal for the Cassandra cluster. |
|secret    | string | An optional path to the file containing the secret that the service will use to authenticate with the Mesos Master in the DCOS cluster. This parameter is optional, and should be omitted unless the DCOS deployment is specifically configured for authentication. |
| apiPort  | integer| The port on which the service’s scheduler will listen for API requests. |
| adminPort| integer| The port on which the service’s scheduler will listen for health checks and admin requests. |

- **In the DCOS CLI, options.json**: `framework-name` = string (default: `cassandra`)
- **In Marathon**: The framework name cannot be changed after the cluster has started.

#### Node Configuration

The node configuration object corresponds to the configuration for Cassandra nodes in the Cassandra cluster. Node configuration MUST be specified during installation and MAY be modified during configuration updates. All of the properties except for volume_size MAY be modified during the configuration update process.

Example node configuration:

``` json
{
	"node": {

		"cpus": 0.5,
		"mem": 4096,
		"disk": 10240,
		"heap": {
			"size": 2048,
			"new": 400
		},
		"volume_size": 9216,
		"count": 3,
		"seeds": 2
	}
}
```

| Property    | Type    | Description     |    
| ----------- | ------- | --------------- |
| cpus        | number  | The number of cpu shares allocated to the container where the Cassandra process resides. Currently, due to a bug in Mesos, it is not safe to modify this parameter. |
| mem         | integer | The amount of memory, in MB, allocated to the container where the Cassandra process resides. This value MUST be larger than the specified max heap size. Make sure to allocate enough space for additional memory used by the JVM and other overhead. |
| disk        | integer | The amount of disk, in MB, allocated to the container where the Cassandra process runs. This value MUST be greater than or equal to the size of the allocated persistent value. In the event that it is greater than the size of the persistent volume, the additional resources will be applied to the sandbox for the Cassandra process. |
| heap.size   | integer | The maximum and minimum heap size used by the Cassandra process in MB. This value SHOULD be at least 2 GB, and it SHOULD be no larger than 80% of the allocated memory for the container. Specifying very large heaps, greater than 8 GB, is currently not a supported configuration. |
| heap.new    | integer | The young generation heap size in MB. This value should be set at roughly 100MB per allocated CPU core. Increasing the size of this value will generally increase the length of garbage collection pauses. Smaller values will increase the frequency of garbage collection pauses. |
| volume_size | integer | The size of the persistent volume on which Cassandra’s commit log and data directory will be stored. This value MUST be less than or equal to the size of the allocated disk to the container. Currently, only one persistent volume per node is supported so the commit log, saved caches, and data directories will all be written to the same volume. |
| count       | integer | The number of nodes in the Cassandra cluster. This value MUST be between 3 and 100. |
| seeds      | integer  | The number of seed nodes that the service will use to seed the cluster. The service selects seed nodes dynamically based on the current state of the cluster. 2 - 5 seed nodes is generally sufficient. |

#### Cassandra Application Configuration

The Cassandra application is configured via the cassandra json object. **You should not modify these settings without strong reason and an advanced knowledge of Cassandra internals and cluster operations.** The available configuration items are included for advanced users who need to tune the default configuration for specific workloads.

Example Cassandra configuration:

``` json
{
	"cassandra": {
		"jmxPort": 7199,
		"hintedHandoffEnabled": true,
		"maxHintWindowInMs": 10800000,
		"hintedHandoffThrottleInKb": 1024,
		"maxHintsDeliveryThreads": 2,
		"batchlogReplayThrottleInKb": 1024,
		"keyCacheSavePeriod": 14400,
		"rowCacheSizeInMb": 0,
		"rowCacheSavePeriod": 0,
		"commitlogSyncPeriodInMs": 10000,
		"commitlogSegmentSizeInMb": 32,
		"concurrentReads": 16,
		"concurrentWrites": 32,
		"concurrentCounterWrites": 16,
		"memtableAllocationType": "heap_buffers",
		"indexSummaryResizeIntervalInMinutes": 60,
		"storagePort": 7000,
		"startNativeTransport": true,
		"nativeTransportPort": 9042,
		"tombstoneWarnThreshold": 1000,
		"tombstoneFailureThreshold": 100000,
		"columnIndexSizeInKb": 64,
		"batchSizeWarnThresholdInKb": 5,
		"batchSizeFailThresholdInKb": 50,
		"compactionThroughputMbPerSec": 16,
		"sstablePreemptiveOpenIntervalInMb": 50,
		"readRequestTimeoutInMs": 5000,
		"rangeRequestTimeoutInMs": 10000,
		"writeRequestTimeoutInMs": 2000,
		"counterWriteRequestTimeoutInMs": 5000,
		"casContentionTimeoutInMs": 1000,
		"truncateRequestTimeoutInMs": 60000,
		"requestTimeoutInMs": 1000,
		"dynamicSnitchUpdateIntervalInMs": 100,
		"dynamicSnitchResetIntervalInMs": 600000,
		"dynamicSnitchBadnessThreshold": 0.1,
		"internodeCompression": "all"
	}
}
```

#### Communication Configuration

The IP address of the Cassandra node is determined automatically by the service when the application is deployed. The listen addresses are appropriately bound to the addresses provided to the container where the process runs. The following configuration items allow users to specify the ports on which the service operates.

| Property             | Type    | Description |
| -------------------- | ------- | --------------- |
| jmxPort              | integer | The port on which the application will listen for JMX connections. Remote JMX connections are disabled due to security considerations. |
| storagePort          | integer | The port the application uses for inter-node communication. |
| internodeCompression | all|none|dc | If set to all, traffic between all nodes is compressed. If set to dc, traffic between datacenters is compressed. If set to none, no compression is used for internode communication. |
| nativeTransportPort  | integer | The port the application uses for inter-node communication. |
#### Commit Log Configuration

The DCOS Cassandra service only supports the commitlog_sync model for configuring the Cassandra commit log. In this model a node responds to write requests after writing the request to file system and replicating to the configured number of nodes, but prior to synchronizing the commit log file to storage media. Cassandra will synchronize the data to storage media after a configurable time period. If all nodes in the cluster should fail, at the Operating System level or below, during this window the acknowledged writes will be lost. Note that, even if the JVM crashes, the data will still be available on the nodes persistent volume when the service recovers the node.The configuration parameters below control the window in which data remains acknowledged but has not been written to storage media.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| commitlogSyncPeriodInMs  | integer | The time, in ms, between successive calls to the fsync system call. This defines the maximum window between write acknowledgement and a potential data loss. |
| commitLogSegmentSizeInMb | integer | The size of the commit log in MB. This property determines the maximum mutation size, defined as half the segment size. If a mutation's size exceeds the maximum mutation size, the mutation is rejected. Before increasing the commitlog segment size of the commitlog segments, investigate why the mutations are larger than expected. |

#### Column Index Configuration
| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| columnIndexSizeInKb | integer | Index size  of rows within a partition. For very large rows this value can be decreased to increase seek time. If key caching is enabled be careful when increasing this value, as the key cache may become overwhelmed. |
|indexSummaryResizeIntervalInMiinutes| integer |  How frequently index summaries should be re-sampled in minutes. This is done periodically to redistribute memory from the fixed-size pool to SSTables proportional their recent read rates.|


#### Hinted Handoff Configuration

Hinted handoff is the process by which Cassandra recovers consistency when a write occurs and a node that should hold a replica of the data is not available. If hinted handoff is enabled, Cassandra will record the fact that the value needs to be replicated and replay the write when the node becomes available. Hinted handoff is enabled by default, and the following table describes how hinted handoff can be configured.

| Property                | Type  | Description     |
| ------------------------ | ------- | ------------------- |
| hintedHandoffEnabled     | boolean | If true, hinted handoff will be used to maintain consistency during node failure. |
| maxHintWindowInMs        | integer | The maximum amount of time, in ms, that Cassandra will record hints for an unavailable node. |
| maxHintDeliveryThreads   | integer | The number of threads that deliver hints. The default value of 2 should be sufficient most use cases. |

#### Dynamic Snitch Configuration
The endpoint snitch for the service is always the GossipPropertyFileSnitch, but, in addition to this, Cassandra uses a dynamic snitch to determine when requests should be routed away from poorly performing nodes. The configuration parameters below control the behavior of the snitch.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| dynamicSnitchBadnessThreshold | number |  Controls how much worse a poorly performing node has to be before the dynamic snitch prefers other replicas over it. A value of 0.2 means Cassandra continues to prefer the static snitch values until the node response time is 20% worse than the best performing node. Until the threshold is reached, incoming requests are statically routed to the closest replica. | 
| dynamicSnitchResetIntervalInMs | integer | Time interval, in ms, to reset all node scores, allowing a bad nodes to recover. |
| dynamicSnitchUpdateIntervalInMs | integer | The time interval, in ms, for node score calculation. This is a CPU intensive operation. Reducing this interval should be performed with extreme caution. |

#### Global Key Cache Configuration
The partition key cache is a cache of the partition index for a Cassandra table. It is enabled by setting the parameter when creating the table. Using the key cache, instead of relying on the OS page cache can decrease CPU and memory utilization. However, as the value associated with keys in the partition index is not cached along with key, reads that utilize the key cache will still require that row values be read from storage media, through the OS page cache. The following configuraiton items control the system global configuration for key caches.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| keyCacheSavePeriod | integer | The duration in seconds that keys are saved in cache. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O. |
| keyCacheSizeInMb | integer | The maximum size of the key cache in Mb. When no value is set, the cache is set to the smaller of 5% of the available heap, or 100MB. To disable set to 0. |

### Global Row Cache Configuration
Row caching caches both the key and the associated row in memory. During the read path, when rows are reconstructed from the MemTable and SSTables, the reconstructed row is cached in memory preventing further reads from storage media until the row is ejected or dirtied. 
Like key caching, row caching is configurable on a per table basis, but it should be used with extreme caution. Misusing row caching can result in overwhelming the JVM and causing Cassandra to crash. Use row caching under the following conditions - only if you are absolutely certain that doing so will not overwhelm the JVM, the partition you will cache is small, and client applications will read most of the partition all at once.
The following configuration properties are global for all row caches.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| rowCacheSavePeriod | integer | The duration in seconds that rows are saved in cache. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O. |
| rowCacheSizeInMb | integer | The maximum size of the key cache in Mb. Make sure to provide enough space to contain all the rows for tables that will have row caching enabled. |

### Connecting Clients
The only supported client for the DSOC Cassandra Service is the Datastax Java CQL Driver. Note that this means that Thrift RPC based clients are not supported for use with this service and any legacy applications that use this communication mechanism are run at the user's risk.
####Connection Info Using the CLI
The following command can be executed from the cli in order to retrieve a set of nodes to connect to.

``` bash
dcos cassandra --framework-name=<framework-name> node
```

####Connection Info Using the API
The following curl example demonstrates how to retrive connection a set of nodes to connect to using the REST API.

``` bash
curl http://<dcos_url>/cassandra/v1/nodes/connect
```

####Connection Info Response
The response, for both the CLI and the REST API is as below.

``` json
[
    "10.0.0.47:9042", 
    "10.0.0.50:9042", 
    "10.0.0.49:9042"
]
```

This JSON array contains a list of valid servers that the client can use to connect to the Cassandra cluster. For availability reasons, it is best to specify multiple servers in configuration of the CQL Driver used by the application. 

####Configuring the CQL Driver
#####Adding the Driver to Your Application

``` xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.0.0</version>
</dependency>
```

The above is the correct dependency for CQL driver to use with the DCOS Cassandra service. After adding this dependency to your project, you should have access to the correct binary dependencies to interface with the Cassandra Cluster.
#####Connecting the CQL Driver.
The code snippet below demonstrates how to connect the CQL driver to the cluster and perform a simple query.

```
Cluster cluster = null;
try {

   List<InetSocketAddress> addresses = Arrays.asList(
       new InetSocketAddress("10.0.0.47", 9042),
       new InetSocketAddress("10.0.0.48", 9042),
       new InetSocketAddress("10.0.0.45", 9042));

    cluster = Cluster.builder()                                                    
            .addContactPointsWithPorts(addresses)
            .build();
    Session session = cluster.connect();                                           

    ResultSet rs = session.execute("select release_version from system.local");   
    Row row = rs.one();
    System.out.println(row.getString("release_version"));                          
} finally {
    if (cluster != null) cluster.close();                                          
}
```

## Managing

### Add a Node
Increase the `NODES` value via Marathon as described in the Configuration Update section. This create a update plan as described in that section. An additional node will be added as the last block of that plan. After a node has been added it is advisable to run cleanup, as described in the Cleanup subsection of the Managing section. It is safe to delay running cleanup until off peak hours.

### Cleanup
Cassandra does not automatically remove data when a node looses part of its partition range. This can occur when nodes are added or removed from the ring. To remove the unnecessary data cleanup should be run. Cleanup can be a CPU and disk intensive operation. As such, it is recommended to delay running cleanup until off peak hours. The DCOS Cassandra service will minimize the aggregate CPU and disk utilization for the cluster by performing cleanup for each selected node, sequentially.

####Cleanup Using the CLI
From the cli enter the following command:

``` bash
dcos cassandra --framework-name=<framewor-name> cleanup --nodes=<nodes>
```

Here <nodes> is an optional comma separated list indicating the nodes to cleanup.

###Cleanup Using the API
The following curl command demonstrates how to start a cleanup operation.

``` bash
curl -X PUT -H “Content-Type:application/json” http://<dcos_url>/service/cassandra/v1/cleanup/start --data @cleanup.json
```

####Cleanup Payload
The cleanup payload reference by, cleanup.json above, is a JSON object that indicates which nodes, keyspaces, and column families should have the cleanup operation applied. Note that system keyspaces should not be included here. If keySpaces is empty then all keyspaces will be cleaned, if nodes is empty then cleanup will be applied to all nodes. If columnFamilies is empty, then all column families, for the selected keyspaces will be cleaned.

``` json
{
	"nodes": [
		"node-0",
		"node-1"
	],
	"keySpaces": [
		"my-keyspace"
	],
	"columnFamilies": [
		"my-first-table",
		"my-second-table"
	]
}
```

### Backup and Restore
The DCOS Cassandra framework does not allow for autosnapshot to be enabled. Autosnapshot created on disk snapshots that Cassandra will never remove. Instead the service provides a mechanism to snapshot your tables and ship them to a remote location. 
Once the snapshots have been uploaded to a remote location, you can restore the data to a new cluster, in the event of a disaster, or restore them to an existing cluster, in the event that a user error has caused a data loss.

#### Backup

You can take a complete snapshot of your DCOS Cassandra ring and upload the artifacts to S3. Do the following to initiate the backup:

##### Backup Using the CLI

Enter the following command on the DCOS CLI:

``` bash
dcos cassandra --framework-name=<framework-name> backup start \
    --name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

Check status of the backup:

``` bash
dcos cassandra --framework-name=<framewor-name> backup status
```

##### Backup using the API

First, create the request payload, for example in a file `backup.json`:

``` json
{
    "name":"<backup-name>",
    "external-location":"s3://<bucket-name>",
    "s3-access-key":"<s3-access-key>",
    "s3-secret-key":"<s3-secret-key>"
}
```

Then, submit the request payload via `PUT` request to `/v1/backup/start`

``` bash
curl -X PUT -H 'Content-Type: application/json' -d @backup.json http://cassandra.marathon.mesos:9000/v1/backup/start
{"status":"started", message:""}
```

Check status of the backup:

``` bash
curl -X GET http://cassandra.marathon.mesos:9000/v1/backup/status
```

#### Restore

You can restore your DCOS Cassandra snapshots on a new Cassandra ring.

##### Restore Using the DCOS CLI

Enter the following command on the DCOS CLI:

``` bash
dcos cassandra --framework-name=<framework-name> restore start \
    --name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

Check the status of the restore:

``` bash
dcos cassandra --framework-name=<framework-name> restore status
```

##### Restore Using the API

First, bring up a new instance of your Cassandra cluster with the same number of nodes as the cluster whose snapshot backup you want to restore.

Next, create the request payload, for example in a file `restore.json`:

``` json
{
    "name":"<backup-name-to-restore>",
    "external-location":"s3://<bucket-name-where-backups-are-stored>",
    "s3-access-key":"<s3-access-key>",
    "s3-secret-key":"<s3-secret-key>"
}
```

Next, submit the request payload via `PUT` request to `/v1/restore/start`

``` bash
curl -X PUT -H 'Content-Type: application/json' -d @restore.json http://cassandra.marathon.mesos:9000/v1/restore/start
{"status":"started", message:""}
```

To check status of the restore:

``` bash
curl -X GET http://cassandra.marathon.mesos:9000/v1/restore/status
```

## Upgrading Software

``` bash
dcos package upgrade cassandra <!-- ??? -->
```

## Troubleshooting

You can access the `stderr` and `stdout` logs from the Marathon web interface. The logs appear in the details view for your DCOS Cassandra instance.

## Limitations

- Cluster backup and restore can only be performed sequentially across the entire cluster. While this makes cluster backup and restore time consuming, it also ensures that taking backups and restoring them will not overwhelm the cluster or the network. In the future, the framework could allow for a user specified degree of parallelism when taking backups. 
- Cluster restore can only restore a cluster of the same size as, or larger than, the cluster from which the backup was taken.
- While nodes can be replaced, there is currently no way to shrink the size of the cluster. Future releases will contain decommissions and remove operations.
- Anti-Entropy repair can only be performed sequentially, for the primary range of each node, across the entire cluster. There are use cases where one might wish to repair an individual node, but running the repair procedure, as implemented, is always sufficient to repair the cluster.

## Development

View the [development guide](https://github.com/mesosphere/dcos-cassandra-service/blob/master/dev-guide.md).
