# Transport Informer

## Background
### Does the informer have to reflect ETCD? 
![00](./docs/images/00.png)

Kubernetes utilizes in-memory caching to effectively reduce the load pressure on the API server caused by a large number of controllers. The consistency between this cache and the persistent layer is ensured by ETCD, which provides a reliable List/Watch interface. So that the [informers can watch the desired sate of resources in a scalable and sustainable fashion. Beyond that, they also implement a resync mechanism that enforces periodic reconciliation, and is often used to make sure that the cluster state and the assumed state cached in memory do not drift(e.g., due bugs or network issues)](https://www.oreilly.com/library/view/programming-kubernetes/9781492047094/ch01.html).

![001](./docs/images/001.png)
So far everything seems fine, but if we need to see the deployed resource not just in one cluster, but in multiple clusters. At this time, [8G's ETCD](https://github.com/etcd-io/etcd/issues/9771) might be inadequate. A more straightforward idea is to replace ETCD with other scalable databases, like kine, as an etcdshim that translates ETCD API to Postgres, MqSql and SQLit. Anther way is to find a reliably method for the controlplane to take advantage of ETCDs distributed on different clusters？

### Transporter might be an interesting candidate!

![message queue](./docs/images/message-queue.png)
Transport(Message Queue) can **decouple**, improve **scalability**, and peak shaving and valley filling of the **flow rate**. These excellent features not only shine in application services, But also should be of great value to platform management.

![twins](./docs/images/twins.png)
The Kubernetes controller achieves the desired state of the resource through event accumulation. The message queue is to change the state of a real object through data streaming. There seem to be some similarities between Kubernetes Event and Transport Message.

In order to gain a large scalability, [Mutilcluster-Global-Hub](https://github.com/stolostron/multicluster-global-hub) brings transport to increases the cluster fleet size of the management. While to ensure data consistency, the agent sends all object status data of a certain type each time, which is called a bundle. And also the manager has a complex message confirmation methods. Which seems not elegant enough and CloudNative.

### How to make this informer to watch the transport message instead of ETCD event?

![002](./docs/images/002.png)
To eliminate the gap between transport and ETCD, the first issue need to be addressed is:
How to make transport respond to Informer's List/Watch like ETCD? Thanks to the [event-informer](https://github.com/qiujian16/events-informer) for bringing us an idea. 

### How to make Transport as reliable as ETCD？

By introducing a custom Transport, we also bring an additional layer that is responsible for communication between the client and the data store. While we can configure the transport to handle failures or errors to decrease the risk of losing data, we still cannot guarantee that the transport will be as reliable as ETCD. 

In order to improve the robustness of reconciliation in Informer, the resync mechanism is used to synchronize the full cache to the delta queue. This make the reconciliation process to handle failures or discrepancies gracefully. This also brings us a way to enhance the reliability of transport, that is, to add a mechanism to resync full data on transport. There are two ways to achieve it:
- Informer lists all the resources from transport Periodically
- The provider periodically sends all objects of a GVR from transport to Informer's watch channel, and Informer determines whether to process them based on ResourceVersion

| Now I will use MQTT as a transport to integrate with informer to do some interesting experiments.
![feel the power](./docs/images/power.png)


## Start a transport: eclipse-mosquitto MQTT Broker
```bash
echo "listener 1883 0.0.0.0
allow_anonymous true" > samples/mosquitto.conf
docker run --rm --name mosquitto -p 1883:1883 -v "$(pwd)/samples/mosquitto.conf:/mosquitto/config/mosquitto.conf" eclipse-mosquitto
```

## Scenario 1: As a monitor to view resources of clusters
![003](./docs/images/003.png)

### Demo
```bash
# build binary
make build
# cluster1 cluster
./bin/provider --broker 127.0.0.1:1883 --client-id providerId --send /provider/payload --receive /informer/signal --cluster cluster1
# controlplane cluster
./bin/informer --broker 127.0.0.1:1883 --client-id informerId --send /informer/signal --receive /provider/payload
```
> Create/update/delete resource on cluster1, then check the resource on controlplane cluster

### Pros
- Using informer's List and Watch mechanism, which can ensure the consistency of data from the clusters end to the hub side.
- The hub side can extend scalability by connecting the informer to a persistent database
  - Replace the informer local cache with a database(RDB/KV)
  - Persist the resources to the RDB/KV database in the informer's event handler(For scalability, LocalCache needs to be disabled)


### Cons
- How to distribute the workload from hub to these clusters?
- How to reconcile the cluster resources by the hub spec, such as deleting a resource on the cluster to ensure it can be recreated?
- How to view the cluster resources status from hub view?

## Scenario 2: Using bidirectional informer to deploy workload

![bidirectional-informer](./docs/images/bidirectional-informer.png)

### Demo
```bash
./bin/agent --broker tcp://127.0.0.1:1883 --client-id sourceId --signal-topic /signal --payload-topic /payload --cluster cluster1
./bin/hub --broker tcp://127.0.0.1:1883 --client-id controlplaneId --signal-topic /signal --payload-topic /payload
```

#### Cons
- How to define the Cluster CRD and manage it's lifecycle
- How to handle workload scheduling, like the Placement in OCM?