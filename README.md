# Cloudsoft Container Service

This project contains entities and other items for using Cloudsoft AMP in a
container ecosystem.

## Docker Engine

*   `docker-engine` for provisioning a VM with Docker Engine; normal
    `SoftwareProcess` configuration keys apply

*   `docker-engine-container` for provisioning a container on a `docker-engine`
     as a child of that node, using the key `container` for the image name at
     Docker Hub and optionally using `postLaunchCommand` key for any additional
     commands e.g. port-forwards

Here's an example:

```YAML
services:
- type: docker-engine
  brooklyn.children:
  - type: docker-engine-container
    container: hello-world
```

The implementation is as simple as possible, as you can see in the
[docker.bom](docker.bom) definition of these entities.

## Docker Swarm

*   `docker-swarm` for provisioning a cluster of VMs with Docker Swarm managed
    Docker Engines.

*   `ca-server` provides a simple certificate authority to generate the
    required TLS keys.

Here's an example, creating a Swarm cluster with a single manager node, an
etcd server for discovery, an OpenSSL certificate server, and three Docker
Engine nodes in the cluster. The cluster can be scaled to a maximum of five
nodes, using the default auto-scaling policy. It uses the `docker-swarm`
catalog entry.

```YAML
services:
- type: docker-swarm
  id: swarm
  name: "swarm"
  brooklyn.config:
    swarm.initial.size: 3
    swarm.max.size: 5
    etcd.initial.size: 1
```

This requires the [ca.bom](ca.bom) and [swarm.bom](swarm.bom) definitions to
be loaded in addition to the Docker Engine definitions in [docker.bom](docker.bom).

To extend the Swarm for high-availability and resilience, simply increase the
size of the manager and etcd clusters and optionally configure the recovery
timeout settings, as shown in the [swarm.yaml](examples/swarm.yaml) snippet
below.

```YAML
services:
- type: docker-swarm:2.0.0-SNAPSHOT # CLOCKER_VERSION
  id: swarm
  name: "swarm"
  brooklyn.config:
    swarm.initial.size: 8
    swarm.max.size: 16
    swarm.manager.size: 3
    etcd.initial.size: 3
    swarm.port: 4000
    swarm.defaultnetwork: "swarm"
    swarm.scaling.cpu.limit: 0.80
    swarm.strategy: "binpack"
    swarm.overcommit: 0.50
    swarm.recovery.stabilizationDelay: 10s
    swarm.recovery.failOnRecurringFailuresInThisDuration: 5m
    swarm.minRam: 32g
    provisioning.properties:
      minRam: 4g
      minCores: 2
```

This is configured for a more typical production Swarm cluster, with 8-16
Docker Engine members, three managers and three etcd servers. An Nginx load
balancer will also be created, and the `swarm.url` sensor on the root
application entity points to this, which will round-robin across the
available, healthy Swarm managers.

There are many other configuration options available for the Swarm service,
some of which are also shown in the above blueprint. Further details
can be found in the [swarm.bom](swarm.bom) file or the AMP documentation.
