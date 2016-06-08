# Cloudsoft Container Service

This project contains entities and other items for using Apache Brooklyn in a
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
[catalog.bom](catalog.bom) definition.

## Docker Swarm

*   `docker-swarm` for provisioning a cluster of VMs with Docker Swarm managed
    Docker Engines.

*   `ca-server` provides a simple certificate authority to generate the
    required TLS keys.

Here's an example:

```YAML
services:
- type: docker-swarm
  id: swarm
  name: "swarm"
  brooklyn.config:
    swarm.initial.size: 3
    etcd.initial.size: 1
```

This requires the [ca.bom](ca.bom) and [swarm.bom](swarm.bom) definitions to
be loaded in addition to Docker Engine.
