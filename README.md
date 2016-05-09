Clocker2
=======

This project contains entities and other items for using Apache Brooklyn in a container ecosystem.

So far this includes:

* `docker-engine` for provisioning a VM with Docker hosts; normal `SoftwareProcess` configuration keys apply
* `docker-engine-container` for provisioning a container on a `docker-engine`, as a child of that node,
  using the key `container` for the image name at Docker Hub
  and optionally using `postLaunchCommand` (from `SoftwareProcess`) for any additional commands e.g. port-forwards

Here's an example:

```
services:
- type: docker-engine
  brooklyn.children:
  - type: docker-engine-container
    container: hello-world
```

The implementation is as simple as possible, as you can see in the `catalog.bom` definition.
