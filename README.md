Clocker2
=======

This project contains entities and other items for using Apache Brooklyn in a container ecosystem.

So far this includes:

* `docker-engine` for provisioning a VM with Docker hosts


Here's an example:

```
services:
- type: docker-engine
  brooklyn.children:
  - type: docker-container
    container: hello-world
```

