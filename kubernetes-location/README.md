# Kubernetes location

This project contains entities and other items for using Cloudsoft AMP in a Kubernetes ecosystem.

Deploy to a Kubernetes cluster by modelling a `KubernetesPod` entity which is made up of multiple heterogeneous `DockerContainer` entities.

## Plain-AMP blueprints

Here's an example:

```YAML
location:
  kubernetes:
    endpoint: "https://192.168.99.100:8443/"
    identity: "test"
    credential: "test"

services:
- type: org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess
  name: Simple Netcat Server
  env: { ROOT_PASS : password }

  brooklyn.config:
    provisioning.properties:
      inboundPorts: [22, 4321]

  launch.command: |
    echo hello | nc -l 4321 &
    echo $! > $PID_FILE
```

## DockerContainer based blueprints

Here's an example:


```YAML
location:
  kubernetes:
    endpoint: "https://192.168.99.100:8443/"
    identity: "test"
    credential: "test"

services:
- type: io.cloudsoft.amp.container.kubernetes.entity.KubernetesPod
  brooklyn.children:
  - type: io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer
    id: wordpress-mysql
    brooklyn.config:
      docker.container.imageName: mysql:5.6
      docker.container.inboundPorts: [ "3306" ]
      env: { MYSQL_ROOT_PASSWORD: "password" }
      provisioning.properties:
        kubernetes.deployment: wordpress-mysql
  - type: io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer
    id: wordpress
    brooklyn.config:
      docker.container.imageName: wordpress:4.4-apache
      docker.container.inboundPorts: [ "80" ]
      env: { WORDPRESS_DB_HOST: "wordpress-mysql", WORDPRESS_DB_PASSWORD: "password" }
```

The implementation is as simple as possible, as you can see in the
[kubernetes.bom](kubernetes.bom) definition of these entities.

