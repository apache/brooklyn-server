# Kubernetes Location

This project contains entities and other items for using Cloudsoft AMP in a Kubernetes ecosystem.

Deploy to a Kubernetes cluster by modelling a `KubernetesPod` entity which is made up of multiple heterogeneous `DockerContainer` entities.

## Plain-AMP blueprints

Here's an example:

```YAML
location:
  kubernetes:
    endpoint: "https://192.168.99.100:8443/"

services:
- type: org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess
  name: Simple Netcat Server

  brooklyn.config:
    provisioning.properties:
      inboundPorts: [22, 4321]

    launch.command: |
      echo hello | nc -l 4321 &
      echo $! > $PID_FILE
    checkRunning.command: |
      true
```

For each entity AMP will create
a [deployment](http://kubernetes.io/docs/user-guide/deployments/)
containing a single [replica](http://kubernetes.io/docs/user-guide/replicasets/)
of a [pod](http://kubernetes.io/docs/user-guide/pods/) containing a single
SSHable container based on the `tutum/ubuntu` image. It will install and launch
the entity in the typical AMP way. Each `inboundPort` will be exposed as a
[NodePort service](http://kubernetes.io/docs/user-guide/services/#type-nodeport).

To explain the config options:
* `env` The `tutum/ubuntu` image uses an environment variable named `ROOT_PASS`
   to assign the SSH login user password.
* `inboundPorts` The set of ports that should be exposed by the service.


## DockerContainer based blueprints

Here's an example:


```YAML
location:
  kubernetes:
    endpoint: "https://192.168.99.100:8443/"

services:
- type: io.cloudsoft.amp.containerservice.kubernetes.entity.KubernetesPod
  brooklyn.children:
  - type: io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer
    id: wordpress-mysql
    name: MySQL
    brooklyn.config:
      docker.container.imageName: mysql:5.6
      docker.container.inboundPorts:
      - "3306"
      provisioning.properties:
        env:
          MYSQL_ROOT_PASSWORD: "password"
        kubernetes.deployment: wordpress-mysql
  - type: io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer
    id: wordpress
    name: Wordpress
    brooklyn.config:
      docker.container.imageName: wordpress:4.4-apache
      docker.container.inboundPorts:
      - "80"
      provisioning.properties:
        env:
          WORDPRESS_DB_HOST: "wordpress-mysql"
          WORDPRESS_DB_PASSWORD: "password"
```
