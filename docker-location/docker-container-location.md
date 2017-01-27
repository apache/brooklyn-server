---
section: Docker Container Location
section_type: inline
section_position: 2.1.2
---

### Docker Container Location

Cloudsoft AMP can deploy applications to [Docker containers](https://www.docker.com/products/docker-engine) both provisioned by Cloudsoft AMP and set up manually.

Here is an example catalog item to add a Docker engine endpoint to your catalog locations:

    brooklyn.catalog:
      id: my-docker-engine
      name: "My Docker engine"
      itemType: location
      item:
        type: jclouds:docker
        brooklyn.config:
          endpoint: << endpoint >>
          identity: << path to my cert.pem >>
          credential: << path to my key.pem >>
          image: "cloudsoft/centos:7"
          loginUser.password: "p4ssw0rd"

**Note** The endpoint of a Docker engine is the IP + port where the docker engine is currently running. As for the identity and credential, the Docker engine will generate those by default in `~/.docker/certs` folder, unless you specified them during the installation.

#### Docker Container based blueprints

Once your Docker container location has been configured, AMP can launch instances based on a `DockerContainer` entity, this means additional configuration such as custom docker images can be specified. Here's an example which sets up a [Wordpress](https://wordpress.org/) instance:

    location:
      << see above >>

    services:
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
          deployment: wordpress-mysql
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

#### Docker container configuration

To configure the `DockerContainer` entity, the following configuration params are available:

- **docker.container.disableSsh** Skip checks such as ssh for when docker image doesn't allow ssh
- **docker.container.imageName** Image name to pull from docker hub
- **docker.container.inboundPorts** List of ports, that the docker image opens, to be made public
- **docker.container.environment** Environment variables to set on container startup. This must be a map
