---
section: Docker Container Location
section_type: inline
section_position: 2
---

### Docker Container Location

Cloudsoft AMP can deploy applications to [Docker containers](https://www.docker.com/products/docker-engine) both provisioned by Cloudsoft AMP and set up manually.

Here is an example catalog item to add a Docker engine endpoint to your catalog locations:

    brooklyn.catalog:
      id: my-docker-engine
      name: "My Docker engine"
      itemType: location
      item:
        type: docker
        brooklyn.config:
          endpoint: https://<< address >>:<< port >>
          identity: << path to my cert.pem >>
          credential: << path to my key.pem >>
          # Default image if no other explicitly set
          # imageId: "cloudsoft/centos:7"

**Note** The endpoint of a Docker engine is the IP + port where the docker engine is currently running. As for the identity and credential, the Docker engine will generate those by default in `~/.docker/certs` folder, unless you specified them during the installation.

#### Docker Container based blueprints

Once your Docker container location has been configured, AMP can launch instances based on a `DockerContainer` entity, this means additional configuration such as custom docker images can be specified. Here's an example which sets up a [Wordpress](https://wordpress.org/) instance:

    # see above for a definition of the location
    location: my-docker-engine

    services:
    - type: org.apache.brooklyn.container.entity.docker.DockerContainer
      id: wordpress-mysql
      name: MySQL
      brooklyn.config:
        mysql.root_password: password
        docker.container.imageName: mysql:5.6
        # Maps the port to the host node, making it available for external access
        docker.container.inboundPorts:
        - "3306"
        docker.container.environment: 
          MYSQL_ROOT_PASSWORD: $brooklyn:config("mysql.root_password")
    - type: org.apache.brooklyn.container.entity.docker.DockerContainer
      id: wordpress
      name: Wordpress
      brooklyn.config:
        docker.container.imageName: wordpress:4-apache
        # Maps the port to the host node, making it available for external access
        docker.container.inboundPorts:
        - "80"
        docker.container.environment: 
          WORDPRESS_DB_HOST: $brooklyn:entity("wordpress-mysql").attributeWhenReady("host.subnet.address")
          WORDPRESS_DB_PASSWORD: $brooklyn:entity("wordpress-mysql").config("mysql.root_password")

#### Docker container configuration

To configure the `DockerContainer` entity, the following configuration params are available:

- **docker.container.disableSsh** Skip checks such as ssh for when docker image doesn't allow ssh; use the default image `cloudsoft/centos:7` for ssh-able image
- **docker.container.imageName** Image name to pull from docker hub; overrides the default one `cloudsoft/centos:7`
- **docker.container.inboundPorts** List of ports, that the docker image maps to the host, opening them to the public
- **docker.container.environment** Environment variables to set on container startup; this must be a map
