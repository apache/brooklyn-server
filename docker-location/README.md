---
section: Docker Swarm Location
section_type: inline
section_position: 2.1.2
---

### Docker Swarm Location

Cloudsoft AMP can deploy applications to [Docker Swarms](https://www.docker.com/products/docker-swarm) both provisioned by Cloudsoft AMP and set up manually.

Here is an example catalog item to add a Docker Swarm endpoint to your catalog locations:

    brooklyn.catalog:
      id: my-docker-swarm
      name: "My Docker Swarm"
      itemType: location
      item:
        type: jclouds:docker
        brooklyn.config:
          endpoint: << endpoint >>
          identity: << path to my cert.pem >>
          credential: << path to my key.pem >>
          image: "cloudsoft/centos:7"
          loginUser.password: "p4ssw0rd"

**Note** if you have provisioned your own docker swarm you may need to first pull the Cloudsoft configured image on the Swarm Manager:

    docker -H ${swarm_endpoint}  ${TLS_OPTIONS} pull cloudsoft/centos:7
    docker -H ${swarm_endpoint}  ${TLS_OPTIONS} images --no-trunc

#### Credentials for Deploying to Docker Swarm

To deploy to a Docker Swarm endpoint, you'll need pem files for identity/credential. These can
either be copied from one of the Docker Engine VMs, or can be generated from the certificate 
authority. The actual IP of the client doesn't matter. 

To generate your own certificates from the example CA server included in AMP (note this is not recommended for use in a production environment
 and could be subject to future removal):

    # Create your certificates directory
    mkdir -p .certs

    # Get yourself a certificate from the CA
    # You can use any IP; to find your IP use `ifconfig`
    own_ip=192.168.1.64
    ca=$(br app "Docker Swarm" ent ca-server sensor main.uri)
    echo ${ca}
    curl -X POST ${ca}/generate/${own_ip}
    curl ${ca}/cert/${own_ip}/ca.pem > .certs/ca.pem
    curl ${ca}/cert/${own_ip}/cert.pem > .certs/cert.pem
    curl ${ca}/cert/${own_ip}/key.pem > .certs/key.pem

To be able to execute `docker ...` commands locally:

    # Set up TLS options to point at your certificates
    CERTS_DIR=${HOME}/.docker/.certs
    TLS_OPTIONS="--tlsverify --tlscacert=${CERTS_DIR}/ca.pem --tlscert=${CERTS_DIR}/cert.pem --tlskey=${CERTS_DIR}/key.pem"

    # Check docker works
    swarm_endpoint=$(br app "Docker Swarm" ent "swarm-cluster" sensor swarm.url)
    echo ${swarm_endpoint}
    docker -H ${swarm_endpoint} ${TLS_OPTIONS} ps

    # Run something, and check it is listed
    docker -H  ${swarm_endpoint} ${TLS_OPTIONS} run hello-world
    docker -H ${swarm_endpoint}  ${TLS_OPTIONS} ps -a

Instead of explicit parameters to `docker` you can use its environment variables as follows:

    export DOCKER_HOST=tcp://10.10.10.152:3376
    export DOCKER_TLS_VERIFY=true
    export DOCKER_CERT_PATH=.certs
    docker ps -a