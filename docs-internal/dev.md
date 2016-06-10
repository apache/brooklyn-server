### Setup

1. Run a bleeding-edge AMP (e.g. a very recent snapshot build)

2. Add each of the .bom files to the catalog 

Using the CLI:

    for b in *.bom tests/*.bom ; do 
       echo $b
       br add-catalog $b || break
     done

Note that the order in which you add the catalog entries matters, as you can’t load a file that uses a definition
that hasn’t been loaded yet.  the loop above relies on the fact that so far the files in alphabetical order are also
in dependency order, just for the convenience of the command. This is really just a convenience and
not an iron rule that we have to keep.  
 
**or** via the REST API:

    AMP_URL=http://127.0.0.1:8081
    AMP_USER=admin
    AMP_PASSWORD=pa55w0rd
    BOMS="ca.bom etcd.bom catalog.bom swarm.bom"
    BOMS="${BOMS} tests/common.tests.bom tests/docker.tests.bom tests/swarm.tests.bom "
    BOMS="${BOMS} tests/jclouds.tests.bom tests/swarm-endpoint.tests.bom tests/tests.bom"
    for i in ${BOMS}; do
      curl -u ${AMP_USER}:${AMP_PASSWORD} ${AMP_URL}/v1/catalog --data-binary @${i}
    done


3. Add the location definition(s) to your test. For example, use the bluebox definitions
   that the QA framework uses (don't forget to run the VPN, and to deploy to CentOS 7).
   See:
   
   - https://github.com/cloudsoft/blueprint-qa-seed/blob/master/locations/bluebox-singapore-centos7.bom
   - https://github.com/cloudsoft/cloudsoft/wiki/QA%20Framework
   - https://github.com/cloudsoft/cloudsoft/wiki/Blue-Box

    br add-catalog https://raw.githubusercontent.com/cloudsoft/blueprint-qa-seed/master/locations/bluebox-singapore-centos7.bom?token=ANfNJ5kY9pvufckGUZZ3mzM4FlHq1D2Aks5XX93owA%3D%3D

NOTE - you'll have to get a fresh token of your own by going to Github - I find it convenient enough to 
navigate to https://github.com/cloudsoft/blueprint-qa-seed/tree/master/locations, pick the cloud I want, hit the
"Raw" button and copy its URL.  Also note to add-catalog from https://... make sure you are using a build of `br` from 
after 8th May.

4. Delete the 1.0.0 version of the etcd entities from the catalog.
   (TODO: we don't replace it because snapshot versions (i.e. our 2.0.0-SNAPSHOT) does not take
   precedence over the 1.0.0 version).


### Running tests

To run the suite of tests, use `tests.bom`. For example:

    location:
      ibm-bluebox-sng-centos7-vpn
    services:
    - type: docker-and-swarm-engine-tests

And to run tests against an existing Swarm endpoint (changing the part to the pem files accordingly):

    location:
      jclouds:docker:
        endpoint: https://10.104.0.75:3376/
        identity: /Users/aled/.docker/.certs-from-server/cert.pem
        credential: /Users/aled/.docker/.certs-from-server/key.pem
        imageId: sha256:5670c22f2f46bfc7578447978fa7be62699d1e90ed88bf7e415ec96b2571ce0b
        loginUser: root
        loginUser.password: p4ssw0rd
        onbox.base.dir: /tmp
        # FIXME
        user: root
    services:
    - type: deploy-app-to-swarm-single-node
    - type: deploy-app-to-swarm-multi-node
    

### Running as an end-user

#### Docker Engine

To deploy a simple Docker Engine:

    name: Docker Engine
    location: ibm-bluebox-sng-centos7-vpn
    services:
    - type: docker-engine

Or a Docker Engine with a container:

    name: Docker Engine with container
    location: ibm-bluebox-sng-centos7-vpn
    services:
    - type: docker-engine
      brooklyn.children:
      - type: docker-engine-container
        container: cloudsoft/centos:7


To deploy a Docker Engine with TLS (which will also require a CA server):

    name: Docker Engine with TLS
    location: ibm-bluebox-sng-centos7-vpn
    services:
    - type: ca-server
      id: ca-server
      name: "ca-server"
    - type: docker-engine-tls
      brooklyn.config:
        customize.latch: $brooklyn:entity("ca-server").attributeWhenReady("service.isUp")
        ca.request.root.url: $brooklyn:entity("ca-server").attributeWhenReady("main.uri")


#### Docker Swarm

To deploy a Docker Swarm cluster:

    name: Docker Swarm
    location: ibm-bluebox-sng-centos7-vpn
    services:
    - type: docker-swarm
      brooklyn.config:
        swarm.initial.size: 1
        etcd.initial.size: 1


#### Deploying apps to Docker

To deploy to an entity to an existing Docker Swarm endpoint (first changing the path to the
cert.pem and key.pem - see next section for how to get those files). If targeting a Docker 
Engine directly, use port 2376; if targeting swarm, use 3376:

    name: SoftwareProcess on Docker
    location:
      jclouds:docker:
        endpoint: https://10.104.0.105:2376/
        identity: /Users/aled/.docker/.certs-from-server/cert.pem
        credential: /Users/aled/.docker/.certs-from-server/key.pem
        imageId: sha256:5670c22f2f46bfc7578447978fa7be62699d1e90ed88bf7e415ec96b2571ce0b
        loginUser: root
        loginUser.password: p4ssw0rd
        onbox.base.dir: /tmp
    services:
    - type: org.apache.brooklyn.entity.machine.MachineEntity
      brooklyn.config:
        onbox.base.dir.skipResolution: true
        sshMonitoring.enabled: false

Note you may need to first pull the image (depending how the Swarm cluster was provisioned):

    docker -H ${swarm_endpoint}  ${TLS_OPTIONS} pull cloudsoft/centos:7
    docker -H ${swarm_endpoint}  ${TLS_OPTIONS} images --no-trunc

Warning: jclouds-docker is currently broken against docker-engine, but works against swarm (as of 8/6/16)
https://github.com/jclouds/jclouds-labs/commit/7e55ad7971f94b19068cd8da32295d2ab5b9c18c
added "Node" but this is not returned by docker-engine rest api when inspecting a container.


### Credentials for Deploying to Docker Swarm

To deploy to a Docker Swarm endpoint, you'll need pem files for identity/credential. These can
either be copied from one of the Docker Engine VMs, or can be generated from the certificate 
authority. The actual IP of the client doesn't matter. 

To generate your own certificates from the CA server rest api that we wrote (note this is subject
to deletion in a future release!):

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
