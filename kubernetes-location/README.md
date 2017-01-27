---
section: Kubernetes Location
section_type: inline
section_position: 4
---

### Kubernetes Location

Cloudsoft AMP can deploy applications to [Kubernetes](http://kubernetes.io/) (k8s) clusters both provisioned by Cloudsoft AMP and set up manually.

Here is an example catalog item to add a Kubernetes endpoint to your catalog locations:

    brooklyn.catalog:
      id: my-kubernetes-cluster
      name: "My Kubernetes Cluster"
      itemType: location
      item:
        type: kubernetes
        brooklyn.config:
          endpoint: << endpoint >>
          identity: "guest"
          credential: "guest"
          image: "cloudsoft/centos:7"
          loginUser.password: "p4ssw0rd"

AMP Deploys to a Kubernetes cluster by modelling a `KubernetesPod` entity which is made up of multiple heterogeneous `DockerContainer` entities.

#### Plain-AMP blueprints

Standard AMP blueprints can be deployed within a K8s cluster, here's a simple example:

    location:
      << see above >>

    services:
    - type: org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess
      name: Simple Netcat Server

      brooklyn.config:
        provisioning.properties:
          inboundPorts: [22, 4321]

        launch.command: |
          yum install -y nc
          echo hello | nc -l 4321 &
          echo $! > $PID_FILE

For each entity AMP will create a [_Deployment_](http://kubernetes.io/docs/user-guide/deployments/).
This deployment contains a [_ReplicaSet_](http://kubernetes.io/docs/user-guide/replicasets/)
of replicas (defaulting to one) of a [_Pod_](http://kubernetes.io/docs/user-guide/pods/).
Each pod contains a single SSHable container based on the `cloudsoft/centos:7` image.

It will then install and launch the entity. Each `inboundPort` will be exposed as a
[_NodePort_](http://kubernetes.io/docs/user-guide/services/#type-nodeport) in a _Service_.

To explain the config options:

- **env**  The `cloudsoft/centos:7` image uses an environment variable named `CLOUDSOFT_ROOT_PASSWORD`
   to assign the SSH login user password. This must match the `loginUser.password` configuration on the location.
- **inboundPorts**  The set of ports that should be exposed by the service.

#### Docker Container based blueprints

Alternatively AMP can launch instances based on a `DockerContainer`, this means additional configuration such as custom docker images can be specified. Here's an example which sets up a [Wordpress](https://wordpress.org/) instance:

    location:
      << see above >>

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

#### Kubernetes location configuration

To configure the kubernetes location for different kubernetes setups the following configuration params are available.

- **caCertData** Data for CA certificate
- **caCertFile** URL of resource containing CA certificate data
- **clientCertData** Data for client certificate
- **clientCertFile** URL of resource containing client certificate data
- **clientKeyData** Data for client key
- **clientKeyFile** URL of resource containing client key data
- **clientKeyAlgo** Algorithm used for the client key
- **clientKeyPassphrase** Passphrase used for the client key
- **oauthToken** The OAuth token data for the current user
- **namespace** Namespace where resources will live; the default is 'amp'
- **namespace.create** Whether to create the namespace if it does not exist
  - **default** true
- **namespace.deleteEmpty** Whether to delete an empty namespace when releasing resources
  - **default** false
- **persistentVolumes** Set up persistent volumes.
- **deployment** Deployment where resources will live.
- **image** Docker image to be deployed into the pod
- **osFamily** OS family, e.g. CentOS, Ubuntu
- **osVersionRegex** Regular expression for the OS version to load
- **env** Environment variables to inject when starting the container
- **replicas** Number of replicas of the pod
  - **default** 1
- **secrets** Kubernetes secrets to be added to the pod
- **limits** Kubernetes resource limits
- **privileged** Whether Kubernetes should allow privileged containers
  - **default** false
- **loginUser** Override the user who logs in initially to perform setup
  - **default** root
- **loginUser.password** Custom password for the user who logs in initially
- **injectLoginCredential** Whether to inject login credentials (if null, will infer from image choice); ignored if explicit 'loginUser.password' supplied
