---
section: OpenShift Location
section_type: inline
section_position: 5
---

### OpenShift Location

Cloudsoft AMP can deploy applications to Red Hat [OpenShift](https://www.openshift.com/) clusters.

Here is an example catalog item to add an OpenShift endpoint to your catalog locations:

    brooklyn.catalog:
      id: my-openshift-cluster
      name: "My Openshift Cluster"
      itemType: location
      item:
        type: openshift
        brooklyn.config:
          endpoint: << endpoint >>
          caCertData: |
            << Generated Ca Cert (see below) >>
          clientCertData: |
            << Generated Cert (see below) >>
          clientKeyData: |
            << Generated client key (see below) >>
          namespace: << project name >>
          privileged: true

* Endpoint

The endpoint key is the https URL of your OpenShift master. AMP connects to this to provision applications on the
cluster.

* OpenShift Authorization

The `caCertData`, `clientCertData` and `clientKeyData` are the credentials for your OpenShift cluster. Note that
they can also be given as paths to files using the keys `caCertFile`, `clientCertFile` and `clientKeyFile`. See the
[OpenShift documentation](https://docs.openshift.com/enterprise/3.1/install_config/certificate_customization.html) for
more detail on the content of these.

An alternate way of authorizing your OpenShift is using OAuth. To obtain the token required you must use the `oc` command-line tool,
first to log in to OpenShift and then to display the token value, using the `whoami` command:

    oc login << endpoint >>
    oc whoami -t

Which will output the token to the command line:

    mzUTj0JmWDYLSspumvW5B74rn8geKd6Qll11IPkaqeE

This is then set as the `oauthToken` field in the location:

    brooklyn.catalog:
      id: my-openshift-cluster
      name: "My Openshift Cluster"
      itemType: location
      item:
        type: openshift
        brooklyn.config:
          endpoint: << endpoint >>
          oauthToken: mzUTj0JmWDYLSspumvW5B74rn8geKd6Qll11IPkaqeE
          namespace: << project name >>
          privileged: true

* Namespace

The `namespace` key relates to the project in which your AMP managed applications will deploy. If no project exists,
you will first need to log into your OpenShift cluster and create a project. The `namespace` key should then contain
the ID of this.

#### OpenShift Configuration

AMP requires that you configure your OpenShift instance with the following options to allow it to fully provision and manage
applications.

* Container Privileges

Depending on how the images you wish to use have been created, you may need to set up accounts and permissions to allow them to run.
Containers written for the OpenShift platform follow certain rules such as logging to the console to allow centralized log
management or avoiding the `root` user since the platform will use an arbitrary user id. For applications that follow these rules
the default `restricted` security constraints are all that is needed. When using images from Docker Hub, or the `cloudsoft/centos:7`
image used by native AMP entities, privileged access must be enabled. This can be done by creating a new user for your application,
and assigning it the `privileged` or `anyuid` security constraints as described in the [documentation](https://docs.openshift.org/latest/admin_guide/manage_scc.html).

Alternatively, for development systems where security is not an issue, you can edit the `restricted` constraint directly, and
set the configuration option `allowPrivilegedContainer` to `true` and `runAsUser` to have type `RunAsAny`. This can be configured
using the [oc command](https://docs.openshift.org/latest/cli_reference/index.html)  to edit the cluster configuration:

    oc login << endpoint >>
    sudo oc edit scc restricted

#### Plain-AMP blueprints

Standard AMP blueprints can be deployed within an OpenShift cluster, here's a simple example:

    location:
      << see above >>

    services:
      - type: org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess
        name: "Simple Netcat Server"
        brooklyn.config:
          provisioning.properties:
            inboundPorts: [ 22, 4321 ]
            env:
              CLOUDSOFT_ROOT_PASSWORD: "p4ssw0rd"
          launch.command: |
            yum install -y nc
            echo hello | nc -l 4321 &
            echo $! > $PID_FILE

For each entity AMP will create a [_DeploymentConfig_](https://docs.openshift.org/latest/architecture/core_concepts/deployments.html#deployments-and-deployment-configurations).
This deployment configuration contains a [_ReplicationController_](https://kubernetes.io/docs/user-guide/replication-controller/)
with replicas (defaulting to one) of a [_Pod_](http://kubernetes.io/docs/user-guide/pods/).
Each pod contains a single SSHable container based on the `cloudsoft/centos:7` image.

It will then install and launch the entity. Each `inboundPort` will be exposed as a
[_NodePort_](http://kubernetes.io/docs/user-guide/services/#type-nodeport) in a _Service_.

The config options in the `provisioning.properties` section allow the location to be further customized for each entity, as follows:

- **env**  The `cloudsoft/centos:7` image uses an environment variable named `CLOUDSOFT_ROOT_PASSWORD`
   to assign the SSH login user password. This must match the `loginUser.password` configuration on the location.
- **inboundPorts**  The set of ports that should be exposed by the service.

Note the use of **deployment** in the `provisioning.properties` configuration, to set the hostname of the MySQL container to allow the Wordpress Apache server to connect to it.

#### DockerContainer based blueprints

Alternatively AMP can launch instances based on a `DockerContainer`, this means additional configuration such as custom docker images can be specified. Here's an example which sets up a [Wordpress](https://wordpress.org/) instance:

    location:
      << see above >>

    services:
      - type: org.apache.brooklyn.container.entity.kubernetes.KubernetesPod
        brooklyn.children:
          - type: org.apache.brooklyn.container.entity.docker.DockerContainer
            id: wordpress-mysql
            name: "MySQL"
            brooklyn.config:
              docker.container.imageName: mysql:5.6
              docker.container.inboundPorts: [ "3306" ]
              docker.container.environment:
                MYSQL_ROOT_PASSWORD: "password"
              provisioning.properties:
                deployment: wordpress-mysql
          - type: org.apache.brooklyn.container.entity.docker.DockerContainer
            id: wordpress
            name: "Wordpress"
            brooklyn.config:
              docker.container.imageName: wordpress:4-apache
              docker.container.inboundPorts: [ "80" ]
              docker.container.environment:
                WORDPRESS_DB_HOST: "wordpress-mysql"
                WORDPRESS_DB_PASSWORD: "password"

The `DockerContainer` entities each create their own _DeploymentConfig_, _ReplicationController_ and _Pod_ entities,
in the same way as the standard AMP blueprint entities above. Each container entity can be further configured using the following options:

- **docker.container.imageName** The Docker image to use for the container
- **docker.container.inboundPorts** The set of ports on the container that should be exposed
- **docker.container.environment** A map of environment variables for the container

#### OpenShift location configuration

The OpenShift location uses the same configuration options as the [Kubernetes](../kubernetes-location/README.md)
location, with the following exception:

- **namespace** Also refers to the OpenShift project the Pod will be started in.
