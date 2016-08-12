package io.cloudsoft.amp.containerservice.dockercontainer;


import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;

import com.google.common.reflect.TypeToken;

/**
 * The DockerContainer type is for easily deploying any docker image from the
 * image repository set on the target swarm or docker-engine based location
 *
 * Example YAML
 *
 * location:
 *   docker:
 *     endpoint: https://52.29.59.193:3376
 *     identity: /Users/duncangrant/.certs/cert.pem
 *     credential: /Users/duncangrant/.certs/key.pem
 *     templateOptions:
 *       networkMode: "brooklyn"
 * services:
 * - type: io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer
 *   brooklyn.config:
 *     docker.container.imageName: httpd
 *     docker.container.disableSsh: true
 *     docker.container.inboundPorts:
 *       - "8080-8081"
 */
@ImplementedBy(DockerContainerImpl.class)
public interface DockerContainer extends SoftwareProcess {

   ConfigKey<Boolean> DISABLE_SSH =
           ConfigKeys.newBooleanConfigKey(
                   "docker.container.disableSsh",
                   "Skip checks such as ssh for when docker image doesn't allow ssh",
                   Boolean.TRUE);

   ConfigKey<String> IMAGE_NAME =
           ConfigKeys.newStringConfigKey(
                   "docker.container.imageName",
                   "Image name to pull from docker hub");

   ConfigKey<Iterable<String>> INBOUND_TCP_PORTS =
           ConfigKeys.newConfigKey(
                   new TypeToken<Iterable<String>>() { },
                   "docker.container.inboundPorts",
                   "List of ports, that the docker image opens, to be made public");
}
