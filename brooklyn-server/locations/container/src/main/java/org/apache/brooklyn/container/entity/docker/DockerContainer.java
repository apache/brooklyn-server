package org.apache.brooklyn.container.entity.docker;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

/**
 * The DockerContainer type is for easily deploying any docker image from the
 * image repository set on the target swarm or docker-engine based location
 * <p>
 * Example YAML is shown below. Note the different types of the {@code env}
 * key in the location config and the {@code docker.container.environment}
 * key on the entity. The entity environment variables will override any
 * environment configured on the location. To specify environment variables
 * that will be set when executing SSH commands against the container you
 * should use the {@link SoftwareProcess#SHELL_ENVIRONMENT shell.env} key.
 * <p>
 * <pre>{@code location:
 *   docker:
 *     endpoint: "https://52.29.59.193:3376"
 *     identity: "~/.certs/cert.pem"
 *     credential: "~/.certs/key.pem"
 *     templateOptions:
 *       networkMode: "brooklyn"
 *     env:
 *       - "HTTP_CONFIG_ROOT=/var/httpd"
 *       - "USE_DEFAULTS=true"
 * services:
 *   - type: org.apache.brooklyn.container.entity.docker.DockerContainer
 *     brooklyn.config:
 *       docker.container.imageName: "apache/httpd:latest"
 *       docker.container.disableSsh: true
 *       docker.container.inboundPorts:
 *         - "8080-8081"
 *       docker.container.environment:
 *         ENABLE_JMX: false
 *         ENABLE_SHUTDOWN: false
 * }</pre>
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

    @SuppressWarnings("serial")
    ConfigKey<Iterable<String>> INBOUND_TCP_PORTS =
            ConfigKeys.newConfigKey(
                    new TypeToken<Iterable<String>>() {},
                    "docker.container.inboundPorts",
                    "List of ports, that the docker image opens, to be made public");

    MapConfigKey<Object> CONTAINER_ENVIRONMENT = new MapConfigKey.Builder<Object>(Object.class, "docker.container.environment")
            .description("Environment variables to set on container startup")
            .defaultValue(ImmutableMap.<String, Object>of())
            .typeInheritance(BasicConfigInheritance.DEEP_MERGE)
            .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED_ELSE_DEEP_MERGE)
            .build();
}
