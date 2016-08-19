package io.cloudsoft.amp.container.kubernetes.location;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locations starting with the given prefix (@code "kubernetes") will use this resolver, to instantiate
 * a {@link KubernetesLocation}.
 * 
 * We ensure that config will be picked up from brooklyn.properties using the appropriate precedence:
 * <ol>
 *   <li>named location config
 *   <li>Prefix {@code brooklyn.location.kubernetes.}
 *   <li>Prefix {@code brooklyn.kubernetes.}
 * </ol>
 */
public class KubernetesLocationResolver extends AbstractLocationResolver implements LocationResolver {

    public static final Logger log = LoggerFactory.getLogger(KubernetesLocationResolver.class);

    public static final String PREFIX = "kubernetes";

    @Override
    public boolean isEnabled() {
        return LocationConfigUtils.isResolverPrefixEnabled(managementContext, getPrefix());
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    protected Class<? extends Location> getLocationType() {
        return KubernetesLocation.class;
    }

    @Override
    protected SpecParser getSpecParser() {
        return new SpecParser(getPrefix()).setExampleUsage("\"kubernetes\"");
    }

}
