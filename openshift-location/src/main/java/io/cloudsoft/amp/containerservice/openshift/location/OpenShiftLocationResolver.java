package io.cloudsoft.amp.containerservice.openshift.location;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locations starting with the given prefix (@code "openshift") will use this resolver, to instantiate
 * a {@link OpenShiftLocation}.
 * 
 * We ensure that config will be picked up from brooklyn.properties using the appropriate precedence:
 * <ol>
 *   <li>named location config
 *   <li>Prefix {@code brooklyn.location.openshift.}
 *   <li>Prefix {@code brooklyn.openshift.}
 * </ol>
 */
public class OpenShiftLocationResolver extends AbstractLocationResolver implements LocationResolver {

    public static final Logger log = LoggerFactory.getLogger(OpenShiftLocationResolver.class);

    public static final String PREFIX = "openshift";

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
        return OpenShiftLocation.class;
    }

    @Override
    protected SpecParser getSpecParser() {
        return new SpecParser(getPrefix()).setExampleUsage("\"openshift\"");
    }

}
