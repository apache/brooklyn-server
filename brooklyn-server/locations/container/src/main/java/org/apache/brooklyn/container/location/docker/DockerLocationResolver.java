package org.apache.brooklyn.container.location.docker;

import java.util.Map;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.core.location.LocationPropertiesFromBrooklynProperties;
import org.apache.brooklyn.location.jclouds.JcloudsPropertiesFromBrooklynProperties;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locations starting with the given prefix (@code "docker") will use this resolver, to instantiate
 * a {@link DockerJcloudsLocation}.
 * 
 * We ensure that config will be picked up from brooklyn.properties using the appropriate precedence:
 * <ol>
 *   <li>named location config
 *   <li>Prefix {@code brooklyn.location.docker.}
 *   <li>Prefix {@code brooklyn.jclouds.docker.}
 *   <li>Prefix {@code brooklyn.jclouds.}
 * </ol>
 */
public class DockerLocationResolver extends AbstractLocationResolver implements LocationResolver {

    public static final Logger log = LoggerFactory.getLogger(DockerLocationResolver.class);
    
    public static final String PREFIX = "docker";

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
        return DockerJcloudsLocation.class;
    }

    @Override
    protected SpecParser getSpecParser() {
        return new AbstractLocationResolver.SpecParser(getPrefix()).setExampleUsage("\"docker\"");
    }

    @Override
    protected Map<String, Object> getFilteredLocationProperties(String provider, String namedLocation, Map<String, ?> prioritisedProperties, Map<String, ?> globalProperties) {
        Map<String, Object> dockerConf = new LocationPropertiesFromBrooklynProperties().getLocationProperties(getPrefix(), namedLocation, globalProperties);
        
        Object providerInConf = dockerConf.get("provider");
        if (providerInConf != null && !provider.equals(providerInConf)) {
            throw new IllegalArgumentException(provider+" location configured with provider '"+providerInConf+"', but must be blank or '"+provider+"'");
        }

        String providerOrApi = "docker";
        String regionName = (String) prioritisedProperties.get("region");
        Map<String, Object> jcloudsConf = new JcloudsPropertiesFromBrooklynProperties().getJcloudsProperties(providerOrApi, regionName, namedLocation, globalProperties);
        return MutableMap.<String, Object>builder()
                .putAll(jcloudsConf)
                .putAll(dockerConf)
                .put("provider", providerOrApi)
                .build();
    }
}
