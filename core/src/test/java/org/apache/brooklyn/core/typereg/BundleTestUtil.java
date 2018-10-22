package org.apache.brooklyn.core.typereg;

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.mockito.Mockito;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

class BundleTestUtil {

    static Bundle newMockBundle(Map<String, String> rawHeaders) {
        return newMockBundle(VersionedName.fromString("do.no.care:1.2.3"), rawHeaders);
    }

    static Bundle newMockBundle(VersionedName name) {
        return newMockBundle(name, ImmutableMap.of());
    }

    static Bundle newMockBundle(VersionedName name, Map<String, String> rawHeaders) {
        Dictionary<String, String> headers = new Hashtable<>(rawHeaders);
        Bundle result;
        try {
            result = Mockito.mock(Bundle.class);
        } catch (Exception e) {
            throw new IllegalStateException("Java too old.  There is a bug in really early java 1.8.0 "
                    + "that causes mocks to fail, and has probably caused this.", e);
        }
        Mockito.when(result.getHeaders()).thenReturn(headers);
        Mockito.when(result.getSymbolicName()).thenReturn(name.getSymbolicName());
        Mockito.when(result.getVersion()).thenReturn(Version.valueOf(name.getOsgiVersionString()));
        return result;
    }


}
