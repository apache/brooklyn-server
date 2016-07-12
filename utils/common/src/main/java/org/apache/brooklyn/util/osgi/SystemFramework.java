/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.osgi;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;

public interface SystemFramework {
    /** 
     * Provides an OSGI framework.
     *
     * When running inside an OSGi container, the container framework is returned.
     * When running standalone a new Apache Felix container is created.
     */
    public Framework getFramework(String felixCacheDir, boolean clean);

    /**
     * Stops/ungets the OSGi framework, depending on the environment.
     */
    public void ungetFramework(Framework framework);

    /**
     * Whether the bundle is exporting classes from the application class path.
     */
    public boolean isSystemBundle(Bundle bundle);

    /**
     * Loads a class from the passed bundle. The embedded environment has some special
     * requirements for loading classes so the implementation will abstract those.
     */
    public <T> Class<T> loadClassFromBundle(String type, Bundle b) throws ClassNotFoundException;

    /**
     * Loads a resource from the passed bundle. The embedded environment has some special
     * requirements for loading resources so the implementation will abstract those.
     */
    public <T> URL getResourceFromBundle(String type, Bundle b);

    /**
     * Loads resources from the passed bundle. The embedded environment has some special
     * requirements for loading resources so the implementation will abstract those.
     * As a single bundle is passed the result will contain at most one element, or be null if
     * none is found.
     */
    public <T> Enumeration<URL> getResourcesFromBundle(String type, Bundle b) throws IOException;
}
