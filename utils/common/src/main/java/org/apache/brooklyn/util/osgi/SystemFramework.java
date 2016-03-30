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
}
