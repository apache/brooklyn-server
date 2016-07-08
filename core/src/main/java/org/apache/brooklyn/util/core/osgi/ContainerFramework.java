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
package org.apache.brooklyn.util.core.osgi;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import org.apache.brooklyn.util.osgi.SystemFramework;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.launch.Framework;

public class ContainerFramework implements SystemFramework {

    @Override
    public Framework getFramework(String felixCacheDir, boolean clean) {
        final Bundle bundle = FrameworkUtil.getBundle(Osgis.class);
        return (Framework) bundle.getBundleContext().getBundle(0);
    }

    @Override
    public void ungetFramework(Framework framework) {
    }

    @Override
    public boolean isSystemBundle(Bundle bundle) {
        return false;
    }

    @Override
    public <T> Class<T> loadClassFromBundle(String type, Bundle b) throws ClassNotFoundException {
        @SuppressWarnings("unchecked")
        Class<T> c = (Class<T>)b.loadClass(type);
        return c;
    }

    @Override
    public <T> URL getResourceFromBundle(String type, Bundle b) {
        return b.getResource(type);
    }

    @Override
    public <T> Enumeration<URL> getResourcesFromBundle(String type, Bundle b) throws IOException {
        return b.getResources(type);
    }

}
