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
package org.apache.brooklyn.rt.felix;

import org.apache.brooklyn.util.osgi.SystemFramework;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;

public class EmbeddedFramework implements SystemFramework {

    @Override
    public Framework getFramework(String felixCacheDir, boolean clean) {
        return EmbeddedFelixFramework.newFrameworkStarted(felixCacheDir, clean, null);
    }

    @Override
    public void ungetFramework(Framework framework) {
        EmbeddedFelixFramework.stopFramework(framework);
    }

    @Override
    public boolean isSystemBundle(Bundle bundle) {
        return EmbeddedFelixFramework.isExtensionBundle(bundle);
    }

    @Override
    public <T> Class<T> loadClassFromBundle(String type, Bundle b) throws ClassNotFoundException {
        Class<T> clazz;
        if (EmbeddedFelixFramework.isExtensionBundle(b)) {
            @SuppressWarnings("unchecked")
            Class<T> c = (Class<T>)Class.forName(type);
            clazz = c;
        } else {
            @SuppressWarnings("unchecked")
            Class<T> c = (Class<T>)b.loadClass(type);
            clazz = c;
        }
        return clazz;
    }

}
