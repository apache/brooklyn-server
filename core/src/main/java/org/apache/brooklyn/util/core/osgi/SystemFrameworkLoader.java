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

import org.apache.brooklyn.api.framework.FrameworkLookup;
import org.apache.brooklyn.util.osgi.SystemFramework;

public class SystemFrameworkLoader {
    private static SystemFramework framework;
    public static SystemFramework get() {
        if (framework == null) {
            return createFramework();
        } else {
            return framework;
        }
    }
    private synchronized static SystemFramework createFramework() {
        if (framework == null) {
            framework = FrameworkLookup.lookup(SystemFramework.class).get();
        }
        return framework;
    }
}
