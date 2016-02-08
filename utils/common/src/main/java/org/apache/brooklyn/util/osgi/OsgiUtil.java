/*
 * Copyright 2016 The Apache Software Foundation.
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

import java.util.ArrayList;
import java.util.List;
import org.osgi.framework.FrameworkUtil;

/**
 * osgi utils
 */
public class OsgiUtil {

    /** Tells if Brooklyn is running in an OSGi environment or not. */
    public static boolean isBrooklynInsideFramework() {
        return FrameworkUtil.getBundle(OsgiUtil.class) != null;
    }

    /** keep a list of threads to be run when an osgi bundle is deactivated */
    public static void addShutdownHook(Runnable shutdownHook) {
        shutdownHooks.add(shutdownHook);
    }

    /** run all previously registered shutdown hooks */
    public static void shutdown() {
        for (Runnable r : shutdownHooks)
            r.run();
    }

    private static List<Runnable> shutdownHooks = new ArrayList<Runnable>();
}
