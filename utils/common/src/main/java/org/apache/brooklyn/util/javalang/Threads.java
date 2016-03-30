/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.util.javalang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class Threads {

    private static final Logger log = LoggerFactory.getLogger(Threads.class);
    private static final Collection<Thread> hooks = new ArrayList<>();
    private static final AtomicBoolean shutdownHookRegistered = new AtomicBoolean();

    public static Thread addShutdownHook(final Runnable task) {
        Thread t = new Thread("shutdownHookThread") {
            @Override
            public void run() {
                try {
                    task.run();
                } catch (Exception e) {
                    log.error("Failed to execute shutdown hook", e);
                }
            }
        };
        synchronized (hooks) {
            hooks.add(t);
        }

        registerShutdownHookOnceInClassicMode();
        return t;
    }

    private static void registerShutdownHookOnceInClassicMode() {
        if (!isRunningInOsgi() && shutdownHookRegistered.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    runShutdownHooks();
                }
            });
        }
    }

    private static boolean isRunningInOsgi() {
        return FrameworkUtil.getBundle(Threads.class) != null;
    }

    public static boolean removeShutdownHook(Thread hook) {
        synchronized (hooks) {
            return hooks.remove(hook);
        }
    }

    public static void runShutdownHooks() {
        while(hasMoreTasks()) {
            Collection<Thread> localHooks;
            synchronized (Threads.hooks) {
                localHooks = ImmutableList.copyOf(hooks);
                hooks.clear();
            }
            for (Thread t : localHooks) {
                try {
                    t.start();
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    log.error("Failed to execute shutdown hook for thread " + t, e);
                }
            }
            for (Thread t : localHooks) {
                try {
                    t.join();
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                }
            }
        }
    }

    private static boolean hasMoreTasks() {
        synchronized (hooks) {
            return !hooks.isEmpty();
        }
    }
}
