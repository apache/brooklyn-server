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
package org.apache.brooklyn.core.mgmt.classloading;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.classloading.BrooklynClassLoadingContextSequential;
import org.apache.brooklyn.core.mgmt.classloading.ClassLoaderFromBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.ClassLoadingContext;

/** Provides a stack where {@link ClassLoadingContext} instances can be pushed and popped,
 * with the most recently pushed one in effect at any given time.
 * <p>
 * This is useful when traversing a tree some of whose elements may bring custom search paths,
 * and the worker wants to have a simple view of the loader to use at any point in time.
 * For example XStream keeps a class-loader in effect but when deserializing things 
 * some of those things may define bundles to use. */
public class ClassLoaderFromStackOfBrooklynClassLoadingContext extends ClassLoader {
    
    private final Stack<BrooklynClassLoadingContext> contexts = new Stack<BrooklynClassLoadingContext>();
    private final Stack<ClassLoader> cls = new Stack<ClassLoader>();
    private final AtomicReference<Thread> lockOwner = new AtomicReference<Thread>();
    private ManagementContext mgmt;
    private ClassLoader currentClassLoader;
    private AtomicReference<ClassLoaderUtils> currentLoader = new AtomicReference<>();
    private int lockCount;
    
    public ClassLoaderFromStackOfBrooklynClassLoadingContext(ClassLoader classLoader) {
        setCurrentClassLoader(classLoader);
    }
    
    public void setManagementContext(ManagementContext mgmt) {
        this.mgmt = checkNotNull(mgmt, "mgmt");
        currentLoader.set(new ClassLoaderUtils(currentClassLoader, mgmt));
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        return currentLoader.get().loadClass(name);
    }
    
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return findClass(name);
    }

    /** Must be accompanied by a corresponding {@link #popClassLoadingContext()} when finished. */
    @SuppressWarnings("deprecation")
    public void pushClassLoadingContext(BrooklynClassLoadingContext clcNew) {
        acquireLock();
        BrooklynClassLoadingContext oldClc;
        if (!contexts.isEmpty()) {
            oldClc = contexts.peek();
        } else {
            // TODO xml serializers using this should take a BCLC instead of a CL
            oldClc = JavaBrooklynClassLoadingContext.create(mgmt, getCurrentClassLoader());
        }
        BrooklynClassLoadingContextSequential clcMerged = new BrooklynClassLoadingContextSequential(mgmt, oldClc, clcNew);
        ClassLoader newCL = ClassLoaderFromBrooklynClassLoadingContext.of(clcMerged);
        contexts.push(clcMerged);
        cls.push(getCurrentClassLoader());
        setCurrentClassLoader(newCL);
    }

    public void popClassLoadingContext() {
        synchronized (lockOwner) {
            releaseXstreamLock();
            setCurrentClassLoader(cls.pop());
            contexts.pop();
        }
    }
    
    private ClassLoader getCurrentClassLoader() {
        return currentClassLoader;
    }
    
    private void setCurrentClassLoader(ClassLoader classLoader) {
        currentClassLoader = checkNotNull(classLoader);
        currentLoader.set(new ClassLoaderUtils(currentClassLoader, mgmt));
    }
    
    protected void acquireLock() {
        synchronized (lockOwner) {
            while (true) {
                if (lockOwner.compareAndSet(null, Thread.currentThread()) || 
                    Thread.currentThread().equals( lockOwner.get() )) {
                    break;
                }
                try {
                    lockOwner.wait(1000);
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }
            lockCount++;
        }
    }

    protected void releaseXstreamLock() {
        synchronized (lockOwner) {
            if (lockCount<=0) {
                throw new IllegalStateException("not locked");
            }
            if (--lockCount == 0) {
                if (!lockOwner.compareAndSet(Thread.currentThread(), null)) {
                    Thread oldOwner = lockOwner.getAndSet(null);
                    throw new IllegalStateException("locked by "+oldOwner+" but unlock attempt by "+Thread.currentThread());
                }
                lockOwner.notifyAll();
            }
        }
    }
}