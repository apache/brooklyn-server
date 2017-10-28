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
package org.apache.brooklyn.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import org.apache.brooklyn.util.exceptions.Exceptions;

public class Locks {

    public static <T> T withLock(Lock lock, Callable<T> body) {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
        try {
            return body.call();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            lock.unlock();
        }       
    }
    
    public static void withLock(Lock lock, Runnable body) {
        withLock(lock, () -> { body.run(); return null; } );
    }
    
}
