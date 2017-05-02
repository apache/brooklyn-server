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
package org.apache.brooklyn.util.groovy;

import java.util.concurrent.Callable;

import groovy.lang.Closure;

/**
 * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
 */
@Deprecated
public class FromCallableClosure<T> extends Closure<T> {
    private static final long serialVersionUID = 1L;
    private Callable<T> job;

    public FromCallableClosure(Class<GroovyJavaMethods> owner, Callable<T> job) {
        super(owner, owner);
        this.job = job;
    }

    public T doCall() throws Exception {
        return job.call();
    }

}
