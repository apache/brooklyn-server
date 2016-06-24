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
package org.apache.brooklyn.util.guava;

import org.apache.brooklyn.util.exceptions.Exceptions.CollapseTextSupplier;
import org.apache.brooklyn.util.javalang.Reflections;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class AnyExceptionSupplier<T extends Throwable> implements Supplier<T> {

    protected final Class<? extends T> type;
    protected final Supplier<String> message;
    protected final Throwable cause;
    
    public AnyExceptionSupplier(Class<T> type) { this(type, (Supplier<String>)null, null); }
    public AnyExceptionSupplier(Class<T> type, String message) { this(type, message, null); }
    public AnyExceptionSupplier(Class<T> type, Throwable cause) { this(type, new CollapseTextSupplier(cause), cause); }
    public AnyExceptionSupplier(Class<T> type, String message, Throwable cause) { this(type, Suppliers.ofInstance(message), cause); } 
    public AnyExceptionSupplier(Class<? extends T> type, Supplier<String> message, Throwable cause) {
        this.type = type;
        this.message = message;
        this.cause = cause;
    }

    @Override
    public T get() {
        String msg = message==null ? null : message.get();
        Maybe<T> result = Maybe.absent();
        if (result.isAbsent() && msg==null && cause==null) result = Reflections.invokeConstructorFromArgs(type);
        if (result.isAbsent() && cause==null) result = Reflections.invokeConstructorFromArgs(type, msg);
        if (result.isAbsent() && msg==null) result = Reflections.invokeConstructorFromArgs(type, cause);
        if (result.isAbsent()) result = Reflections.invokeConstructorFromArgs(type, msg, cause);
        if (result.isAbsent()) {
            throw new IllegalStateException("Cannot create desired "+type+" (missing constructor)",
                new IllegalStateException(message==null ? null : message.get(), cause));
        }
        return result.get();
    }
    
    public Throwable getCause() {
        return cause;
    }
    
    public Supplier<String> getMessageSupplier() {
        return message;
    }

}
