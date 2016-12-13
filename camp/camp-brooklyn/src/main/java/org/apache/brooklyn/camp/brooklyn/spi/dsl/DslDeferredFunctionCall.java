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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class DslDeferredFunctionCall extends BrooklynDslDeferredSupplier<Object> {

    private static final long serialVersionUID = 3243262633795112155L;

    private Object object;
    private String fnName;
    private List<?> args;

    public DslDeferredFunctionCall(Object o, String fn, List<Object> args) {
        this.object = o;
        this.fnName = fn;
        this.args = args;
    }

    @Override
    public Maybe<Object> getImmediately() {
        return invokeOnDeferred(object, true);
    }

    @Override
    public Task<Object> newTask() {
        return Tasks.builder()
                .displayName("Deferred function call " + object + "." + fnName + "(" + toString(args) + ")")
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .dynamic(false)
                .body(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        return invokeOnDeferred(object, false).get();
                    }

                }).build();
    }

    protected Maybe<Object> invokeOnDeferred(Object obj, boolean immediate) {
        Maybe<?> resolvedMaybe = resolve(obj, immediate);
        if (resolvedMaybe.isPresent()) {
            Object instance = resolvedMaybe.get();

            if (instance == null) {
                throw new IllegalArgumentException("Deferred function call, " + object + 
                        " evaluates to null (when calling " + fnName + "(" + toString(args) + "))");
            }

            return invokeOn(instance);
        } else {
            if (immediate) {
                return Maybe.absent("Could not evaluate immediately " + obj);
            } else {
                return Maybe.absent(Maybe.getException(resolvedMaybe));
            }
        }
    }

    protected Maybe<Object> invokeOn(Object obj) {
        return invokeOn(obj, fnName, args);
    }

    protected static Maybe<Object> invokeOn(Object obj, String fnName, List<?> args) {
        Object instance = obj;
        List<?> instanceArgs = args;
        Maybe<Method> method = Reflections.getMethodFromArgs(instance, fnName, instanceArgs);

        if (method.isAbsent()) {
            instance = BrooklynDslCommon.class;
            instanceArgs = ImmutableList.builder().add(obj).addAll(args).build();
            method = Reflections.getMethodFromArgs(instance, fnName, instanceArgs);
        }

        if (method.isAbsent()) {
            Maybe<?> facade;
            try {
                facade = Reflections.invokeMethodFromArgs(BrooklynDslCommon.DslFacades.class, "wrap", ImmutableList.of(obj));
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
                facade = Maybe.absent();
            }

            if (facade.isPresent()) {
                instance = facade.get();
                instanceArgs = args;
                method = Reflections.getMethodFromArgs(instance, fnName, instanceArgs);
            }
        }

        if (method.isPresent()) {
            Method m = method.get();

            checkCallAllowed(m);

            try {
                // Value is most likely another BrooklynDslDeferredSupplier - let the caller handle it,
                return Maybe.of(Reflections.invokeMethodFromArgs(instance, m, instanceArgs));
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
                // If the method is there but not executable for whatever reason fail with a fatal error, don't return an absent.
                throw Exceptions.propagate(new InvocationTargetException(e, "Error invoking '"+fnName+"("+toString(instanceArgs)+")' on '"+instance+"'"));
            }
        } else {
            return Maybe.absent(new IllegalArgumentException("No such function '"+fnName+"("+toString(args)+")' on "+obj));
        }
    }

    protected Maybe<?> resolve(Object object, boolean immediate) {
        if (object instanceof DslFunctionSource || object == null) {
            return Maybe.of(object);
        }

        Maybe<?> resultMaybe = Tasks.resolving(object, Object.class)
                .context(((EntityInternal)entity()).getExecutionContext())
                .deep(true)
                .immediately(immediate)
                .recursive(false)
                .getMaybe();

        if (resultMaybe.isPresent()) {
            // No nice way to figure out whether the object is deferred. Try to resolve it
            // until it matches the input value as a poor man's replacement.
            Object result = resultMaybe.get();
            if (result == object) {
                return resultMaybe;
            } else {
                return resolve(result, immediate);
            }
        } else {
            return resultMaybe;
        }
    }

    private static void checkCallAllowed(Method m) {
        Class<?> clazz = m.getDeclaringClass();
        if (clazz.getPackage() == null || // Proxy objects don't have a package
                !(clazz.getPackage().getName().startsWith(BrooklynDslCommon.class.getPackage().getName())))
            throw new IllegalArgumentException("Not permitted to invoke function on '"+clazz+"' (outside allowed package scope)");
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(object, fnName, args);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DslDeferredFunctionCall that = DslDeferredFunctionCall.class.cast(obj);
        return Objects.equal(this.object, that.object) &&
                Objects.equal(this.fnName, that.fnName) &&
                Objects.equal(this.args, that.args);
    }

    @Override
    public String toString() {
        return object + "." + fnName + "(" + toString(args) + ")";
    }

    private static String toString(List<?> args) {
        if (args == null) return "";
        return Joiner.on(", ").join(args);
    }
}
