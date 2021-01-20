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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslToStringHelpers;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class DslDeferredFunctionCall extends BrooklynDslDeferredSupplier<Object> {
    private static final Logger log = LoggerFactory.getLogger(DslDeferredFunctionCall.class);
    private static final Set<Method> DEPRECATED_ACCESS_WARNINGS = Collections.newSetFromMap(new ConcurrentHashMap<Method, Boolean>());

    private static final long serialVersionUID = 3243262633795112155L;

    private Object object;
    private String fnName;
    private List<?> args;

    public DslDeferredFunctionCall(Object o, String fn, List<Object> args) {
        this.object = o;
        this.fnName = fn;
        this.args = args;
    }

    @Override @JsonIgnore
    public Maybe<Object> getImmediately() {
        return invokeOnDeferred(object, true);
    }

    private static String toStringF(String fnName, Object args) {
        return fnName + blankIfNull(args);
    }
    private static String blankIfNull(Object args) {
        if (args==null) return "";
        return args.toString();
    }
    
    @Override
    public Task<Object> newTask() {
        return Tasks.builder()
                .displayName("Deferred function call " + object + "." + toStringF(fnName, args))
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
        Maybe<?> resolvedMaybe = resolve(obj, immediate, true);
        if (resolvedMaybe.isPresent()) {
            Object instance = resolvedMaybe.get();

            if (instance == null) {
                throw new IllegalArgumentException("Deferred function call not found: " + 
                        object + " evaluates to null (wanting to call " + toStringF(fnName, args) + ")");
            }

            Maybe<Object> tentative = invokeOn(instance);
            if (tentative.isAbsent()) {
                return tentative;
            }
            return (Maybe) resolve(tentative.get(), immediate, false);
        } else {
            if (immediate) {
                return Maybe.absent(new ImmediateSupplier.ImmediateValueNotAvailableException("Could not evaluate immediately: " + obj));
            } else {
                return Maybe.absent(Maybe.getException(resolvedMaybe));
            }
        }
    }

    protected Maybe<Object> invokeOn(Object obj) {
        return invokeOn(obj, fnName, args);
    }

    protected static Maybe<Object> invokeOn(Object obj, String fnName, List<?> args) {
        return new Invoker(obj, fnName, args).invoke();
    }
    
    protected static class Invoker {
        final Object obj;
        final String fnName;
        final List<?> args;
        
        Maybe<Method> method;
        Object instance;
        List<?> instanceArgs;
        
        protected Invoker(Object obj, String fnName, List<?> args) {
            this.fnName = fnName;
            this.obj = obj;
            this.args = args;
        }
        
        protected Maybe<Object> invoke() {
            findMethod();
            
            if (method.isPresent()) {
                Method m = method.get();
    
                checkCallAllowed(m);
    
                try {
                    // Value is most likely another BrooklynDslDeferredSupplier - let the caller handle it,
                    return Maybe.of(Reflections.invokeMethodFromArgs(instance, m, instanceArgs));
                } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
                    // If the method is there but not executable for whatever reason fail with a fatal error, don't return an absent.
                    throw Exceptions.propagateAnnotated("Error invoking '"+toStringF(fnName, instanceArgs)+"' on '"+instance+"'", e);
                }
            } else {
                // could do deferred execution if an argument is a deferred supplier:
                // if we get a present from:
                // new Invoker(obj, fnName, replaceSuppliersWithNull(args)).findMethod()
                // then return a
                // new DslDeferredFunctionCall(...)
                
                return Maybe.absent(new IllegalArgumentException("No such function '"+fnName+"' taking args "+args+" (on "+obj+")"));
            }
        }
    
        protected void findMethod() {
            method = Reflections.getMethodFromArgs(obj, fnName, args);
            if (method.isPresent()) {
                this.instance = obj;
                this.instanceArgs = args;
                return ;
            }
                
            instance = BrooklynDslCommon.class;
            instanceArgs = ImmutableList.builder().add(obj).addAll(args).build();
            method = Reflections.getMethodFromArgs(instance, fnName, instanceArgs);
            if (method.isPresent()) return ;
    
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
                if (method.isPresent()) return ;
            }
            
            method = Maybe.absent();
        }
    }
    
    protected Maybe<?> resolve(Object object, boolean immediate, boolean dslFunctionSource) {
        ValueResolver<Object> r = Tasks.resolving(object, Object.class)
                .context(entity().getExecutionContext())
                .deep()
                .immediately(immediate);
        if (dslFunctionSource) {
            return r.iterator()
                    .nextOrLast(DslFunctionSource.class);
        } else {
            return r.getMaybe();
        }
    }

    private static void checkCallAllowed(Method m) {
        DslAccessible dslAccessible = m.getAnnotation(DslAccessible.class);
        boolean isAnnotationAllowed = dslAccessible != null;
        if (isAnnotationAllowed) return;

        // TODO white-list using brooklyn.properties (at runtime)

        Class<?> clazz = m.getDeclaringClass();
        Package whiteListPackage = BrooklynDslCommon.class.getPackage();
        boolean isPackageAllowed = (clazz.getPackage() != null && // Proxy objects don't have a package
                    clazz.getPackage().getName().startsWith(whiteListPackage.getName()));
        if (isPackageAllowed) {
            if (DEPRECATED_ACCESS_WARNINGS.add(m)) {
                log.warn("Deprecated since 0.11.0. The method '" + m.toString() + "' called by DSL should be white listed using the " + DslAccessible.class.getSimpleName() + " annotation. Support for DSL callable methods under the " + whiteListPackage + " will be fremoved in a future release.");
            }
            return;
        }

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
        // prefer the dsl set on us, if set
        if (dsl instanceof String && Strings.isNonBlank((String)dsl)) return (String)dsl;

        return DslToStringHelpers.fn(DslToStringHelpers.internal(object) + "." + fnName, args);
    }

}
