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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.PropertyAccess;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * TODO parser return objects that we CAN recognise as being interpreted to form the thing below.
 *  ie currently it return eg QuotedString(s) and FunctionWithArg(f, args). it will now also return PropertyAccess(target, index).
 * then BrooklynDslInterpreter looks at those tokens to create the right BrooklynDslDeferredSupplier instances.
 * so it needs also to recognise PropertyAccess and create instances of the class below.
 *
 * thing below might need a bit more eg a nice toString (which should produce re-interpretable input),
 * but is roughly
 * <code>[index]</code> is tried:
 * * an argument to a `get(index)` method (works for lists and maps: index or key)
 * * a bean property (if index is a string, look for method called getIndex() or a field called  'index'
 * * a config key, if the target is Configurable , ie getConfig(index) or config().get(index)
 */
public class DslDeferredPropertyAccess extends BrooklynDslDeferredSupplier {
    private static final Logger LOG = LoggerFactory.getLogger(PropertyAccess.class);

    BrooklynDslDeferredSupplier target;
    Object index;

    public DslDeferredPropertyAccess() {}

    public DslDeferredPropertyAccess(BrooklynDslDeferredSupplier target, Object index) {
        this.target = target;
        this.index = index;
    }

    protected Object getIndexOnTarget(Object targetEvaluated) {
        Object targetResult = target.get();
        if (targetResult==null) {
            throw new IllegalStateException("Source was null when evaluating property '"+index+"' in "+this);
        }

        Exception failure = null;
        try {
            if (targetResult instanceof List) {
                return ((List<?>) targetResult).get(Integer.parseInt(index.toString()));
            } else if (targetResult instanceof Map) {  // maybe add configurable here
                return ((Map<?, ?>) targetResult).get(index);
            } else if (index instanceof String) {
                final String propName = (String) index;
                if (targetResult instanceof Configurable) {
                    return ((Configurable) targetResult).getConfig(ConfigKeys.newConfigKey(Object.class, propName));
                }
                try {
                    Field field = targetResult.getClass().getDeclaredField(propName);
                    // field.setAccessible(true); // private not allowed, for security
                    return field.get(targetResult);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    Exceptions.propagateIfFatal(e);

                    if (LOG.isTraceEnabled())
                        LOG.trace("Field " + propName + " not found on type " + targetResult.getClass(), e);
                }

                final String getter = "get" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
                try {
                    Method method = targetResult.getClass().getDeclaredMethod(getter);
                    // method.setAccessible(true); // private not allowed, for security
                    return method.invoke(targetResult);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);

                    if (LOG.isTraceEnabled())
                        LOG.trace("Problem invoking method " + getter + " on type " + targetResult.getClass(), e);
                }

                throw new IllegalStateException("Neither field and using getter)");
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            failure = e;
        }

        throw new IllegalStateException("Unable to access property '"+index+"' on '"+targetResult+"' in "+this, failure);
    }

    @Override
    public Task newTask() {
        return Tasks.builder().displayName("Retrieving property or index " + index).tag("TRANSIENT").dynamic(false)
                .body(new Callable<Object>() {
            public Object call() throws Exception {
                ValueResolver<Object> r = Tasks.resolving(target, Object.class).context(entity().getExecutionContext()).deep().immediately(false);
                return getIndexOnTarget(r);
            }
        }).build();
    }

    @Override  @JsonIgnore
    public Maybe getImmediately() {
        return target.getImmediately().transformNow(this::getIndexOnTarget);
    }

    @Override
    public String toString() {
        // prefer the dsl set on us, if set
        if (dsl instanceof String && Strings.isNonBlank((String)dsl)) return (String)dsl;

        return target + "[" +
                (index instanceof Integer ? index : StringEscapes.JavaStringEscapes.wrapJavaString("" + index)) + "]";
    }
}
