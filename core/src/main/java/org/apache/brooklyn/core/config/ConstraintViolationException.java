/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.core.config;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.text.Strings;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link ConstraintViolationException} indicates one or more problems applying
 * values for {@link org.apache.brooklyn.config.ConfigKey ConfigKeys} when creating
 * a {@link org.apache.brooklyn.api.objs.BrooklynObject}.
 */
public class ConstraintViolationException extends UserFacingException {
    private static final long serialVersionUID = -6719912119648996815L;

    public ConstraintViolationException(String message) {
        super(message);
    }

    public ConstraintViolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConstraintViolationException(Throwable cause) {
        super(cause);
    }

    Object context;
    ConfigKey<?> key;
    Object value;

    public static ConstraintViolationException of(Throwable e, Object context, ConfigKey<?> key, Object value) {
        if (e instanceof ConstraintViolationException) {
            // don't wrap if it's already the right type

            ConstraintViolationException cve = (ConstraintViolationException)e;
            // only set value if key or context supplied; otherwise caller might have deliberately omitted it
            if (cve.value==null) {
                if (cve.context!=null || cve.key!=null) {
                    cve.value = value;
                }
            }

            if (cve.context==null) cve.context = context;
            if (cve.key==null) cve.key = key;

            if (Objects.equals(context, cve.context) && Objects.equals(key, cve.key)) {
                return cve;
            }
        }

        return new ConstraintViolationException(e).with(context, key, value);
    }

    public Object getContext() {
        return context;
    }
    public ConfigKey<?> getKey() {
        return key;
    }
    public Object getValue() {
        return value;
    }

    public ConstraintViolationException with(Object context, ConfigKey<?> key, Object value) {
        setContext(context);
        setKey(key);
        setValue(value);
        return this;
    }
    public void setContext(Object context) {
        this.context = context;
    }
    public void setKey(ConfigKey<?> key) {
        this.key = key;
    }
    public void setValue(Object value) {
        this.value = value;
    }

    public String getSuppliedMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        String supplied = getSuppliedMessage();
        String cause = Exceptions.collapseText(getCause());
        if (cause.contains(supplied)) {
            supplied = cause;
        } else {
            supplied = supplied + "; caused by: "+cause;
        }

        List<String> output = MutableList.of();
        output.add("Invalid value");

        if (key!=null && !Strings.containsLiteralAsWord(supplied, key.getName())) {
            output.add(" for key '"+key.getName()+"'");
        }

        if (context!=null && !Strings.containsLiteralAsWord(supplied, ""+context)) {
            output.add(" on "+context);
        }

        if (key!=null && !Strings.containsLiteralAsWord(supplied, ""+value)) {
            output.add(" " + (value==null ? "(value is null)" : ": '"+value+"'"));
        }

        if (supplied!=null) {
            if (output.size()==1) {
                output = MutableList.of(supplied);
            } else {
                output.add("; ");
                output.add(supplied);
            }
        }

        return Strings.join(output, "");
    }

    public static class CompoundConstraintViolationException extends ConstraintViolationException {
        private final Collection<Throwable> allViolations;

        protected CompoundConstraintViolationException(String message, Collection<Throwable> causes) {
            super(message, causes.iterator().next());
            allViolations = causes;
        }

        public static CompoundConstraintViolationException of(Object source, Map<ConfigKey<?>, Throwable> violations) {
            Set<ConstraintViolationException> cves = getCVEs(violations.values());

            StringBuilder message = new StringBuilder();
            if (source!=null) {
                message.append("Error configuring ")
                        .append(source)
                        .append(": ");
            }

            if (violations.size() == 1) {
                message.append(Exceptions.collapseText(Iterables.getOnlyElement(violations.values())));
            } else {
                Set<ConfigKey<?>> keys = MutableSet.copyOf(getConfigKeys(cves)).putAll(violations.keySet());

                if (keys.size() > 1) {
                    message.append("Invalid values for " + keys.stream().map(ConfigKey::getName).collect(Collectors.toSet()) + ": ");
                }
                if (!violations.isEmpty()) {
                    message.append(violations.values().stream().map(Throwable::toString).collect(Collectors.toSet()));
                }
            }

            CompoundConstraintViolationException result = new CompoundConstraintViolationException(message.toString(), violations.values());
            result.setContext(source);

            if (source instanceof String || source==null) {
                // prefer setting the BrooklynObject itself, if it is unique
                Set<Object> contexts = getContexts(cves);
                if (contexts.size()==1) result.setContext( Iterables.getOnlyElement(contexts) );
            }

            return result;
        }

        private static Set<ConstraintViolationException> getCVEs(Collection<? extends Throwable> violations) {
            return (Set) violations.stream().filter(t -> t instanceof ConstraintViolationException).collect(Collectors.toSet());
        }

        private static Set<ConfigKey<?>> getConfigKeys(Collection<? extends Throwable> violations) {
            return getCVEs(violations).stream().map(ConstraintViolationException::getKey).collect(Collectors.<ConfigKey<?>>toSet());
        }

        private static Set<Object> getContexts(Collection<? extends Throwable> violations) {
            return getCVEs(violations).stream().map(ConstraintViolationException::getContext).collect(Collectors.toSet());
        }


        public Set<ConstraintViolationException> getCVEs() {
            return getCVEs(getAllViolations());
        }

        public Set<ConfigKey<?>> getConfigKeys() {
            return getConfigKeys(getAllViolations());
        }

        public Set<Object> getContexts() {
            return getContexts(getAllViolations());
        }

        public Collection<Throwable> getAllViolations() {
            return allViolations;
        }

        @Override
        public String toString() {
            // suppress causes as they are included
            return getClass().getName()+": "+getMessage();
        }

    }
}
