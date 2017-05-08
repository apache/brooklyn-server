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
package org.apache.brooklyn.core.effector.script;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;

public final class ScriptEffector<T> extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(ScriptEffector.class);

    @SetFromFlag("lang")
    public static final ConfigKey<String> EFFECTOR_SCRIPT_LANGUAGE = ConfigKeys.newStringConfigKey(
            "script.language", "The scripting language the effector is written in", "JavaScript");

    @SetFromFlag("content")
    public static final ConfigKey<String> EFFECTOR_SCRIPT_CONTENT = ConfigKeys.newStringConfigKey(
            "script.content", "The script code to evaluate for the effector");

    @SetFromFlag("script")
    public static final ConfigKey<String> EFFECTOR_SCRIPT_URL = ConfigKeys.newStringConfigKey(
            "script.url", "A URL for the script to evaluate for the effector");

    @SetFromFlag("return")
    public static final ConfigKey<String> EFFECTOR_SCRIPT_RETURN_VAR = ConfigKeys.newStringConfigKey(
            "script.return.var", "An optional script variable to return from the effector");

    @SetFromFlag("type")
    public static final ConfigKey<Class<?>> EFFECTOR_SCRIPT_RETURN_TYPE = ConfigKeys.newConfigKey(
            new TypeToken<Class<?>>() { },
            "script.return.type", "The type of the return value from the effector", Object.class);

    public ScriptEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public ScriptEffector(Map<String,?> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Object> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Object> eff = AddEffector.newEffectorBuilder(Object.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    protected static class Body extends EffectorBody<Object> {
        private final Effector<?> effector;
        private final String script;
        private final String language;
        private final String returnVar;
        private final Class<?> returnType;

        public Body(Effector<?> eff, ConfigBag params) {
            this.effector = eff;
            String content = params.get(EFFECTOR_SCRIPT_CONTENT);
            String url = params.get(EFFECTOR_SCRIPT_URL);
            if (Strings.isNonBlank(content)) {
                this.script = content;
            } else {
                Preconditions.checkNotNull(url, "Script URL or content must be specified");
                this.script = ResourceUtils.create().getResourceAsString(url);
            }
            this.language = params.get(EFFECTOR_SCRIPT_LANGUAGE);
            this.returnVar = params.get(EFFECTOR_SCRIPT_RETURN_VAR);
            this.returnType = params.get(EFFECTOR_SCRIPT_RETURN_TYPE);

            // Check the language is supported by trying to create a ScriptEngine
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName(language);
            if (engine == null) {
                String message = "Script language not supported: " + language;
                LOG.warn(message);
                if (LOG.isDebugEnabled()) {
                    List<String> supported = getScriptLanguages(manager);
                    LOG.debug("Supported languages for scripts: " + Joiner.on(',').join(supported));
                }
                throw new IllegalStateException(message);
            }
        }

        @Override
        public Object call(ConfigBag params) {
            ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader scriptLoader = new ScriptClassLoader(parentLoader, "org.apache.brooklyn.*");
            Thread.currentThread().setContextClassLoader(scriptLoader);
            ScriptEngineManager manager = new ScriptEngineManager(scriptLoader);
            ScriptEngine engine = manager.getEngineByName(language);
            ScriptContext defaultContext = engine.getContext();
            ScriptContext context = new SimpleScriptContext();
            context.setBindings(defaultContext.getBindings(ScriptContext.ENGINE_SCOPE), ScriptContext.ENGINE_SCOPE); 

            // Store effector arguments as engine scope bindings
            for (ParameterType<?> param: effector.getParameters()) {
                context.setAttribute(param.getName(), params.get(Effectors.asConfigKey(param)), ScriptContext.ENGINE_SCOPE);
            }

            // Add object bindings
            context.setAttribute("entity", entity(), ScriptContext.ENGINE_SCOPE);
            context.setAttribute("managementContext", entity().getManagementContext(), ScriptContext.ENGINE_SCOPE);
            context.setAttribute("task", Tasks.current(), ScriptContext.ENGINE_SCOPE);
            context.setAttribute("config", params.getAllConfig(), ScriptContext.ENGINE_SCOPE);

            try {
                // Execute the script and return result
                Object result = engine.eval(script, context);
                if (Strings.isNonBlank(returnVar)) {
                    result = context.getAttribute(returnVar, ScriptContext.ENGINE_SCOPE);
                }
                String resultClass = result.getClass().getName();

                // Unwrap JavaScript return values to underlying Java object
                if (resultClass.endsWith("ScriptObjectMirror")) {
                    // JDK 8.0 Nashorn interpreter
                    Class<?> scriptUtils = Class.forName("jdk.nashorn.api.scripting.ScriptUtils");
                    Optional<Object> unwrapped = Reflections.invokeMethodWithArgs(scriptUtils, "unwrap", ImmutableList.of(result));
                    result = unwrapped.get();
                }
                if (resultClass.endsWith("NativeJavaObject")) {
                    // JDK 7.0 Rhino interpreter
                    Optional<Object> unwrapped = Reflections.invokeMethodWithArgs(result, "unwrap", ImmutableList.of());
                    result = unwrapped.get();
                }
                return TypeCoercions.coerce(result, returnType);
            } catch (ScriptException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
                throw Exceptions.propagate(e);
            } finally {
                Thread.currentThread().setContextClassLoader(parentLoader);
            }
        }

        /** Returns a list of all JSR-223 script language names. */
        private List<String> getScriptLanguages(ScriptEngineManager manager) {
            List<String> languages = Lists.newArrayList();
            List<ScriptEngineFactory> factories = manager.getEngineFactories();
            for (ScriptEngineFactory factory : factories) {
                List<String> engNames = factory.getNames();
                for (String name : engNames) {
                    languages.add(name);
                }
            }
            return ImmutableList.copyOf(languages);
        }
    }
}
