/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.getAssertions;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * {@inheritDoc}
 */
public class TestHttpCallImpl extends TargetableTestComponentImpl implements TestHttpCall {

    private static final Logger LOG = LoggerFactory.getLogger(TestHttpCallImpl.class);

    /**
     * {@inheritDoc}
     */
    public void start(Collection<? extends Location> locations) {
        String url = null;
        
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);

        try {
            url = getRequiredConfig(TARGET_URL);
            final List<Map<String, Object>> assertions = getAssertions(this, ASSERTIONS);
            final Duration timeout = getConfig(TIMEOUT);
            final HttpAssertionTarget target = getRequiredConfig(ASSERTION_TARGET);
            if (!getChildren().isEmpty()) {
                throw new RuntimeException(String.format("The entity [%s] cannot have child entities", getClass().getName()));
            }
            
            doRequestAndCheckAssertions(ImmutableMap.of("timeout", timeout), assertions, target, url);
            setUpAndRunState(true, Lifecycle.RUNNING);

        } catch (Throwable t) {
            if (url != null) {
                LOG.info("{} Url [{}] test failed (rethrowing)", this, url);
            } else {
                LOG.info("{} Url test failed (no url; rethrowing)", this);
            }
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    private void doRequestAndCheckAssertions(Map<String, Duration> flags, List<Map<String, Object>> assertions,
                                             HttpAssertionTarget target, final String url) {
        switch (target) {
            case body:
                Supplier<String> getBody = new Supplier<String>() {
                    @Override
                    public String get() {
                        return HttpTool.getContent(url);
                    }
                };
                TestFrameworkAssertions.checkAssertions(flags, assertions, target.toString(), getBody);
                break;
            case status:
                Supplier<Integer> getStatusCode = new Supplier<Integer>() {
                    @Override
                    public Integer get() {
                        try {
                            return HttpTool.getHttpStatusCode(url);
                        } catch (Exception e) {
                            LOG.info("HTTP call to [{}] failed due to [{}]", url, e.getMessage());
                            throw Exceptions.propagate(e);
                        }
                    }
                };
                TestFrameworkAssertions.checkAssertions(flags, assertions, target.toString(), getStatusCode);
                break;
            default:
                throw new RuntimeException("Unexpected assertion target (" + target + ")");
        }
    }

    /**
     * {@inheritDoc}
     */
    public void stop() {
        setUpAndRunState(false, Lifecycle.STOPPED);
    }

    /**
     * {@inheritDoc}
     */
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }

}