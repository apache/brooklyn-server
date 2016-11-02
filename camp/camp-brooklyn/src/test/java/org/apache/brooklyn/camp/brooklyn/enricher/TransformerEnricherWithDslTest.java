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
package org.apache.brooklyn.camp.brooklyn.enricher;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.enricher.stock.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TransformerEnricherWithDslTest extends BrooklynAppUnitTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(TransformerEnricherWithDslTest.class);

    int START_PORT = 10000;

    @Test
    // See https://issues.apache.org/jira/browse/BROOKLYN-356
    public void testTransformerResolvesResolvableValues() {
        LOG.info("Starting 100 iterations of testTransformerResolvesResolvableValues");
        testTransformerResolvesResolvableValues(START_PORT, 100);
    }

    @Test(groups={"Integration"}, invocationCount=10)
    // See https://issues.apache.org/jira/browse/BROOKLYN-356
    public void testTransformerResolvesResolvableValuesIntegration() {
        LOG.info("Starting 1000 iterations of testTransformerResolvesResolvableValues");
        testTransformerResolvesResolvableValues(START_PORT, 1000);
    }

    private void testTransformerResolvesResolvableValues(int portStart, int portCount) {
        // Note: The test gets progressively slower with iterations, probably due to the GC triggering much more frequently.
        //       There's no memory leak, but doesn't seem right to be putting so much pressure on the GC with such a simple test.
        AttributeSensor<Integer> sourceSensor = Sensors.newIntegerSensor("port");
        AttributeSensor<String> targetSensor = Sensors.newStringSensor("port.transformed");
        app.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.SOURCE_SENSOR, sourceSensor)
                .configure(Transformer.TARGET_SENSOR, targetSensor)
                .configure(Transformer.TARGET_VALUE,
                        // Can only use the inner-most sensor, but including the
                        // wrapping formatStrings amplifies the resolving effort, making
                        // a bug more probable to manifest.
                        BrooklynDslCommon.formatString("%s",
                                BrooklynDslCommon.formatString("%d",
                                        BrooklynDslCommon.attributeWhenReady("port")))));

        int failures = 0;
        for (int port = portStart; port < portStart + portCount; port++) {
            app.sensors().set(sourceSensor, port);
            try {
                EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, Integer.toString(port));
            } catch (Exception e) {
                failures++;
                LOG.warn("Assertion failed, port=" + port + ", transformed sensor is " + app.sensors().get(targetSensor), e);
            }
        }

        assertEquals(failures, 0, failures + " assertion failures while transforming sensor; see logs for detailed errors");
    }

}
