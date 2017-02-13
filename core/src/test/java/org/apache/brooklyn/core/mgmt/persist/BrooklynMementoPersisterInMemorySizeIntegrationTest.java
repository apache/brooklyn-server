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
package org.apache.brooklyn.core.mgmt.persist;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.persist.ListeningObjectStore.RecordingTransactionListener;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/** uses recorder to ensure not too much data is written */
@Test
public class BrooklynMementoPersisterInMemorySizeIntegrationTest extends BrooklynMementoPersisterTestFixture {

    // TODO If you decrease persistPeriod to 1ms (from 100ms) then
    //   SoftwareProcessPersisterInMemorySizeIntegrationTest.testPersistenceVolumeFastNoTrigger
    // will fail: "Phase one recorded 234239 bytes, 123 files".
    // It fails non-deterministically on Apache Jenkins (with 100ms persistPeriod) because that 
    // machine can be very slow sometimes.
    // Increased persistPeriod to 250ms to improve chances of it working on jenkins, but really we
    // need to revisit how this test is written!
    
    private static final Logger LOG = LoggerFactory.getLogger(BrooklynMementoPersisterInMemorySizeIntegrationTest.class);

    protected static int pass1MaxFiles = 30;
    protected static int pass1MaxKb = 30;
    protected static int pass2MaxFiles = 40;
    protected static int pass2MaxKb = 50;
    protected static int pass3MaxKb = 50;

    protected RecordingTransactionListener recorder;

    @Override
    protected ManagementContext newPersistingManagementContext() {
        recorder = new RecordingTransactionListener("in-mem-test-"+Identifiers.makeRandomId(4));
        return RebindTestUtils.managementContextBuilder(classLoader, 
            new ListeningObjectStore(new InMemoryObjectStore(), recorder))
            .persistPeriod(Duration.millis(250)).buildStarted();
    }
    
    @Test
    public void testPersistenceVolumeFast() throws Exception {
        doTestPersistenceVolume(false, true);
    }
    
    @Test(groups="Integration",invocationCount=20)
    public void testPersistenceVolumeFastManyTimes() throws Exception {
        doTestPersistenceVolume(false, true);
    }
    
    @Test(groups="Integration")
    public void testPersistenceVolumeWaiting() throws Exception {
        // by waiting we ensure there aren't extra writes going on
        doTestPersistenceVolume(true, true);
    }
    
    @Test
    public void testPersistenceVolumeFastNoTrigger() throws Exception {
        doTestPersistenceVolume(false, false);
    }
    
    @Test(groups="Integration",invocationCount=20)
    public void testPersistenceVolumeFastNoTriggerManyTimes() throws Exception {
        doTestPersistenceVolume(false, false);
    }
    
    protected void doTestPersistenceVolume(boolean forceDelay, boolean canTrigger) throws Exception {
        if (forceDelay) Time.sleep(Duration.FIVE_SECONDS);
        else recorder.blockUntilDataWrittenExceeds(512, Duration.FIVE_SECONDS);
        localManagementContext.getRebindManager().waitForPendingComplete(Duration.FIVE_SECONDS, canTrigger);
        
        long out1 = recorder.getBytesOut();
        int filesOut1 = recorder.getCountDataOut();
        LOG.info("Phase one recorded "+out1+" bytes, "+filesOut1+" files");
        Assert.assertTrue(out1>512, "should have written at least 0.5k, only wrote "+out1);
        Assert.assertTrue(out1<pass1MaxKb*1000, "should have written less than " + pass1MaxKb + "k, wrote "+out1);
        Assert.assertTrue(filesOut1<pass1MaxFiles, "should have written fewer than " + pass1MaxFiles + " files, wrote "+filesOut1);
        
        ((EntityInternal)app).sensors().set(TestEntity.NAME, "hello world");
        if (forceDelay) Time.sleep(Duration.FIVE_SECONDS);
        else recorder.blockUntilDataWrittenExceeds(out1+10, Duration.FIVE_SECONDS);
        localManagementContext.getRebindManager().waitForPendingComplete(Duration.FIVE_SECONDS, canTrigger);
        
        long out2 = recorder.getBytesOut();
        int filesOut2 = recorder.getCountDataOut();
        LOG.info("Phase two recorded "+out2+" bytes, "+filesOut2+" files");
        Assert.assertTrue(out2-out1>10, "should have written more data");
        Assert.assertTrue(filesOut2>filesOut1, "should have written more files");
        
        Assert.assertTrue(out2<pass2MaxKb*1000, "should have written less than " + pass2MaxKb + "k, wrote "+out2);
        Assert.assertTrue(filesOut2<pass2MaxFiles, "should have written fewer than " + pass2MaxFiles + " files, wrote "+filesOut2);
        
        ((EntityInternal)entity).sensors().set(TestEntity.NAME, Identifiers.makeRandomId(pass3MaxKb));
        if (forceDelay) Time.sleep(Duration.FIVE_SECONDS);
        else recorder.blockUntilDataWrittenExceeds(out2+pass3MaxKb, Duration.FIVE_SECONDS);
        localManagementContext.getRebindManager().waitForPendingComplete(Duration.FIVE_SECONDS, canTrigger);

        long out3 = recorder.getBytesOut();
        int filesOut3 = recorder.getCountDataOut();
        LOG.info("Phase three recorded "+out3+" bytes, "+filesOut3+" files");
        Assert.assertTrue(out3-out2 > pass3MaxKb, "should have written " + pass3MaxKb + "k more data, only wrote "+out3+" compared with "+out2);
        Assert.assertTrue(filesOut3>filesOut2, "should have written more files");
    }
    
}
