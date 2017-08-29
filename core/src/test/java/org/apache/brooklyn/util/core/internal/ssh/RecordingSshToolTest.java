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
package org.apache.brooklyn.util.core.internal.ssh;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RecordingSshToolTest extends BrooklynMgmtUnitTestSupport {

    private SshMachineLocation machine;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
        machine = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "1.2.3.4")
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()));
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testCustomOneOffResponse() throws Exception {
        RecordingSshTool.setCustomOneOffResponse(".*mycmd.*", new CustomResponse(1, "mystdout", "mystderr"));
        ExecResult result1 = execScript(machine, "mycmd");
        ExecResult result2 = execScript(machine, "mycmd");
        assertEquals(result1, new ExecResult(1, "mystdout", "mystderr"));
        assertEquals(result2, new ExecResult(0, "", ""));
    }
    
    private ExecResult execScript(SshMachineLocation machine, String cmd) {
        OutputStream outStream = new ByteArrayOutputStream();
        OutputStream errStream = new ByteArrayOutputStream();
        int exitCode = machine.execScript(ImmutableMap.of("out", outStream, "err", errStream), "mysummary", ImmutableList.of(cmd));
        String outString = outStream.toString();
        String errString = errStream.toString();
        return new ExecResult(exitCode, outString, errString);
    }
    
    static class ExecResult {
        final int exitCode;
        final String out;
        final String err;
        
        ExecResult(int exitCode, String out, String err) {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ExecResult)) return false;
            ExecResult o = (ExecResult) obj;
            return (exitCode == o.exitCode) && Objects.equal(out, o.out) && Objects.equal(err, o.err);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(exitCode, out, err);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("exitCode", exitCode)
                    .add("out", out)
                    .add("err", err)
                    .toString();
        }
    }
}
