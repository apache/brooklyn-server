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
package org.apache.brooklyn.core.mgmt.rebind;

import com.google.common.io.Files;
import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;

import java.io.File;

public abstract class RebindAbstractCommandFeedTest extends RebindTestFixtureWithApp {

    @Override
    protected TestApplication rebind() throws Exception {
        RebindExceptionHandler exceptionHandler = RebindExceptionHandlerImpl.builder()
                .danglingRefFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .rebindFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .addConfigFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .addPolicyFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .loadPolicyFailureMode(RebindManager.RebindFailureMode.FAIL_AT_END)
                .build();
        return super.rebind(RebindOptions.create().exceptionHandler(exceptionHandler));
    }

    protected void addMemento(BrooklynObjectType type, String label, String id) throws Exception {
        String mementoFilename = label+"-"+id;
        String memento = Streams.readFullyString(getClass().getResourceAsStream(mementoFilename));

        File persistedFile = getPersistanceFile(type, id);
        Files.write(memento.getBytes(), persistedFile);
    }

    protected File getPersistanceFile(BrooklynObjectType type, String id) {
        String dir;
        switch (type) {
            case FEED: dir = "feeds"; break;
            default: throw new UnsupportedOperationException("type="+type);
        }
        return new File(mementoDir, Os.mergePaths(dir, id));
    }
}
