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
package org.apache.brooklyn.location.winrm;

import com.google.common.base.Suppliers;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.util.stream.KnownSizeInputStream;
import org.apache.brooklyn.util.stream.ReaderInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;

public class WinRmPutTaskFactory extends WinRmPutTaskStub implements TaskFactory<WinRmPutTaskWrapper> {

    private static final Logger log = LoggerFactory.getLogger(WinRmPutTaskFactory.class);

    private boolean dirty = false;

    /** constructor where machine will be added later */
    public WinRmPutTaskFactory(String remoteFile) {
        remoteFile(remoteFile);
    }

    /** convenience constructor to supply machine immediately */
    public WinRmPutTaskFactory(WinRmMachineLocation machine, String remoteFile) {
        machine(machine);
        remoteFile(remoteFile);
    }

    protected WinRmPutTaskFactory self() { return this; }

    protected void markDirty() {
        dirty = true;
    }

    public WinRmPutTaskFactory machine(WinRmMachineLocation machine) {
        markDirty();
        this.machine = machine;
        return self();
    }

    public WinRmPutTaskFactory remoteFile(String remoteFile) {
        this.remoteFile = remoteFile;
        return self();
    }

    public WinRmPutTaskFactory summary(String summary) {
        markDirty();
        this.summary = summary;
        return self();
    }

    public WinRmPutTaskFactory contents(String contents) {
        markDirty();
        this.contents = Suppliers.ofInstance(KnownSizeInputStream.of(contents));
        return self();
    }

    public WinRmPutTaskFactory contents(byte[] contents) {
        markDirty();
        this.contents = Suppliers.ofInstance(KnownSizeInputStream.of(contents));
        return self();
    }

    public WinRmPutTaskFactory contents(InputStream stream) {
        markDirty();
        this.contents = Suppliers.ofInstance(stream);
        return self();
    }

    public WinRmPutTaskFactory contents(Reader reader) {
        markDirty();
        this.contents = Suppliers.ofInstance(new ReaderInputStream(reader));
        return self();
    }

    public WinRmPutTaskFactory allowFailure() {
        markDirty();
        allowFailure = true;
        return self();
    }

    public WinRmPutTaskFactory createDirectory() {
        markDirty();
        createDirectory = true;
        return self();
    }

    @Override
    public WinRmPutTaskWrapper newTask() {
        dirty = false;
        return new WinRmPutTaskWrapper(this);
    }

    @Override
    protected void finalize() throws Throwable {
        // help let people know of API usage error
        if (dirty)
            log.warn("Task "+this+" was modified but modification was never used");
        super.finalize();
    }
}
