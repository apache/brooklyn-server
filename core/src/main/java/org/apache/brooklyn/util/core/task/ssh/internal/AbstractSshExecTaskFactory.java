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
package org.apache.brooklyn.util.core.task.ssh.internal;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.core.task.system.internal.AbstractProcessTaskFactory;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.io.output.WriterOutputStream;

// cannot be (cleanly) instantiated due to nested generic self-referential type; however trivial subclasses do allow it 
public abstract class AbstractSshExecTaskFactory<T extends AbstractProcessTaskFactory<T,RET>,RET> extends AbstractProcessTaskFactory<T,RET> {
    
    /** constructor where machine will be added later */
    public AbstractSshExecTaskFactory(String ...commands) {
        super(commands);
    }

    /** convenience constructor to supply machine immediately */
    public AbstractSshExecTaskFactory(MachineLocation machine, String ...commands) {
        this(commands);
        machine(machine);
    }

    protected abstract String taskTypeShortName();

    @Override
    public ProcessTaskWrapper<RET> newTask() {
        dirty = false;
        return new ProcessTaskWrapper<RET>(this) {
            protected Std2x2StreamProvider richStreamProvider = null;

            @Override
            protected void run(ConfigBag config) {
                Preconditions.checkNotNull(getMachine(), "machine");
                if (Boolean.FALSE.equals(this.runAsScript)) {
                    this.exitCode = getMachine().execCommands(config.getAllConfig(), getSummary(), getCommands(true), shellEnvironment);
                } else { // runScript = null or TRUE
                    this.exitCode = getMachine().execScript(config.getAllConfig(), getSummary(), getCommands(true), shellEnvironment);
                }
            }
            @Override
            protected String taskTypeShortName() { return AbstractSshExecTaskFactory.this.taskTypeShortName(); }

            @Override
            protected void initStreams(TaskBuilder<Object> tb) {
                richStreamProvider = getRichStreamProvider(tb);
                if (richStreamProvider==null) {
                    super.initStreams(tb);
                } else {
                    super.initStreams(richStreamProvider);
                }
            }

            @Override
            protected ByteArrayOutputStream stderrForReading() {
            if (richStreamProvider!=null) return richStreamProvider.stderrForReading;
                return super.stderrForReading();
            }

            @Override
            protected OutputStream stderrForWriting() {
                if (richStreamProvider!=null) return richStreamProvider.stderrForWriting;
                return super.stderrForWriting();
            }

            @Override
            protected ByteArrayOutputStream stdoutForReading() {
                if (richStreamProvider!=null) return richStreamProvider.stdoutForReading;
                return super.stdoutForReading();
            }

            @Override
            protected OutputStream stdoutForWriting() {
                if (richStreamProvider!=null) return richStreamProvider.stdoutForWriting;
                return super.stdoutForWriting();
            }
        };
    }

    protected Std2x2StreamProvider getRichStreamProvider(TaskBuilder<?> tb) {
        return null;
    }

    @Beta
    public static class Std2x2StreamProvider {
        public ByteArrayOutputStream stdoutForReading;
        public ByteArrayOutputStream stderrForReading;
        public OutputStream stdoutForWriting;
        public OutputStream stderrForWriting;

        // also see WinRm XML variant which returns this class - newStreamProviderForWindowsXml
        public static Std2x2StreamProvider newDefault(TaskBuilder<?> tb) {
            Std2x2StreamProvider r = new Std2x2StreamProvider();
            r.stdoutForWriting = r.stdoutForReading = new ByteArrayOutputStream();
            r.stderrForWriting = r.stderrForReading = new ByteArrayOutputStream();
            tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDOUT, r.stdoutForReading));
            tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDERR, r.stderrForReading));
            return r;
        }
    }

}
