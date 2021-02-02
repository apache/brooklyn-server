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

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.internal.winrm.winrm4j.ErrorXmlWriter;
import org.apache.brooklyn.util.core.internal.winrm.winrm4j.PrettyXmlWriter;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.ssh.internal.AbstractSshExecTaskFactory;
import org.apache.brooklyn.util.core.task.ssh.internal.PlainSshExecTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;

import java.util.List;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.commons.lang3.tuple.Pair;

public class PlainWinRmExecTaskFactory<RET> extends AbstractSshExecTaskFactory<PlainSshExecTaskFactory<RET>,RET> {

    public static final String STREAM_XML_OUT = "xmlout";

    /** constructor where machine will be added later */
    public PlainWinRmExecTaskFactory(String ...commands) {
        super(commands);
    }

    /** convenience constructor to supply machine immediately */
    public PlainWinRmExecTaskFactory(WinRmMachineLocation machine, String ...commands) {
        this(commands);
        machine(machine);
    }

    /** Constructor where machine will be added later */
    public PlainWinRmExecTaskFactory(List<String> commands) {
        this(commands.toArray(new String[commands.size()]));
    }

    /** Convenience constructor to supply machine immediately */
    public PlainWinRmExecTaskFactory(WinRmMachineLocation machine, List<String> commands) {
        this(machine, commands.toArray(new String[commands.size()]));
    }

    @Override
    public <T2> PlainWinRmExecTaskFactory<T2> returning(ScriptReturnType type) {
        return (PlainWinRmExecTaskFactory<T2>) super.<T2>returning(type);
    }

    @Override
    public <RET2> PlainWinRmExecTaskFactory<RET2> returning(Function<ProcessTaskWrapper<?>, RET2> resultTransformation) {
        return (PlainWinRmExecTaskFactory<RET2>) super.returning(resultTransformation);
    }

    @Override
    public PlainWinRmExecTaskFactory<Boolean> returningIsExitCodeZero() {
        return (PlainWinRmExecTaskFactory<Boolean>) super.returningIsExitCodeZero();
    }

    @Override
    public PlainWinRmExecTaskFactory<String> requiringZeroAndReturningStdout() {
        return (PlainWinRmExecTaskFactory<String>) super.requiringZeroAndReturningStdout();
    }

    /** In WinRM we sometimes get huge XML output from powershell, in the stream powershell says is `stderr`.
     * This seems to contravene the docs which say only Write-Error should come back in that stream.
     * But when that does happen, stderr is sometimes unusable. So we make a new stream for the big XML,
     * and we filter the error output into stderr.
     * <p>
     * Note this does some simple auto-detection so if the stream seems not to be xml we write the same data to both.
     * */
    @Override
    protected Std2x2StreamProvider getRichStreamProvider(TaskBuilder<?> tb) {
        return newStreamProviderForWindowsXml(tb);
    }

    @Beta
    public static Std2x2StreamProvider newStreamProviderForWindowsXml(TaskBuilder<?> tb) {
        Std2x2StreamProvider r = new Std2x2StreamProvider();
        r.stdoutForWriting = r.stdoutForReading = new ByteArrayOutputStream();
        tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDOUT, r.stdoutForReading));

        ByteArrayOutputStream prettyXmlOut = new ByteArrayOutputStream();
        ByteArrayOutputStream errorFilteredOut = new ByteArrayOutputStream();
        TeeOutputStream rawxml = new TeeOutputStream(
                new WriterOutputStream(new PrettyXmlWriter(new OutputStreamWriter(prettyXmlOut))),
                new WriterOutputStream(new ErrorXmlWriter(new OutputStreamWriter(errorFilteredOut))) );

        tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDERR, errorFilteredOut));
        tb.tag(BrooklynTaskTags.tagForStreamSoft(STREAM_XML_OUT, prettyXmlOut));

        r.stderrForReading = errorFilteredOut;
        r.stderrForWriting = rawxml;

        return r;
    }

}


