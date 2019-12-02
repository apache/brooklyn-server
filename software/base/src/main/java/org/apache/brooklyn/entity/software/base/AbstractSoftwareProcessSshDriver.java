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
package org.apache.brooklyn.entity.software.base;

import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.drivers.downloads.DownloadResolver;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.effector.EffectorTasks;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.feed.ConfigToAttributes;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.entity.software.base.lifecycle.NaiveScriptRunner;
import org.apache.brooklyn.entity.software.base.lifecycle.ScriptHelper;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.mutex.WithMutexes;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.stream.KnownSizeInputStream;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An abstract SSH implementation of the {@link AbstractSoftwareProcessDriver}.
 *
 * This provides conveniences for clients implementing the install/customize/launch/isRunning/stop lifecycle
 * over SSH.  These conveniences include checking whether software is already installed,
 * creating/using a PID file for some operations, and reading ssh-specific config from the entity
 * to override/augment ssh flags on the session.
 */
public abstract class AbstractSoftwareProcessSshDriver extends AbstractSoftwareProcessDriver implements NaiveScriptRunner {

    public static final Logger log = LoggerFactory.getLogger(AbstractSoftwareProcessSshDriver.class);
    public static final Logger logSsh = LoggerFactory.getLogger(BrooklynLogging.SSH_IO);

    protected volatile DownloadResolver resolver;

    @Override
    public void prepare() {
        // Check if we should create a download resolver?
        String downloadUrl = getEntity().config().get(SoftwareProcess.DOWNLOAD_URL);
        if (Strings.isNonEmpty(downloadUrl)) {
            resolver = Entities.newDownloader(this);
            String formatString = getArchiveNameFormat();
            if (Strings.isNonEmpty(formatString)) {
                setExpandedInstallDir(Os.mergePaths(getInstallDir(), resolver.getUnpackedDirectoryName(String.format(formatString, getVersion()))));
            } else {
                setExpandedInstallDir(getInstallDir());
            }
        }
    }

    /** include this flag in newScript creation to prevent entity-level flags from being included;
     * any SSH-specific flags passed to newScript override flags from the entity,
     * and flags from the entity override flags on the location
     * (where there aren't conflicts, flags from all three are used however) */
    public static final String IGNORE_ENTITY_SSH_FLAGS = SshEffectorTasks.IGNORE_ENTITY_SSH_FLAGS.getName();

    public AbstractSoftwareProcessSshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);

        // FIXME this assumes we own the location, and causes warnings about configuring location after deployment;
        // better would be to wrap the ssh-execution-provider to supply these flags
        if (getSshFlags()!=null && !getSshFlags().isEmpty())
            machine.configure(getSshFlags());

        // ensure these are set using the routines below, not a global ConfigToAttributes.apply()
        getInstallDir();
        getRunDir();
    }

    /** returns location (tighten type, since we know it is an ssh machine location here) */
    @Override
    public SshMachineLocation getLocation() {
        return (SshMachineLocation) super.getLocation();
    }

    public SshMachineLocation getMachine() { return getLocation(); }
    public String getHostname() { return entity.getAttribute(Attributes.HOSTNAME); }
    public String getAddress() { return entity.getAttribute(Attributes.ADDRESS); }
    public String getSubnetHostname() { return entity.getAttribute(Attributes.SUBNET_HOSTNAME); }
    public String getSubnetAddress() { return entity.getAttribute(Attributes.SUBNET_ADDRESS); }

    protected Map<String, Object> getSshFlags() {
        return SshEffectorTasks.getSshFlags(getEntity(), getMachine());
    }

    /**
     * @deprecated since 0.10.0 This method will become private in a future release.
     */
    @Deprecated
    public int execute(String command, String summaryForLogging) {
        return execute(ImmutableList.of(command), summaryForLogging);
    }

    /**
     * @deprecated since 0.10.0 This method will become private in a future release.
     */
    @Override
    @Deprecated
    public int execute(List<String> script, String summaryForLogging) {
        return execute(Maps.newLinkedHashMap(), script, summaryForLogging);
    }

    /**
     * @deprecated since 0.10.0 This method will become private in a future release.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    @Deprecated
    public int execute(Map flags2, List<String> script, String summaryForLogging) {
        // TODO replace with SshEffectorTasks.ssh ?; remove the use of flags
        // TODO log the stdin/stdout/stderr upon error
        Map flags = Maps.newLinkedHashMap();
        if (!flags2.containsKey(IGNORE_ENTITY_SSH_FLAGS)) {
            flags.putAll(getSshFlags());
        }
        flags.putAll(flags2);
        Map<String, String> environment = (Map<String, String>) flags.get("env");
        if (environment == null) {
            // Important only to call getShellEnvironment() if env was not supplied; otherwise it
            // could cause us to resolve config (e.g. block for attributeWhenReady) too early.
            environment = getShellEnvironment();
        }
        if (Tasks.current()!=null) {
            // attach tags here, as well as in ScriptHelper, because they may have just been read from the driver
            if (environment!=null) {
                Tasks.addTagDynamically(BrooklynTaskTags.tagForEnvStream(BrooklynTaskTags.STREAM_ENV, environment));
            }
            if (BrooklynTaskTags.stream(Tasks.current(), BrooklynTaskTags.STREAM_STDIN)==null) {
                Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDIN,
                    Streams.byteArrayOfString(Strings.join(script, "\n"))));
            }
            if (BrooklynTaskTags.stream(Tasks.current(), BrooklynTaskTags.STREAM_STDOUT)==null) {
                ByteArrayOutputStream stdout = new ByteArrayOutputStream();
                Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDOUT, stdout));
                ByteArrayOutputStream stderr = new ByteArrayOutputStream();
                Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDERR, stderr));
                flags.put("out", stdout);
                flags.put("err", stderr);
            }
        }
        if (!flags.containsKey("logPrefix")) flags.put("logPrefix", ""+entity.getId()+"@"+getLocation().getDisplayName());
        return getMachine().execScript(flags, summaryForLogging, script, environment);
    }

    @Override
    public void copyPreInstallResources() {
        final WithMutexes mutexSupport = getLocation().mutexes();
        String mutexId = "installation lock at host";
        mutexSupport.acquireMutex(mutexId, "pre-installation lock at host for files and templates");
        try {
            super.copyPreInstallResources();
        } catch (Exception e) {
            log.warn("Error copying pre-install resources", e);
            throw Exceptions.propagate(e);
        } finally {
            mutexSupport.releaseMutex(mutexId);
        }
    }

    @Override
    public void copyInstallResources() {
        final WithMutexes mutexSupport = getLocation().mutexes();
        String mutexId = "installation lock at host";
        mutexSupport.acquireMutex(mutexId, "installation lock at host for files and templates");
        try {
            super.copyInstallResources();
        } catch (Exception e) {
            log.warn("Error copying install resources", e);
            throw Exceptions.propagate(e);
        } finally {
            mutexSupport.releaseMutex(mutexId);
        }
    }

    @Override
    public void copyCustomizeResources() {
        final WithMutexes mutexSupport = getLocation().mutexes();
        String mutexId = "installation lock at host";
        mutexSupport.acquireMutex(mutexId, "installation lock at host for files and templates");
        try {
            super.copyCustomizeResources();
        } catch (Exception e) {
            log.warn("Error copying customize resources", e);
            throw Exceptions.propagate(e);
        } finally {
            mutexSupport.releaseMutex(mutexId);
        }
    }

    private void executeSuccessfully(ConfigKey<String> configKey, String label) {
        if(Strings.isNonBlank(getEntity().getConfig(configKey))) {
            log.debug("Executing {} on entity {}", label, entity.getDisplayName());
            int result = execute(ImmutableList.of(getEntity().getConfig(configKey)), label);
            if (0 != result) {
                log.debug("Executing {} failed with return code {}", label, result);
                throw new IllegalStateException("commands for " + configKey.getName() + " failed with return code " + result);
            }
        }
    }

    @Override
    public void runPreInstallCommand() {
        executeSuccessfully(BrooklynConfigKeys.PRE_INSTALL_COMMAND, "running pre-install commands");
    }

    @Override
    public void runPostInstallCommand() {
        executeSuccessfully(BrooklynConfigKeys.POST_INSTALL_COMMAND, "running post-install commands");
    }

    @Override
    public void runPreCustomizeCommand() {
        executeSuccessfully(BrooklynConfigKeys.PRE_CUSTOMIZE_COMMAND, "running pre-customize commands");
    }

    @Override
    public void runPostCustomizeCommand() {
        executeSuccessfully(BrooklynConfigKeys.POST_CUSTOMIZE_COMMAND, "running post-customize commands");
    }

    @Override
    public void runPreLaunchCommand() {
        executeSuccessfully(BrooklynConfigKeys.PRE_LAUNCH_COMMAND, "running pre-launch commands");
    }

    @Override
    public void runPostLaunchCommand() {
        executeSuccessfully(BrooklynConfigKeys.POST_LAUNCH_COMMAND, "running post-launch commands");
    }

    /**
     * @param sshFlags Extra flags to be used when making an SSH connection to the entity's machine.
     *                 If the map contains the key {@link #IGNORE_ENTITY_SSH_FLAGS} then only the
     *                 given flags are used. Otherwise, the given flags are combined with (and take
     *                 precendence over) the flags returned by {@link #getSshFlags()}.
     * @param source URI of file to copy, e.g. file://.., http://.., classpath://..
     * @param target Destination on server, relative to {@link #getRunDir()} if not absolute path
     * @param createParentDir Whether to create the parent target directory, if it doesn't already exist
     * @return The exit code of the SSH command run
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public int copyResource(Map<Object,Object> sshFlags, String source, String target, boolean createParentDir) {
        // TODO use SshTasks.put instead, better logging
        Map flags = Maps.newLinkedHashMap();
        if (!sshFlags.containsKey(IGNORE_ENTITY_SSH_FLAGS)) {
            flags.putAll(getSshFlags());
        }
        flags.putAll(sshFlags);

        String destination = Os.isAbsolutish(target) ? target : Os.mergePathsUnix(getRunDir(), target);

        if (createParentDir) {
            // don't use File.separator because it's remote machine's format, rather than local machine's
            int lastSlashIndex = destination.lastIndexOf("/");
            String parent = (lastSlashIndex > 0) ? destination.substring(0, lastSlashIndex) : null;
            if (parent != null) {
                getMachine().execCommands("createParentDir", ImmutableList.of("mkdir -p "+parent));
            }
        }

        int result = getMachine().installTo(resource, flags, source, destination);
        if (result == 0) {
            if (log.isDebugEnabled()) {
                log.debug("Copied file for {}: {} to {} - result {}", new Object[] { entity, source, destination, result });
            }
        }
        return result;
    }

    /**
     * Input stream will be closed automatically.
     * <p>
     * If using {@link SshjTool} usage, consider using {@link KnownSizeInputStream} to avoid having
     * to write out stream once to find its size!
     *
     * @see #copyResource(Map, String, String) for parameter descriptions.
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public int copyResource(Map<Object,Object> sshFlags, InputStream source, String target, boolean createParentDir) {
        Map flags = Maps.newLinkedHashMap();
        if (!sshFlags.containsKey(IGNORE_ENTITY_SSH_FLAGS)) {
            flags.putAll(getSshFlags());
        }
        flags.putAll(sshFlags);

        String destination = Os.isAbsolutish(target) ? target : Os.mergePathsUnix(getRunDir(), target);

        if (createParentDir) {
            // don't use File.separator because it's remote machine's format, rather than local machine's
            int lastSlashIndex = destination.lastIndexOf("/");
            String parent = (lastSlashIndex > 0) ? destination.substring(0, lastSlashIndex) : null;
            if (parent != null) {
                getMachine().execCommands("createParentDir", ImmutableList.of("mkdir -p "+parent));
            }
        }

        // TODO SshMachineLocation.copyTo currently doesn't log warn on non-zero or set blocking details
        // (because delegated to by installTo, for multiple calls). So do it here for now.
        int result;
        String prevBlockingDetails = Tasks.setBlockingDetails("copying resource to server at "+destination);
        try {
            result = getMachine().copyTo(flags, source, destination);
        } finally {
            Tasks.setBlockingDetails(prevBlockingDetails);
        }

        if (result == 0) {
            log.debug("copying stream complete; {} on {}", new Object[] { destination, getMachine() });
        } else {
            log.warn("copying stream failed; {} on {}: {}", new Object[] { destination, getMachine(), result });
        }
        return result;
    }

    public void checkNoHostnameBug() {
        try {
            ProcessTaskWrapper<Integer> hostnameTask = DynamicTasks.queue(SshEffectorTasks.ssh("echo FOREMARKER; hostname; echo AFTMARKER")).block();
            String stdout = Strings.getFragmentBetween(hostnameTask.getStdout(), "FOREMARKER", "AFTMARKER");
            if (hostnameTask.getExitCode() == 0 && Strings.isNonBlank(stdout)) {
                String hostname = stdout.trim();
                if (hostname.equals("(none)")) {
                    String newHostname = "br-"+getEntity().getId().toLowerCase();
                    log.info("Detected no-hostname bug with hostname "+hostname+" for "+getEntity()+"; renaming "+getMachine()+"  to hostname "+newHostname);
                    DynamicTasks.queue(SshEffectorTasks.ssh(BashCommands.setHostname(newHostname, null))).block();
                }
            } else {
                log.debug("Hostname could not be determined for location "+EffectorTasks.findSshMachine()+"; not doing no-hostname bug check");
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Error checking/fixing no-hostname bug (continuing): "+e, e);
        }
    }

    public static final String INSTALLING = "installing";
    public static final String CUSTOMIZING = "customizing";
    public static final String LAUNCHING = "launching";
    public static final String CHECK_RUNNING = "check-running";
    public static final String STOPPING = "stopping";
    public static final String KILLING = "killing";
    public static final String RESTARTING = "restarting";

    public static final String PID_FILENAME = "pid.txt";
    static final String INSTALL_DIR_ENV_VAR = "INSTALL_DIR";
    static final String RUN_DIR_ENV_VAR = "RUN_DIR";

    /* Flags */

    /**
     * Use a PID file, created by <em>launching</em>, and reading it for <em>check-running</em>,
     * <em>stopping</em> and <em>killing</em>. The value can be <em>true</em> or a path to a file to
     * use; either relative to <em>RUN_DIR</em> or an absolute path.
     */
    public static final String USE_PID_FILE = "usePidFile";

    /**
     * Define the process owner if not the same as the brooklyn user. Both <em>stopping</em> and
     * <em>killing</em> will sudo to this user before issuing the <code>kill</code> command. Only valid
     * if <em>USE_PID_FILE</em> is also set.
     */
    public static final String PROCESS_OWNER = "processOwner";

    /**
     * Marks the script as having a customised setup, to prevent the default headers and footers being
     * added to the list of commands.
     */
    public static final String NON_STANDARD_LAYOUT = "nonStandardLayout";

    /**
     * Prevents creation of the <code>$INSTALL_DIR/BROOKLYN</code> marker file after <em>installing</em>
     * phase finishes, to allow further installation phases to execute.
     */
    public static final String INSTALL_INCOMPLETE = "installIncomplete";

    /**
     * Enable shell debugging output via <code>set -x</code> prepended to the command header.
     */
    public static final String DEBUG = "debug";

    /** Permitted flags for {@link #newScript(Map, String)}. */
    public static final List<String> VALID_FLAGS =
            ImmutableList.of(USE_PID_FILE, PROCESS_OWNER, NON_STANDARD_LAYOUT, INSTALL_INCOMPLETE, DEBUG);

    /** @see #newScript(Map, String) */
    protected ScriptHelper newScript(String phase) {
        return newScript(Maps.<String, Object>newLinkedHashMap(), phase);
    }

    /**
     * Sets up a {@link ScriptHelper} to generate a script that controls the given phase
     * (<em>check-running</em>, <em>launching</em> etc.) including default header and
     * footer commands.
     * <p>
     * Supported flags:
     * <ul>
     * <li><strong>usePidFile</strong> - <em>true</em> or <em>filename</em> to save and retrieve the PID
     * <li><strong>processOwner</strong> - <em>username</em> that owns the running process
     * <li><strong>nonStandardLayout</strong> - <em>true</em> to omit all default commands
     * <li><strong>installIncomplete</strong> - <em>true</em> to prevent marking complete
     * <li><strong>debug</strong> - <em>true</em> to enable shell debug output
     * </li>
     *
     * @param flags a {@link Map} of flags to control script generation
     * @param phase the phase to create the ScriptHelper for
     *
     * @see #newScript(String)
     * @see #USE_PID_FILE
     * @see #PROCESS_OWNER
     * @see #NON_STANDARD_LAYOUT
     * @see #INSTALL_INCOMPLETE
     * @see #DEBUG
     */
    protected ScriptHelper newScript(Map<String, ?> flags, String phase) {
        if (!Entities.isManaged(getEntity()))
            throw new IllegalStateException(getEntity()+" is no longer managed; cannot create script to run here ("+phase+")");

        if (!Iterables.all(flags.keySet(), StringPredicates.equalToAny(VALID_FLAGS))) {
            throw new IllegalArgumentException("Invalid flags passed: " + flags);
        }

        ScriptHelper s = new ScriptHelper(this, phase+" "+elvis(entity,this));
        if (!groovyTruth(flags.get(NON_STANDARD_LAYOUT))) {
            if (groovyTruth(flags.get(DEBUG))) {
                s.header.prepend("set -x");
            }
            if (INSTALLING.equals(phase)) {
                // mutexId should be global because otherwise package managers will contend with each other
                final String mutexId = "installation lock at host";
                s.useMutex(getLocation().mutexes(), mutexId, "installing "+elvis(entity,this));
                s.header.append(
                        "export "+INSTALL_DIR_ENV_VAR+"=\""+getInstallDir()+"\"",
                        "mkdir -p $"+INSTALL_DIR_ENV_VAR,
                        "cd $"+INSTALL_DIR_ENV_VAR,
                        "test -f BROOKLYN && exit 0"
                        );

                if (!groovyTruth(flags.get(INSTALL_INCOMPLETE))) {
                    s.footer.append("date > $INSTALL_DIR/BROOKLYN");
                }
                // don't set vars during install phase, prevent dependency resolution
                s.environmentVariablesReset();
            }
            if (ImmutableSet.of(CUSTOMIZING, LAUNCHING, CHECK_RUNNING, STOPPING, KILLING, RESTARTING).contains(phase)) {
                s.header.append(
                        "export "+INSTALL_DIR_ENV_VAR+"=\""+getInstallDir()+"\"",
                        "export "+RUN_DIR_ENV_VAR+"=\""+getRunDir()+"\"",
                        "mkdir -p $"+RUN_DIR_ENV_VAR,
                        "cd $"+RUN_DIR_ENV_VAR
                        );
            }
        }

        if (ImmutableSet.of(LAUNCHING, RESTARTING).contains(phase)) {
            s.failIfBodyEmpty();
        }
        if (ImmutableSet.of(STOPPING, KILLING).contains(phase)) {
            // stopping and killing allowed to have empty body if pid file set
            if (!groovyTruth(flags.get(USE_PID_FILE)))
                s.failIfBodyEmpty();
        }
        if (ImmutableSet.of(INSTALLING, LAUNCHING).contains(phase)) {
            s.updateTaskAndFailOnNonZeroResultCode();
        }
        if (phase.equalsIgnoreCase(CHECK_RUNNING)) {
            s.setInessential();
            s.setTransient();
            s.setFlag(SshTool.PROP_CONNECT_TIMEOUT, Duration.TEN_SECONDS.toMilliseconds());
            s.setFlag(SshTool.PROP_SESSION_TIMEOUT, Duration.THIRTY_SECONDS.toMilliseconds());
            s.setFlag(SshTool.PROP_SSH_TRIES, 1);
        }

        if (groovyTruth(flags.get(USE_PID_FILE))) {
            Object usePidFile = flags.get(USE_PID_FILE);
            String pidFile = (usePidFile instanceof CharSequence ? usePidFile : Os.mergePathsUnix(getRunDir(), PID_FILENAME)).toString();
            String processOwner = (String) flags.get(PROCESS_OWNER);
            if (LAUNCHING.equals(phase)) {
                entity.sensors().set(SoftwareProcess.PID_FILE, pidFile);
                s.footer.prepend("echo $! > "+pidFile);
            } else if (CHECK_RUNNING.equals(phase)) {
                // old method, for supplied service, or entity.id
                // "ps aux | grep ${service} | grep \$(cat ${pidFile}) > /dev/null"
                // new way, preferred?
                if (processOwner != null) {
                    s.body.append(
                            BashCommands.sudoAsUser(processOwner, "test -f "+pidFile) + " || exit 1",
                            "ps -p $(" + BashCommands.sudoAsUser(processOwner, "cat "+pidFile) + ")"
                    );
                } else {
                    s.body.append(
                            "test -f "+pidFile+" || exit 1",
                            "ps -p `cat "+pidFile+"`"
                    );
                }
                // no pid, not running; 1 is not running
                s.requireResultCode(Predicates.or(Predicates.equalTo(0), Predicates.equalTo(1)));
            } else if (STOPPING.equals(phase)) {
                String stopCommand = Joiner.on('\n').join("PID=$(cat "+pidFile + ")",
                        "test -n \"$PID\" || exit 0",
                        "SIGTERM_USED=\"\"",
                        "for i in $(seq 1 16); do",
                        "  if ps -p $PID > /dev/null ; then",
                        "     kill $PID",
                        "     echo Attempted to stop PID $PID by sending SIGTERM.",
                        "  else",
                        "     echo Process $PID stopped successfully.",
                        "     SIGTERM_USED=\"true\"",
                        "     break",
                        "  fi",
                        "  sleep 1",
                        "done",
                        "if test -z $SIGTERM_USED; then",
                        "  kill -9 $PID",
                        "  echo Sent SIGKILL to $PID",
                        "fi",
                        "rm -f " + pidFile);
                if (processOwner != null) {
                    s.body.append(BashCommands.sudoAsUser(processOwner, stopCommand));
                } else {
                    s.body.append(stopCommand);
                }
            } else if (KILLING.equals(phase)) {
                if (processOwner != null) {
                    s.body.append(
                            "export PID=$(" + BashCommands.sudoAsUser(processOwner, "cat "+pidFile) + ")",
                            "test -n \"$PID\" || exit 0",
                            BashCommands.sudoAsUser(processOwner, "kill -9 $PID"),
                            BashCommands.sudoAsUser(processOwner, "rm -f "+pidFile)
                    );
                } else {
                    s.body.append(
                            "export PID=$(cat "+pidFile+")",
                            "test -n \"$PID\" || exit 0",
                            "kill -9 $PID",
                            "rm -f "+pidFile
                    );
                }
            } else if (RESTARTING.equals(phase)) {
                if (processOwner != null) {
                    s.footer.prepend(
                            BashCommands.sudoAsUser(processOwner, "test -f "+pidFile) + " || exit 1",
                            "ps -p $(" + BashCommands.sudoAsUser(processOwner, "cat "+pidFile) + ") || exit 1"
                    );
                } else {
                    s.footer.prepend(
                            "test -f "+pidFile+" || exit 1",
                            "ps -p $(cat "+pidFile+") || exit 1"
                    );
                }
                // no pid, not running; no process; can't restart, 1 is not running
            } else {
                log.warn(USE_PID_FILE + ": script option not valid for " + s.summary);
            }
        }

        return s;
    }

    @Override
    protected void createDirectory(String directoryName, String summaryForLogging) {
        DynamicTasks.queue(SshEffectorTasks.ssh("mkdir -p " + directoryName).summary(summaryForLogging)
                .requiringExitCodeZero()).get();
    }

    public Set<Integer> getPortsUsed() {
        Set<Integer> result = Sets.newLinkedHashSet();
        result.add(22);
        return result;
    }

    @Override
    public void setup() { }

}
