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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.ReaderInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

/**
 * An abstract implementation of the {@link SoftwareProcessDriver}.
 */
public abstract class AbstractSoftwareProcessDriver implements SoftwareProcessDriver {

    private static final Logger log = LoggerFactory.getLogger(AbstractSoftwareProcessDriver.class);

    protected final EntityLocal entity;
    protected final ResourceUtils resource;
    protected final Location location;

    public AbstractSoftwareProcessDriver(EntityLocal entity, Location location) {
        this.entity = checkNotNull(entity, "entity");
        this.location = checkNotNull(location, "location");
        this.resource = ResourceUtils.create(entity);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.brooklyn.entity.software.base.SoftwareProcessDriver#rebind()
     */
    @Override
    public void rebind() {
        // no-op
    }

    /**
     * Start the entity.
     * <p>
     * This installs, configures and launches the application process. However,
     * users can also call the {@link #install()}, {@link #customize()} and
     * {@link #launch()} steps independently. The {@link #postLaunch()} will
     * be called after the {@link #launch()} metheod is executed, but the
     * process may not be completely initialised at this stage, so care is
     * required when implementing these stages.
     * <p>
     * The {@link BrooklynConfigKeys#SKIP_ENTITY_START_IF_RUNNING} key can be set on the location
     * or the entity to skip the startup process if the entity is already running,
     * according to the {@link #isRunning()} method. To force the startup to be
     * skipped, {@link BrooklynConfigKeys#SKIP_ENTITY_START} can be set on the entity.
     * The {@link BrooklynConfigKeys#SKIP_ENTITY_INSTALLATION} key can also be used to
     * skip the {@link #setup()}, {@link #copyInstallResources()} and
     * {@link #install()} methods if set on the entity or location.
     *
     * @see #stop()
     */
    @Override
    public void start() {
        boolean skipStart = false;
        Optional<Boolean> locationRunning = Optional.fromNullable(getLocation().getConfig(BrooklynConfigKeys.SKIP_ENTITY_START_IF_RUNNING));
        Optional<Boolean> entityRunning = Optional.fromNullable(entity.getConfig(BrooklynConfigKeys.SKIP_ENTITY_START_IF_RUNNING));
        Optional<Boolean> entityStarted = Optional.fromNullable(entity.getConfig(BrooklynConfigKeys.SKIP_ENTITY_START));
        if (locationRunning.or(entityRunning).or(false)) {
            skipStart = isRunning();
        } else {
            skipStart = entityStarted.or(false);
        }

        DynamicTasks.queue("prepare", new Runnable() { public void run() {
            prepare();
        }});

        if (!skipStart) {
            DynamicTasks.queue("install", new Runnable() { public void run() {
                Optional<Boolean> locationInstalled = Optional.fromNullable(getLocation().getConfig(BrooklynConfigKeys.SKIP_ENTITY_INSTALLATION));
                Optional<Boolean> entityInstalled = Optional.fromNullable(entity.getConfig(BrooklynConfigKeys.SKIP_ENTITY_INSTALLATION));

                boolean skipInstall = locationInstalled.or(entityInstalled).or(false);
                if (!skipInstall) {
                    DynamicTasks.queue("copy-pre-install-resources", new Runnable() { public void run() {
                        waitForConfigKey(BrooklynConfigKeys.PRE_INSTALL_RESOURCES_LATCH);
                        copyPreInstallResources();
                    }});

                    DynamicTasks.queue("pre-install", new Runnable() { public void run() {
                        preInstall();
                    }});

                    DynamicTasks.queue("pre-install-command", new Runnable() { public void run() {
                        runPreInstallCommand();
                    }});

                    DynamicTasks.queue("setup", new Runnable() { public void run() {
                        waitForConfigKey(BrooklynConfigKeys.SETUP_LATCH);
                        setup();
                    }});

                    DynamicTasks.queue("copy-install-resources", new Runnable() { public void run() {
                        waitForConfigKey(BrooklynConfigKeys.INSTALL_RESOURCES_LATCH);
                        copyInstallResources();
                    }});

                    DynamicTasks.queue("install (main)", new Runnable() { public void run() {
                        waitForConfigKey(BrooklynConfigKeys.INSTALL_LATCH);
                        install();
                    }});

                    DynamicTasks.queue("post-install-command", new Runnable() { public void run() {
                        runPostInstallCommand();
                    }});
                }
            }});

            DynamicTasks.queue("customize", new Runnable() { public void run() {
                DynamicTasks.queue("pre-customize-command", new Runnable() { public void run() {
                    runPreCustomizeCommand();
                }});

                DynamicTasks.queue("customize (main)", new Runnable() { public void run() {
                    waitForConfigKey(BrooklynConfigKeys.CUSTOMIZE_LATCH);
                    customize();
                }});

                DynamicTasks.queue("post-customize-command", new Runnable() { public void run() {
                    runPostCustomizeCommand();
                }});
            }});

            DynamicTasks.queue("launch", new Runnable() { public void run() {
                DynamicTasks.queue("copy-runtime-resources", new Runnable() { public void run() {
                    waitForConfigKey(BrooklynConfigKeys.RUNTIME_RESOURCES_LATCH);
                    copyRuntimeResources();
                }});

                DynamicTasks.queue("pre-launch-command", new Runnable() { public void run() {
                    runPreLaunchCommand();
                }});

                DynamicTasks.queue("launch (main)", new Runnable() { public void run() {
                    waitForConfigKey(BrooklynConfigKeys.LAUNCH_LATCH);
                    launch();
                }});

                DynamicTasks.queue("post-launch-command", new Runnable() { public void run() {
                    runPostLaunchCommand();
                }});
            }});
        }

        DynamicTasks.queue("post-launch", new Runnable() { public void run() {
            postLaunch();
        }});
    }

    @Override
    public abstract void stop();

    /**
     * Prepare the entity instance before running any commands. Always executed during {@link #start()}.
     */
    public void prepare() {}

    /**
     * Implement this method in child classes to add some pre-install behavior
     */
    public void preInstall() {}

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void runPreInstallCommand();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void setup();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void install();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void runPostInstallCommand();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void runPreCustomizeCommand();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void customize();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void runPostCustomizeCommand();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void runPreLaunchCommand();

    /**
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void launch();

    /**
     * Only run if launch is run (if start is not skipped).
     * Implementations should fail if the return code is non-zero, by throwing some appropriate exception.
     */
    public abstract void runPostLaunchCommand();

    @Override
    public void kill() {
        stop();
    }

    /**
     * Implement this method in child classes to add some post-launch behavior.
     * This is run even if start is skipped and launch is not run.
     */
    public void postLaunch() {}

    @Override
    public void restart() {
        DynamicTasks.queue("stop (best effort)", new Runnable() {
            public void run() {
                DynamicTasks.markInessential();
                boolean previouslyRunning = isRunning();
                try {
                    ServiceStateLogic.setExpectedState(getEntity(), Lifecycle.STOPPING);
                    stop();
                } catch (Exception e) {
                    // queue a failed task so that there is visual indication that this task had a failure,
                    // without interrupting the parent
                    if (previouslyRunning) {
                        log.warn(getEntity() + " restart: stop failed, when was previously running (ignoring)", e);
                        DynamicTasks.queue(Tasks.fail("Primary job failure (when previously running)", e));
                    } else {
                        log.debug(getEntity() + " restart: stop failed (but was not previously running, so not a surprise)", e);
                        DynamicTasks.queue(Tasks.fail("Primary job failure (when not previously running)", e));
                    }
                    // the above queued tasks will cause this task to be indicated as failed, with an indication of severity
                }
            }
        });

        DynamicTasks.queue("restart", new Runnable() {
            public void run() {
                try {
                    if (doFullStartOnRestart()) {
                        DynamicTasks.waitForLast();
                        ServiceStateLogic.setExpectedState(getEntity(), Lifecycle.STARTING);
                        start();
                    } else {
                        DynamicTasks.queue("pre-launch-command", new Runnable() { public void run() {
                            ServiceStateLogic.setExpectedState(getEntity(), Lifecycle.STARTING);
                            runPreLaunchCommand();
                        }});
                        DynamicTasks.queue("launch (main)", new Runnable() { public void run() {
                            launch();
                        }});
                        DynamicTasks.queue("post-launch-command", new Runnable() { public void run() {
                            runPostLaunchCommand();
                        }});
                        DynamicTasks.queue("post-launch", new Runnable() { public void run() {
                            postLaunch();
                        }});
                    }
                    DynamicTasks.waitForLast();
                } catch (Exception e) {
                    ServiceStateLogic.setExpectedState(entity, Lifecycle.ON_FIRE);
                    throw Exceptions.propagate(e);
                }
            }
        });
    }

    @Beta
    /** ideally restart() would take options, e.g. whether to do full start, skip installs, etc;
     * however in the absence here is a toggle - not sure how well it works;
     * default is false which is similar to previous behaviour (with some seemingly-obvious tidies),
     * meaning install and configure will NOT be done on restart. */
    protected boolean doFullStartOnRestart() {
        return false;
    }

    @Override
    public EntityLocal getEntity() { return entity; }

    @Override
    public Location getLocation() { return location; }

    public InputStream getResource(String url) {
        return resource.getResourceFromUrl(url);
    }

    /**
     * Files and templates to be copied to the server <em>before</em> pre-install. This allows the {@link #preInstall()}
     * process to have access to all required resources.
     * <p>
     * Will be prefixed with the entity's {@link #getInstallDir() install directory} if relative.
     *
     * @see SoftwareProcess#PRE_INSTALL_FILES
     * @see SoftwareProcess#PRE_INSTALL_TEMPLATES
     * @see #copyRuntimeResources()
     */
    public void copyPreInstallResources() {
        copyResources(getInstallDir(), entity.getConfig(SoftwareProcess.PRE_INSTALL_FILES), entity.getConfig(SoftwareProcess.PRE_INSTALL_TEMPLATES));
    }

    /**
     * Files and templates to be copied to the server <em>before</em> installation. This allows the {@link #install()}
     * process to have access to all required resources.
     * <p>
     * Will be prefixed with the entity's {@link #getInstallDir() install directory} if relative.
     *
     * @see SoftwareProcess#INSTALL_FILES
     * @see SoftwareProcess#INSTALL_TEMPLATES
     * @see #copyRuntimeResources()
     */
    public void copyInstallResources() {
        // Ensure environment variables are not looked up here, otherwise sub-classes might
        // lookup port numbers and fail with ugly error if port is not set; better to wait
        // until in Entity's code (e.g. customize) where such checks are done explicitly.
        copyResources(getInstallDir(), entity.getConfig(SoftwareProcess.INSTALL_FILES), entity.getConfig(SoftwareProcess.INSTALL_TEMPLATES));
    }

    private void copyResources(String destinationParentDir, Map<String, String> files, Map<String, String> templates) {
        if (files == null) files = Collections.emptyMap();
        if (templates == null) templates = Collections.emptyMap();

        final List<TaskAdaptable<?>> tasks = new ArrayList<>(files.size() + templates.size());
        applyFnToResourcesAppendToList(files, newCopyResourceFunction(), destinationParentDir, tasks);
        applyFnToResourcesAppendToList(templates, newCopyTemplateFunction(), destinationParentDir, tasks);

        if (!tasks.isEmpty()) {
            String oldBlockingDetails = Tasks.setBlockingDetails("Copying resources");
            try {
                DynamicTasks.queue(Tasks.sequential(tasks)).getUnchecked();
            } finally {
                Tasks.setBlockingDetails(oldBlockingDetails);
            }
        }
    }

    private void applyFnToResourcesAppendToList(
            Map<String, String> resources, final Function<SourceAndDestination, Task<?>> function,
            String destinationParentDir, final List<TaskAdaptable<?>> tasks) {

        for (Map.Entry<String, String> entry : resources.entrySet()) {
            final String source = checkNotNull(entry.getKey(), "Missing source for resource");
            String target = checkNotNull(entry.getValue(), "Missing destination for resource");
            final String destination = Os.isAbsolutish(target) ? target : Os.mergePathsUnix(destinationParentDir, target);

            // if source is a directory then copy all files underneath.
            // e.g. /tmp/a/{b,c/d}, source = /tmp/a, destination = dir/a/b and dir/a/c/d.
            final File srcFile = new File(source);
            if (srcFile.isDirectory() && srcFile.exists()) {
                try {
                    final Path start = srcFile.toPath();
                    final int startElements = start.getNameCount();
                    // Actually walking to a depth of Integer.MAX_VALUE would be interesting.
                    Files.walkFileTree(start, EnumSet.of(FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            if (attrs.isRegularFile()) {
                                Path relativePath = file.subpath(startElements, file.getNameCount());
                                tasks.add(function.apply(new SourceAndDestination(file.toString(), Os.mergePathsUnix(destination, relativePath.toString()))));
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) {
                    throw Exceptions.propagate(e);
                }
            } else {
                tasks.add(function.apply(new SourceAndDestination(source, destination)));
            }
        }
    }

    private static class SourceAndDestination {
        final String source;
        final String destination;
        private SourceAndDestination(String source, String destination) {
            this.source = source;
            this.destination = destination;
        }
    }

    private Function<SourceAndDestination, Task<?>> newCopyResourceFunction() {
        return new Function<SourceAndDestination, Task<?>>() {
            @Override
            public Task<?> apply(final SourceAndDestination input) {
                return Tasks.builder()
                        .displayName("Copying file: source=" + input.source + ", destination=" + input.destination)
                        .body(new Callable<Object>() {
                            @Override
                            public Integer call() {
                                return copyResource(input.source, input.destination, true);
                            }
                        })
                        .build();
            }
        };
    }

    private Function<SourceAndDestination, Task<?>> newCopyTemplateFunction() {
        return new Function<SourceAndDestination, Task<?>>() {
            @Override
            public Task<?> apply(final SourceAndDestination input) {
                return Tasks.builder()
                        .displayName("Copying template: source=" + input.source + ", destination=" + input.destination)
                        .body(new Callable<Object>() {
                            @Override
                            public Integer call() {
                                return copyTemplate(input.source, input.destination, true, Collections.<String, Object>emptyMap());
                            }
                        })
                        .build();
            }
        };
    }

    protected abstract void createDirectory(String directoryName, String summaryForLogging);

    /**
     * Files and templates to be copied to the server <em>after</em> customisation. This allows overwriting of
     * existing files such as entity configuration which may be copied from the installation directory
     * during the {@link #customize()} process.
     * <p>
     * Will be prefixed with the entity's {@link #getRunDir() run directory} if relative.
     *
     * @see SoftwareProcess#RUNTIME_FILES
     * @see SoftwareProcess#RUNTIME_TEMPLATES
     * @see #copyInstallResources()
     */
    public void copyRuntimeResources() {
        try {
            copyResources(getRunDir(), entity.getConfig(SoftwareProcess.RUNTIME_FILES), entity.getConfig(SoftwareProcess.RUNTIME_TEMPLATES));
        } catch (Exception e) {
            log.warn("Error copying runtime resources", e);
            throw Exceptions.propagate(e);
        }
    }

    /**
     * @param template File to template and copy.
     * @param target Destination on server.
     * @return The exit code the SSH command run.
     */
    public int copyTemplate(File template, String target) {
        return copyTemplate(template.toURI().toASCIIString(), target);
    }

    /**
     * @param template URI of file to template and copy, e.g. file://.., http://.., classpath://..
     * @param target Destination on server.
     * @return The exit code of the SSH command run.
     */
    public int copyTemplate(String template, String target) {
        return copyTemplate(template, target, false, ImmutableMap.<String, String>of());
    }

    /**
     * @param template URI of file to template and copy, e.g. file://.., http://.., classpath://..
     * @param target Destination on server.
     * @param extraSubstitutions Extra substitutions for the templater to use, for example
     *               "foo" -> "bar", and in a template ${foo}.
     * @return The exit code of the SSH command run.
     */
    public int copyTemplate(String template, String target, boolean createParent, Map<String, ?> extraSubstitutions) {
        String data = processTemplate(template, extraSubstitutions);
        return copyResource(MutableMap.<Object,Object>of(), new StringReader(data), target, createParent);
    }

    public abstract int copyResource(Map<Object,Object> sshFlags, String source, String target, boolean createParentDir);

    public abstract int copyResource(Map<Object,Object> sshFlags, InputStream source, String target, boolean createParentDir);

    /**
     * @param file File to copy.
     * @param target Destination on server.
     * @return The exit code the SSH command run.
     */
    public int copyResource(File file, String target) {
        return copyResource(file.toURI().toASCIIString(), target);
    }

    /**
     * @param resource URI of file to copy, e.g. file://.., http://.., classpath://..
     * @param target Destination on server.
     * @return The exit code of the SSH command run
     */
    public int copyResource(String resource, String target) {
        return copyResource(MutableMap.of(), resource, target);
    }

    public int copyResource(String resource, String target, boolean createParentDir) {
        return copyResource(MutableMap.of(), resource, target, createParentDir);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public int copyResource(Map sshFlags, String source, String target) {
        return copyResource(sshFlags, source, target, false);
    }

    /**
     * @see #copyResource(Map, InputStream, String, boolean)
     */
    public int copyResource(Reader source, String target) {
        return copyResource(MutableMap.of(), source, target, false);
    }

    /**
     * @see #copyResource(Map, InputStream, String, boolean)
     */
    public int copyResource(Map<Object,Object> sshFlags, Reader source, String target, boolean createParent) {
        return copyResource(sshFlags, new ReaderInputStream(source), target, createParent);
    }

    /**
     * @see #copyResource(Map, InputStream, String, boolean)
     */
    public int copyResource(InputStream source, String target) {
        return copyResource(MutableMap.of(), source, target, false);
    }

    public String getResourceAsString(String url) {
        return resource.getResourceAsString(url);
    }

    public String processTemplate(File templateConfigFile, Map<String,Object> extraSubstitutions) {
        return processTemplate(templateConfigFile.toURI().toASCIIString(), extraSubstitutions);
    }

    public String processTemplate(File templateConfigFile) {
        return processTemplate(templateConfigFile.toURI().toASCIIString());
    }

    /** Takes the contents of a template file from the given URL (often a classpath://com/myco/myprod/myfile.conf or .sh)
     * and replaces "${entity.xxx}" with the result of entity.getXxx() and similar for other driver, location;
     * as well as replacing config keys on the management context
     * <p>
     * uses Freemarker templates under the covers
     **/
    public String processTemplate(String templateConfigUrl) {
        return processTemplate(templateConfigUrl, ImmutableMap.<String,String>of());
    }

    public String processTemplate(String templateConfigUrl, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents(getResourceAsString(templateConfigUrl), extraSubstitutions);
    }

    public String processTemplateContents(String templateContents) {
        return processTemplateContents(templateContents, ImmutableMap.<String,String>of());
    }

    public String processTemplateContents(String templateContents, Map<String,? extends Object> extraSubstitutions) {
        return TemplateProcessor.processTemplateContents(templateContents, this, extraSubstitutions);
    }

    protected void waitForConfigKey(ConfigKey<?> configKey) {
        Object val = entity.config().get(configKey);
        if (val != null) log.debug("{} finished waiting for {} (value {}); continuing...", new Object[] {this, configKey, val});
    }

    public String getArchiveNameFormat() {
        return getEntity().config().get(SoftwareProcess.ARCHIVE_DIRECTORY_NAME_FORMAT);
    }

    public String getVersion() {
        return getEntity().config().get(SoftwareProcess.SUGGESTED_VERSION);
    }

    public abstract String getRunDir();
    public abstract String getInstallDir();
}
