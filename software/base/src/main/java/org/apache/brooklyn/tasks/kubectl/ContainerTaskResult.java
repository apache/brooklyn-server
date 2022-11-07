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
package org.apache.brooklyn.tasks.kubectl;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Object containing details of a container task. This object is notified when containerStarted and containerEnded are set.
 */
public class ContainerTaskResult {
    String mainStdout;
    Integer mainExitCode;
    String namespace;
    String kubeJobName;
    public String kubePodName;
    boolean containerStarted = false;
    boolean containerEnded = false;

    /**
     * This will be 0 unless allowNonZeroExitCode was specified
     */
    public Integer getMainExitCode() {
        return mainExitCode;
    }

    public String getMainStdout() {
        return mainStdout;
    }
    @JsonProperty("mainStdout")
    public String getMainStdoutForJson() {
        if (mainStdout!=null && mainStdout.length()>200) {
            return "... "+mainStdout.substring(mainStdout.length()-196);
        }
        return mainStdout;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getKubeJobName() {
        return kubeJobName;
    }

    public String getKubePodName() {
        return kubePodName;
    }

    /** indicates container has started; namespace, pod name and job name will be available now */
    public boolean getContainerStarted() {
        return containerStarted;
    }
    /** indicates container has ended; all output will be set */
    public boolean getContainerEnded() {
        return containerEnded;
    }
}
