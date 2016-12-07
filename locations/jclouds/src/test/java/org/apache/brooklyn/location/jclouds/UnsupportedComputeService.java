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
package org.apache.brooklyn.location.jclouds;

import java.util.Map;
import java.util.Set;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.extensions.ImageExtension;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Location;
import org.jclouds.scriptbuilder.domain.Statement;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;

public class UnsupportedComputeService implements ComputeService {

    @Override
    public ComputeServiceContext getContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TemplateBuilder templateBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TemplateOptions templateOptions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends Hardware> listHardwareProfiles() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends Image> listImages() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Image getImage(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends ComputeMetadata> listNodes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> listNodesByIds(Iterable<String> ids) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> createNodesInGroup(String group, int count, Template template) throws RunNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> createNodesInGroup(String group, int count, TemplateOptions templateOptions)
            throws RunNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> createNodesInGroup(String group, int count) throws RunNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resumeNode(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> resumeNodesMatching(Predicate<? super NodeMetadata> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void suspendNode(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> suspendNodesMatching(Predicate<? super NodeMetadata> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroyNode(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> destroyNodesMatching(Predicate<? super NodeMetadata> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rebootNode(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> rebootNodesMatching(Predicate<? super NodeMetadata> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeMetadata getNodeMetadata(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends NodeMetadata> listNodesDetailsMatching(Predicate<? super NodeMetadata> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(Predicate<? super NodeMetadata> filter, String runScript) throws RunScriptOnNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(Predicate<? super NodeMetadata> filter, Statement runScript) throws RunScriptOnNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(Predicate<? super NodeMetadata> filter, String runScript, RunScriptOptions options) throws RunScriptOnNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(Predicate<? super NodeMetadata> filter, Statement runScript, RunScriptOptions options) throws RunScriptOnNodesException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecResponse runScriptOnNode(String id, Statement runScript, RunScriptOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ExecResponse> submitScriptOnNode(String id, String runScript, RunScriptOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ExecResponse> submitScriptOnNode(String id, Statement runScript, RunScriptOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecResponse runScriptOnNode(String id, Statement runScript) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecResponse runScriptOnNode(String id, String runScript, RunScriptOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecResponse runScriptOnNode(String id, String runScript) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ImageExtension> getImageExtension() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SecurityGroupExtension> getSecurityGroupExtension() {
        throw new UnsupportedOperationException();
    }
}
