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
package org.apache.brooklyn.rest.domain;

import java.util.Map;

import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult.ResultCode;
import org.apache.brooklyn.util.collections.MutableMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class BundleInstallationRestResult {
    // as Osgi result, but without bundle, and with maps of catalog items installed
    
    private final String message;
    private final String bundle;
    private final OsgiBundleInstallationResult.ResultCode code;
    @JsonInclude(Include.NON_EMPTY)
    private Map<String,TypeSummary> types = MutableMap.of();
    
    /** json internal only */
    @SuppressWarnings("unused")
    private BundleInstallationRestResult() {
        this.message = null;
        this.bundle = null;
        this.code = null;
    }
    
    public BundleInstallationRestResult(String message, String bundle, ResultCode code) {
        this.message = message;
        this.bundle = bundle;
        this.code = code;
    }
    
    public String getMessage() {
        return message;
    }
    public String getBundle() {
        return bundle;
    }
    public OsgiBundleInstallationResult.ResultCode getCode() {
        return code;
    }
    
    public Map<String, TypeSummary> getTypes() {
        return types;
    }
    
}