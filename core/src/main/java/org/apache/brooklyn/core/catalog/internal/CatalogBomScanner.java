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
package org.apache.brooklyn.core.catalog.internal;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

/** Scans bundles being added, filtered by a whitelist and blacklist, and adds catalog.bom files to the catalog.
 * See karaf blueprint.xml for configuration, and tests in dist project. */
@Beta
public class CatalogBomScanner {

    private final String ACCEPT_ALL_BY_DEFAULT = ".*";

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBomScanner.class);

    private List<String> whiteList = ImmutableList.of(ACCEPT_ALL_BY_DEFAULT);
    private List<String> blackList = ImmutableList.of();

    private CatalogBundleTracker catalogBundleTracker;

    public void bind(ServiceReference<ManagementContext> mgmtContextReference) throws Exception {
        if (isEnabled()) {
            LOG.debug("Binding management context with whiteList [{}] and blacklist [{}]",
                Strings.join(getWhiteList(), "; "),
                Strings.join(getBlackList(), "; "));

            final BundleContext bundleContext = mgmtContextReference.getBundle().getBundleContext();
            ManagementContext mgmt = bundleContext.getService(mgmtContextReference);
            CatalogBundleLoader bundleLoader = new CatalogBundleLoader(new SymbolicNameAccessControl(), mgmt);

            catalogBundleTracker = new CatalogBundleTracker(bundleContext, bundleLoader);
            catalogBundleTracker.open();
        }
    }

    public void unbind(ServiceReference<ManagementContext> mgmtContextReference) throws Exception {
        if (isEnabled()) {
            LOG.debug("Unbinding management context");
            if (null != catalogBundleTracker) {
                CatalogBundleTracker temp = catalogBundleTracker;
                catalogBundleTracker = null;
                temp.close();
            }
            mgmtContextReference.getBundle().getBundleContext().ungetService(mgmtContextReference);
        }
    }

    private boolean isEnabled() {
        return BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_LOAD_BUNDLE_CATALOG_BOM);
    }

    public List<String> getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(List<String> whiteList) {
        this.whiteList = whiteList;
    }

    public void setWhiteList(String whiteListText) {
        LOG.debug("Setting whiteList to ", whiteListText);
        this.whiteList = Strings.parseCsv(whiteListText);
    }

    public List<String> getBlackList() {
        return blackList;
    }

    public void setBlackList(List<String> blackList) {
        this.blackList = blackList;
    }

    public void setBlackList(String blackListText) {
        LOG.debug("Setting blackList to ", blackListText);
        this.blackList = Strings.parseCsv(blackListText);
    }

    public class SymbolicNameAccessControl implements Predicate<Bundle> {
        @Override
        public boolean apply(@Nullable Bundle input) {
            return passesWhiteAndBlacklists(input);
        }
    }

    private boolean passesWhiteAndBlacklists(Bundle bundle) {
        return on(bundle, getWhiteList()) && !on(bundle, getBlackList());
    }

    private boolean on(Bundle bundle, List<String> list) {
        for (String candidate : list) {
            final String symbolicName = bundle.getSymbolicName();
            if (symbolicName.matches(candidate.trim())) {
                return true;
            }
        }
        return false;
    }

}
