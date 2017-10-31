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
package org.apache.brooklyn.core.typereg;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.CatalogUpgrades;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.VersionRangedName;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.mockito.Mockito;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;
import org.osgi.framework.VersionRange;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class BundleUpgradeParserTest {

    private VersionRange from0lessThan1 = new VersionRange('[', Version.valueOf("0"), Version.valueOf("1.0.0"), ')');
    private VersionRange from0lessThan1_2_3 = new VersionRange('[', Version.valueOf("0"), Version.valueOf("1.2.3"), ')');
    private VersionRange exactly0dot1 = new VersionRange('[', Version.valueOf("0.1.0"), Version.valueOf("0.1.0"), ']');
    private VersionRangedName fooFrom0lessThan1 = new VersionRangedName("foo", from0lessThan1);
    private VersionRangedName barFrom0lessThan1 = new VersionRangedName("bar", from0lessThan1);

    @Test
    public void testVersionRangedName() throws Exception {
        assertEquals(VersionRangedName.fromString("foo:0.1.0", true).toOsgiString(), "foo:0.1.0");
        assertEquals(VersionRangedName.fromString("foo:0.1.0", false).toOsgiString(), "foo:[0.1.0,0.1.0]");
        assertEquals(VersionRangedName.fromString("foo:[0,1)", false).toOsgiString(), "foo:[0.0.0,1.0.0)");
        
        assertVersionRangedNameFails("foo", "'foo' must be of 'name:versionRange' syntax");
        assertVersionRangedNameFails("foo:bar:0.1.0", "has too many parts");
        assertVersionRangedNameFails("", "Must not be blank");
        assertVersionRangedNameFails(null, "Must not be blank");
    }
    
    @Test
    public void testVersionRangesWithSnapshots() throws Exception {
        VersionRange from0lessThan1 = VersionRangedName.fromString("foo:[0,1)", false).getOsgiVersionRange();
        assertTrue(from0lessThan1.includes(Version.valueOf("0.1.0.SNAPSHOT")));
        assertTrue(from0lessThan1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("0.1.0-SNAPSHOT"))));
        assertTrue(from0lessThan1.includes(Version.valueOf("0.0.0.SNAPSHOT")));
        assertTrue(from0lessThan1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("0.0.0-SNAPSHOT"))));
        assertFalse(from0lessThan1.includes(Version.valueOf("1.0.0.SNAPSHOT")));
        assertFalse(from0lessThan1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("1.0.0-SNAPSHOT"))));
        
        VersionRange from1 = VersionRangedName.fromString("foo:[1,9999)", false).getOsgiVersionRange();
        assertTrue(from1.includes(Version.valueOf("1.0.0.SNAPSHOT")));
        assertTrue(from1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("1.SNAPSHOT"))));
        assertFalse(from1.includes(Version.valueOf("0.0.0.SNAPSHOT")));
        assertFalse(from1.includes(Version.valueOf("0.1.0.SNAPSHOT")));
    }
    
    @Test
    public void testParseSingleQuotedVal() throws Exception {
        String input = "\"foo:[0,1.0.0)\"";
        assertParseList(input, ImmutableList.of(fooFrom0lessThan1));
    }
    
    @Test
    public void testParseSingleQuotedValWithNestedQuotes() throws Exception {
        String input = "\"foo:[0,\"1.0.0\")\"";
        assertParseList(input, ImmutableList.of(fooFrom0lessThan1));
    }
    
    @Test
    public void testParseMultipleVals() throws Exception {
        String input = "\"foo:[0,1.0.0)\",\"bar:[0,1.0.0)\"";
        assertParseList(input, ImmutableList.of(fooFrom0lessThan1, barFrom0lessThan1));
    }

    @Test
    public void testParseValWithExactVersion() throws Exception {
        String input = "\"foo:0.1.0\"";
        assertParseList(input, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
    }

    @Test
    public void testParseForceRemoveBundlesHeader() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("foo.bar", "1.2.3"));
        
        assertParseForceRemoveBundlesHeader("\"foo:0.1.0\"", bundle, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
        assertParseForceRemoveBundlesHeader("\"*\"", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
        assertParseForceRemoveBundlesHeader("*", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
        assertParseForceRemoveBundlesHeader("other:1, '*:[0,1)'", bundle, ImmutableList.of(
            new VersionRangedName("other", VersionRange.valueOf("[1.0.0,1.0.0]")), 
            new VersionRangedName("foo.bar", new VersionRange('[', Version.valueOf("0"), Version.valueOf("1"), ')'))));
    }
    
    @Test
    public void testParseForceRemoveBundlesHeaderWithSnapshot() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("foo.bar", "1.2.3.SNAPSHOT"));
        
        assertParseForceRemoveBundlesHeader("\"*\"", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
        assertParseForceRemoveBundlesHeader("*", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
    }
    
    @Test
    public void testParseForceRemoveLegacyItemsHeader() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("mybundle", "1.0.0"));
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of(
                newMockRegisteredType("foo", "1.0.0"),
                newMockRegisteredType("bar", "1.0.0")));
        
        assertParseForceRemoveLegacyItemsHeader("\"foo:0.1.0\"", bundle, typeSupplier, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
        assertParseForceRemoveLegacyItemsHeader("\"*\"", bundle, typeSupplier, ImmutableList.of(new VersionRangedName("foo", from0lessThan1), new VersionRangedName("bar", from0lessThan1)));
        assertParseForceRemoveLegacyItemsHeader("*", bundle, typeSupplier, ImmutableList.of(new VersionRangedName("foo", from0lessThan1), new VersionRangedName("bar", from0lessThan1)));
        assertParseForceRemoveLegacyItemsHeader("*:1.0.0.SNAPSHOT, \"foo:[0.1,1)", bundle, typeSupplier, 
            ImmutableList.of(new VersionRangedName("foo", VersionRange.valueOf("[1.0.0.SNAPSHOT,1.0.0.SNAPSHOT]")), new VersionRangedName("bar", VersionRange.valueOf("[1.0.0.SNAPSHOT,1.0.0.SNAPSHOT]")), 
                new VersionRangedName("foo", VersionRange.valueOf("[0.1,1)"))));
    }
    
    @Test
    public void testParseBundleEmptyManifest() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of());
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        CatalogUpgrades upgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
        assertTrue(upgrades.isEmpty());
        assertFalse(upgrades.isBundleRemoved(new VersionedName("org.example.brooklyn.mybundle", "0.1.0")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.1.0")));
    }

    @Test
    public void testParseBundleManifest() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"foo:[0,1.0.0)\",\"foo:1.0.0.SNAPSHOT\",\"bar:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)=org.example.brooklyn.mybundle:1\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, "\"foo:[0,1)=foo:1\""));
        checkParse(bundle);
    }

    @Test
    public void testParseBundleManifestWithSpaces() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"foo:[0,1.0.0)\", \"foo:1.0.0.SNAPSHOT\", \"bar:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, " \"org.example.brooklyn.mybundle:[0,1.0.0)\""));
        checkParse(bundle);
    }

    protected void checkParse(Bundle bundle) {
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        CatalogUpgrades upgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
        assertFalse(upgrades.isEmpty());
        assertTrue(upgrades.isBundleRemoved(new VersionedName("org.example.brooklyn.mybundle", "0.1.0")));
        assertFalse(upgrades.isBundleRemoved(new VersionedName("org.example.brooklyn.mybundle", "1.0.0")));
        
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.1.0")));
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.1.0-SNAPSHOT")));
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.0.0-SNAPSHOT")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "1.0.0")));
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "1.0.0.SNAPSHOT")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "1.0.0.GA")));
        
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("bar", "0.1.0")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("bar", "1.0.0")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("bar", "1.0.0.SNAPSHOT")));
        
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("different", "0.1.0")));
    }
    
    @Test
    public void testForgetQuotesGivesNiceError() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "foo:[0,1.0.0),bar:[0,1.0.0)"));
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        try {
            BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "quote");
        }
    }

    @Test
    public void testStripQuotes() throws Exception {
        assertEquals(BundleUpgradeParser.stripQuotes("a"), "a");
        assertEquals(BundleUpgradeParser.stripQuotes("'a'"), "a");
        assertEquals(BundleUpgradeParser.stripQuotes("\"\""), "");
        assertEquals(BundleUpgradeParser.stripQuotes("''"), "");
    }
    
    private Bundle newMockBundle(Map<String, String> rawHeaders) {
        return newMockBundle(VersionedName.fromString("do.no.care:1.2.3"), rawHeaders);
    }

    private Bundle newMockBundle(VersionedName name) {
        return newMockBundle(name, ImmutableMap.of());
    }
    
    private Bundle newMockBundle(VersionedName name, Map<String, String> rawHeaders) {
        Dictionary<String, String> headers = new Hashtable<>(rawHeaders);
        Bundle result;
        try {
            result = Mockito.mock(Bundle.class);
        } catch (Exception e) {
            throw new IllegalStateException("Java too old.  There is a bug in really early java 1.8.0 "
                + "that causes mocks to fail, and has probably caused this.", e);
        }
        Mockito.when(result.getHeaders()).thenReturn(headers);
        Mockito.when(result.getSymbolicName()).thenReturn(name.getSymbolicName());
        Mockito.when(result.getVersion()).thenReturn(Version.valueOf(name.getOsgiVersionString()));
        return result;
    }

    private RegisteredType newMockRegisteredType(String symbolicName, String version) {
        RegisteredType result = Mockito.mock(RegisteredType.class);
        Mockito.when(result.getSymbolicName()).thenReturn(symbolicName);
        Mockito.when(result.getVersion()).thenReturn(version);
        Mockito.when(result.getVersionedName()).thenReturn(new VersionedName(symbolicName, version));
        return result;
    }

    private CatalogItem<?,?> newMockCatalogItem(String symbolicName, String version) {
        CatalogItem<?,?> result = Mockito.mock(CatalogItem.class);
        Mockito.when(result.getSymbolicName()).thenReturn(symbolicName);
        Mockito.when(result.getVersion()).thenReturn(version);
        Mockito.when(result.getId()).thenReturn(symbolicName+":"+version);
        Mockito.when(result.getCatalogItemId()).thenReturn(symbolicName+":"+version);
        return result;
    }
    
    private void assertParseList(String input, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseVersionRangedNameList(input, false, MutableList.of("WILCARD-NAME"), "0-WILDCARD_VERSION");
        assertListsEqual(actual, expected);
    }
    
    private void assertParseForceRemoveBundlesHeader(String input, Bundle bundle, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseForceRemoveBundlesHeader(input, bundle);
        assertListsEqual(actual, expected);
    }

    private void assertParseForceRemoveLegacyItemsHeader(String input, Bundle bundle, Supplier<? extends Iterable<? extends RegisteredType>> typeSupplier, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseForceRemoveLegacyItemsHeader(input, bundle, typeSupplier);
        assertListsEqual(actual, expected);
    }

    private void assertListsEqual(List<VersionRangedName> actual, List<VersionRangedName> expected) throws Exception {
        String errMsg = "actual="+actual;
        assertEquals(actual.size(), expected.size(), errMsg); 
        for (int i = 0; i < actual.size(); i++) {
            assertEquals(actual.get(i).getSymbolicName(), expected.get(i).getSymbolicName(), errMsg);
            assertEquals(actual.get(i).getOsgiVersionRange(), expected.get(i).getOsgiVersionRange(), errMsg);
        }
    }

    private void assertVersionRangedNameFails(String input, String expectedFailure, String... optionalOtherExpectedFailures) {
        try {
            VersionRangedName.fromString(input, false);
            Asserts.shouldHaveFailedPreviously();
        } catch (IllegalArgumentException e) {
            Asserts.expectedFailureContains(e, expectedFailure, optionalOtherExpectedFailures);
        }
    }
}
