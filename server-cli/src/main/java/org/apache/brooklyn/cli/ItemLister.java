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
package org.apache.brooklyn.cli;

import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatform;
import org.apache.brooklyn.camp.spi.PlatformRootSummary;
import org.apache.brooklyn.cli.lister.ClassFinder;
import org.apache.brooklyn.cli.lister.ItemDescriptors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class ItemLister {
    
    private static final Logger LOG = LoggerFactory.getLogger(ItemLister.class);
    private static final String BASE = "brooklyn/item-lister";
    private static final String BASE_TEMPLATES = BASE+"/"+"templates";
    private static final String BASE_STATICS = BASE+"/"+"statics";

    @Command(name = "list-objects", description = "List Brooklyn objects (Entities, Policies, Enrichers and Locations)")
    public static class ListAllCommand extends AbstractMain.BrooklynCommandCollectingArgs {

        @Option(name = { "--jar" }, title = "JAR to scan", description = "A JAR file to scan. If a file (not a url) pointing at a directory, will include all JAR files in that directory. "
            + "Due to how classes are scanned the JARs for all classes up to and including brooklyn core must be included as arguments. "
            + "Argument can be supplied multiple times to scan multiple JARs. If not supplied and no YAML, this attempts to use the initial classpath")
        public List<String> jarsToScan = Lists.newLinkedList();
        
        @Option(name = { "--jars" }, title = "JAR to scan", description = "DEPRECATED: synonym for --jar")
        public List<String> jarsToScanOld = Lists.newLinkedList();

        @Option(name = { "--yaml" }, title = "YAML blueprint file", description = "A YAML blueprint file to parse. If a file is pointing at a directory, will include all YAML and BOM files in that directory")
        public List<String> yamlToScan = Lists.newLinkedList();

        @Option(name = { "--type-regex" }, title = "Regex to restrict the Java types loaded")
        public String typeRegex;

        @Option(name = { "--catalog-only" }, title = "When scanning JAR files, whether to include only items annotated with @Catalog (default true)")
        public boolean catalogOnly = true;

        @Option(name = { "--ignore-impls" }, title = "Ignore Entity class implementation where there is an Entity interface with @ImplementedBy (default true)")
        public boolean ignoreImpls = true;

        @Option(name = { "--headings-only" }, title = "Whether to only show name/type, and not config keys etc")
        public boolean headingsOnly = false;
        
        @Option(name = { "--output-folder" }, title = "If supplied, generate HTML pages in the given folder; otherwise only generates JSON")
        public String outputFolder;

        @SuppressWarnings("unchecked")
        @Override
        public Void call() throws Exception {
            Map<String, Object> result = MutableMap.of();

            result = populateDescriptors();
            String json = toJson(result);
            
            if (outputFolder == null) {
                System.out.println(json);

            } else {
                LOG.info("Outputting item list (size "+itemCount+") to " + outputFolder);
                String outputPath = Os.mergePaths(outputFolder, "index.html");
                String parentDir = (new File(outputPath).getParentFile()).getAbsolutePath();
                mkdir(parentDir, "entities");
                mkdir(parentDir, "policies");
                mkdir(parentDir, "enrichers");
                mkdir(parentDir, "locations");
                mkdir(parentDir, "locationResolvers"); //TODO nothing written here yet...

                mkdir(parentDir, "style");
                mkdir(Os.mergePaths(parentDir, "style"), "js");
                mkdir(Os.mergePaths(parentDir, "style", "js"), "catalog");
                mkdir(parentDir, "images");

                // create both a JS and a JSON object
                Files.write("var items = " + json, new File(Os.mergePaths(outputFolder, "items.js")), Charsets.UTF_8);
                Files.write(json, new File(Os.mergePaths(outputFolder, "items.json")), Charsets.UTF_8);
                ResourceUtils resourceUtils = ResourceUtils.create(this);

                // root - just loads the above JSON
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "brooklyn-object-list.html", "index.html");

                // statics - structure mirrors docs (not for any real reason however... the json is usually enough for our docs)
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "common.js");
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "items.css");
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "images/brooklyn.gif");
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "style/js/underscore-min.js");
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "style/js/underscore-min.map");
                copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, "style/js/catalog/typeahead.js");

                // now make pages for each item

                List<Map<String, Object>> entities = (List<Map<String, Object>>) result.get("entities");
                String entityTemplateHtml = resourceUtils.getResourceAsString(Urls.mergePaths(BASE_TEMPLATES, "entity.html"));
                for (Map<String, Object> entity : entities) {
                    String type = (String) entity.get("type");
                    String name = (String) entity.get("name");
                    String entityHtml = TemplateProcessor.processTemplateContents(entityTemplateHtml, ImmutableMap.of("type", type, "name", name));
                    Files.write(entityHtml, new File(Os.mergePaths(outputFolder, "entities", type + ".html")), Charsets.UTF_8);
                }

                List<Map<String, Object>> policies = (List<Map<String, Object>>) result.get("policies");
                String policyTemplateHtml = resourceUtils.getResourceAsString(Urls.mergePaths(BASE_TEMPLATES, "policy.html"));
                for (Map<String, Object> policy : policies) {
                    String type = (String) policy.get("type");
                    String name = (String) policy.get("name");
                    String policyHtml = TemplateProcessor.processTemplateContents(policyTemplateHtml, ImmutableMap.of("type", type, "name", name));
                    Files.write(policyHtml, new File(Os.mergePaths(outputFolder, "policies", type + ".html")), Charsets.UTF_8);
                }

                List<Map<String, Object>> enrichers = (List<Map<String, Object>>) result.get("enrichers");
                String enricherTemplateHtml = resourceUtils.getResourceAsString(Urls.mergePaths(BASE_TEMPLATES, "enricher.html"));
                for (Map<String, Object> enricher : enrichers) {
                    String type = (String) enricher.get("type");
                    String name = (String) enricher.get("name");
                    String enricherHtml = TemplateProcessor.processTemplateContents(enricherTemplateHtml, ImmutableMap.of("type", type, "name", name));
                    Files.write(enricherHtml, new File(Os.mergePaths(outputFolder, "enrichers", type + ".html")), Charsets.UTF_8);
                }

                List<Map<String, Object>> locations = (List<Map<String, Object>>) result.get("locations");
                String locationTemplateHtml = resourceUtils.getResourceAsString(Urls.mergePaths(BASE_TEMPLATES, "location.html"));
                for (Map<String, Object> location : locations) {
                    String type = (String) location.get("type");
                    String locationHtml = TemplateProcessor.processTemplateContents(locationTemplateHtml, ImmutableMap.of("type", type));
                    Files.write(locationHtml, new File(Os.mergePaths(outputFolder, "locations", type + ".html")), Charsets.UTF_8);
                }
                LOG.info("Finished outputting item list to " + outputFolder);
            }
            return null;
        }

        protected List<String> getJars() {
            return MutableList.<String>builder().addAll(jarsToScan).addAll(jarsToScanOld).build();
        }
        
        protected Map<String, Object> populateDescriptors() throws MalformedURLException, IOException {
            Map<String, Object> result;
            List<Map<?,?>> entities = new ArrayList<>();
            List<Map<?,?>> policies = new ArrayList<>();
            List<Map<?,?>> enrichers = new ArrayList<>();
            List<Map<?,?>> locations = new ArrayList<>();
            List<Object> locationResolvers = new ArrayList<>();

            if (!getJars().isEmpty() || yamlToScan.isEmpty()) {
                List<URL> urls = getJarUrls();
                LOG.info("Retrieving classes from "+urls);
                
                // TODO Remove duplication from separate ListPolicyCommand etc
                entities.addAll(ItemDescriptors.toItemDescriptors(getTypes(urls, Entity.class), headingsOnly, "name"));
                policies.addAll(ItemDescriptors.toItemDescriptors(getTypes(urls, Policy.class), headingsOnly, "name"));
                enrichers.addAll(ItemDescriptors.toItemDescriptors(getTypes(urls, Enricher.class), headingsOnly, "name"));
                locations.addAll(ItemDescriptors.toItemDescriptors(getTypes(urls, Location.class, false), headingsOnly, "type"));
                locationResolvers.addAll(ItemDescriptors.toItemDescriptors(ImmutableList.copyOf(ServiceLoader.load(LocationResolver.class)), true));
            }
            if (!yamlToScan.isEmpty()) {
                List<URL> urls = getYamlUrls();
                LOG.info("Retrieving items from "+urls);
                
                LocalManagementContext lmgmt = new LocalManagementContext(BrooklynProperties.Factory.newEmpty());
                @SuppressWarnings("unused")
                BrooklynCampPlatform platform = new BrooklynCampPlatform(
                        PlatformRootSummary.builder().name("Brooklyn CAMP Platform").build(),lmgmt)
                        .setConfigKeyAtManagmentContext();
                BrooklynCatalog catalog = lmgmt.getCatalog();

                for (URL url: urls) {
                    String yamlContent = Streams.readFullyString(url.openStream());

                    Iterable<? extends CatalogItem<?, ?>> items = catalog.addItems(yamlContent);
                    for (CatalogItem<?,?> item: items) {
                        Map<String,Object> itemDescriptor = ItemDescriptors.toItemDescriptor(catalog, item, headingsOnly);

                        itemCount++;
                        if (item.getCatalogItemType() == CatalogItem.CatalogItemType.ENTITY || item.getCatalogItemType() == CatalogItem.CatalogItemType.TEMPLATE) {
                            entities.add(itemDescriptor);
                        } else if (item.getCatalogItemType() == CatalogItem.CatalogItemType.POLICY) {
                            policies.add(itemDescriptor);
                        } else if (item.getCatalogItemType() == CatalogItem.CatalogItemType.LOCATION) {
                            locations.add(itemDescriptor);
                        } else {
                            LOG.warn("Skipping unknown catalog item type "+item.getCatalogItemType()+": "+item);
                            itemCount--;
                        }
                    }
                    
                }
                Entities.destroyAll(lmgmt);
            }
            
            result = ImmutableMap.<String, Object>builder()
                    .put("entities", entities)
                    .put("policies", policies)
                    .put("enrichers", enrichers)
                    .put("locations", locations)
                    .put("locationResolvers", locationResolvers)
                    .build();
            return result;
        }

        private void copyFromItemListerClasspathBaseStaticsToOutputDir(ResourceUtils resourceUtils, String item) throws IOException {
            copyFromItemListerClasspathBaseStaticsToOutputDir(resourceUtils, item, item);
        }
        private void copyFromItemListerClasspathBaseStaticsToOutputDir(ResourceUtils resourceUtils, String item, String dest) throws IOException {
            String js = resourceUtils.getResourceAsString(Urls.mergePaths(BASE_STATICS, item));
            Files.write(js, new File(Os.mergePaths(outputFolder, dest)), Charsets.UTF_8);
        }

        private void mkdir(String rootDir, String dirName) {
            (new File(Os.mergePaths(rootDir, dirName))).mkdirs();
        }

        protected List<URL> getJarUrls() throws MalformedURLException, IOException {
            List<URL> urls = Lists.newArrayList();
            if (!getJars().isEmpty()) {
                for (String jar : getJars()) {
                    List<URL> expanded = ClassFinder.toJarUrls(jar);
                    if (expanded.isEmpty())
                        LOG.warn("No jars found at: "+jar);
                    urls.addAll(expanded);
                }
            } else if (yamlToScan.isEmpty()) {
                // NB: there is a better way; see comments on getTypes
                String classpath = System.getenv("INITIAL_CLASSPATH");
                if (Strings.isNonBlank(classpath)) {
                    List<String> entries = Splitter.on(":").omitEmptyStrings().trimResults().splitToList(classpath);
                    for (String entry : entries) {
                        if (entry.endsWith(".jar") || entry.endsWith("/*")) {
                            urls.addAll(ClassFinder.toJarUrls(entry.replace("/*", "")));
                        }
                    }
                } else {
                    throw new FatalConfigurationRuntimeException("No JARs to process and could not infer from INITIAL_CLASSPATH env var.");
                }
            }
            return urls;
        }
        protected List<URL> getYamlUrls() throws MalformedURLException, IOException {
            List<URL> urls = Lists.newArrayList();
            if (!yamlToScan.isEmpty()) {
                for (String y: yamlToScan) {
                    File yamlFolder = new File(y);
                    if (yamlFolder.isDirectory()) {
                        File[] fileList = yamlFolder.listFiles();

                        for (File file : fileList) {
                            if (file.isFile() && (file.getName().toLowerCase().endsWith(".yaml") || file.getName().toLowerCase().endsWith(".bom"))) {
                                urls.add(file.toURI().toURL());
                            }
                        }
                    } else {
                        urls.add(new File(y).toURI().toURL());
                    }
                }
            }
            return urls;
        }

        private <T extends BrooklynObject> List<Class<? extends T>> getTypes(List<URL> urls, Class<T> type) {
            return getTypes(urls, type, null);
        }

        int itemCount = 0;
        
        private <T extends BrooklynObject> List<Class<? extends T>> getTypes(List<URL> urls, Class<T> type, Boolean catalogOnlyOverride) {
            // TODO this only really works if you give it lots of URLs - see comment on "--jar" argument
            // NB if the ReflectionScanner class is given "null" then it will scan, better than INITIAL_CLASSPATH 
            FluentIterable<Class<? extends T>> fluent = FluentIterable.from(ClassFinder.findClasses(urls, type));
            if (typeRegex != null) {
                fluent = fluent.filter(ClassFinder.withClassNameMatching(typeRegex));
            }
            if (catalogOnlyOverride == null ? catalogOnly : catalogOnlyOverride) {
                fluent = fluent.filter(ClassFinder.withAnnotation(Catalog.class));
            }
            List<Class<? extends T>> filtered = fluent.toList();
            Collection<Class<? extends T>> result;
            if (ignoreImpls) {
                result = MutableSet.copyOf(filtered);
                for (Class<? extends T> clazz : filtered) {
                    ImplementedBy implementedBy = clazz.getAnnotation(ImplementedBy.class);
                    if (implementedBy != null) {
                        result.remove(implementedBy.value());
                    }
                }
            } else {
                result = filtered;
            }
            itemCount += result.size();
            return ImmutableList.copyOf(result);
        }
        
        private String toJson(Object obj) throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper()
                    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .enable(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
                    .enable(SerializationFeature.INDENT_OUTPUT)
                    .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
                    .setSerializationInclusion(JsonInclude.Include.ALWAYS)
            
                    // Only serialise annotated fields
                    .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                    .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            
            return objectMapper.writeValueAsString(obj);
        }
    }
}
