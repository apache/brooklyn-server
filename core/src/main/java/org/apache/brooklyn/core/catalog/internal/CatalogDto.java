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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

@Beta
public class CatalogDto {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CatalogDto.class);

    String id;
    String contentsDescription;
    String name;
    String description;
    CatalogClasspathDto classpath;
    private List<CatalogItemDtoAbstract<?,?>> entries = null;
    
    // for thread-safety, any dynamic additions to this should be handled by a method 
    // in this class which does copy-on-write
    List<CatalogDto> catalogs = null;

    public static CatalogDto newDefaultLocalScanningDto(CatalogClasspathDo.CatalogScanningModes scanMode) {
        CatalogDo result = new CatalogDo(
                newNamedInstance("Local Scanned Catalog", "All annotated Brooklyn entities detected in the default classpath", "scanning-local-classpath") );
        result.setClasspathScanForEntities(scanMode);
        return result.dto;
    }

    /**
     * Creates a DTO.
     * <p>
     * The way contents is treated may change; thus this (and much of catalog) should be treated as beta.
     * 
     * @param name
     * @param description
     * @param optionalContentsDescription optional description of contents; if null, we normally expect source 'contents' to be set later;
     *   if the DTO has no 'contents' (ie XML source) then a description should be supplied so we know who is populating it
     *   (e.g. manual additions); without this, warnings may be generated
     *   
     * @return a new Catalog DTO
     */
    public static CatalogDto newNamedInstance(String name, String description, String optionalContentsDescription) {
        CatalogDto result = new CatalogDto();
        result.name = name;
        result.description = description;
        if (optionalContentsDescription!=null) result.contentsDescription = optionalContentsDescription;
        return result;
    }

    /** Used when caller wishes to create an explicitly empty catalog */
    public static CatalogDto newEmptyInstance(String optionalContentsDescription) {
        CatalogDto result = new CatalogDto();
        if (optionalContentsDescription!=null) result.contentsDescription = optionalContentsDescription;
        return result;
    }

    /** @deprecated since 0.7.0 use {@link #newDtoFromCatalogItems(Collection, String)}, supplying a description for tracking */
    @Deprecated
    public static CatalogDto newDtoFromCatalogItems(Collection<CatalogItem<?, ?>> entries) {
        return newDtoFromCatalogItems(entries, null);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static CatalogDto newDtoFromCatalogItems(Collection<CatalogItem<?, ?>> entries, String description) {
        CatalogDto result = new CatalogDto();
        result.contentsDescription = description;
        // Weird casts because compiler does not seem to like
        // .copyInto(Lists.<CatalogItemDtoAbstract<?, ?>>newArrayListWithExpectedSize(entries.size()));
        result.entries = (List) FluentIterable.from(entries)
                .filter(CatalogItemDtoAbstract.class)
                .copyInto(Lists.newArrayListWithExpectedSize(entries.size()));
        return result;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .omitNullValues()
                .add("name", name)
                .add("id", id)
                .add("contentsDescription", contentsDescription)
                .toString();
    }

    // temporary fix for issue where entries might not be unique
    Iterable<CatalogItemDtoAbstract<?, ?>> getUniqueEntries() {
        if (entries==null) return null;
        Map<String, CatalogItemDtoAbstract<?, ?>> result = getEntriesMap();
        return result.values();
    }

    private Map<String, CatalogItemDtoAbstract<?, ?>> getEntriesMap() {
        if (entries==null) return null;
        Map<String,CatalogItemDtoAbstract<?, ?>> result = MutableMap.of();
        for (CatalogItemDtoAbstract<?,?> entry: entries) {
            result.put(entry.getId(), entry);
        }
        return result;
    }

    void removeEntry(CatalogItemDtoAbstract<?, ?> entry) {
        if (entries!=null)
            entries.remove(entry);
    }

    void addEntry(CatalogItemDtoAbstract<?, ?> entry) {
        if (entries == null) entries = MutableList.of();
        CatalogItemDtoAbstract<?, ?> oldEntry = getEntriesMap().get(entry.getId());
        entries.add(entry);
        if (oldEntry!=null)
            removeEntry(oldEntry);
    }

}
