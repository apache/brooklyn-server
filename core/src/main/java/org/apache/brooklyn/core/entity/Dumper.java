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
package org.apache.brooklyn.core.entity;

import static org.apache.brooklyn.core.entity.Entities.isTrivial;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Convenience methods for dumping info about entities, locations, policies, etc.
 */
public class Dumper {

    public static void dumpInfo(Iterable<? extends Entity> entities) {
        for (Entity e : entities) {
            dumpInfo(e);
        }
    }

    public static void dumpInfo(Entity e) {
        try {
            dumpInfo(e, new PrintWriter(System.out), "", "  ");
        } catch (IOException exc) {
            // system.out throwing an exception is odd, so don't have IOException on signature
            throw new RuntimeException(exc);
        }
    }
    
    public static void dumpInfo(Entity e, Writer out) throws IOException {
        dumpInfo(e, out, "", "  ");
    }
    
    static void dumpInfo(Entity e, String currentIndentation, String tab) throws IOException {
        dumpInfo(e, new PrintWriter(System.out), currentIndentation, tab);
    }
    
    static void dumpInfo(Entity e, Writer out, String currentIndentation, String tab) throws IOException {
        out.append(currentIndentation+e.toString()+" "+e.getId()+"\n");

        out.append(currentIndentation+tab+tab+"displayName = "+e.getDisplayName()+"\n");
        if (Strings.isNonBlank(e.getCatalogItemId())) {
            out.append(currentIndentation+tab+tab+"catalogItemId = "+e.getCatalogItemId()+"\n");
        }
        final List<String> searchPath = e.getCatalogItemIdSearchPath();
        if (!searchPath.isEmpty()) {
            out.append(currentIndentation + tab + tab + "searchPath = [");
            for (int i = 0 ; i < searchPath.size() ; i++) {
                out.append(i > 0 ? ",\n" : "\n");
                out.append(currentIndentation + tab + tab + searchPath.get(i));
            }
            out.append("\n" + currentIndentation + tab + tab + "]");
        }

        out.append(currentIndentation+tab+tab+"locations = "+e.getLocations()+"\n");

        Set<ConfigKey<?>> keys = Sets.newLinkedHashSet(
            ((EntityInternal)e).config().getLocalBag().getAllConfigAsConfigKeyMap().keySet()
            //((EntityInternal)e).getConfigMap().getLocalConfig().keySet() 
            );
        for (ConfigKey<?> it : sortConfigKeys(keys)) {
            // use the official config key declared on the type if available
            // (since the map sometimes contains <object> keys
            ConfigKey<?> realKey = e.getEntityType().getConfigKey(it.getName());
            if (realKey!=null) it = realKey;

            Maybe<Object> mv = ((EntityInternal)e).config().getLocalRaw(it);
            if (!isTrivial(mv)) {
                Object v = mv.get();
                out.append(currentIndentation+tab+tab+it.getName());
                out.append(" = ");
                if (isSecret(it.getName())) out.append("xxxxxxxx");
                else if ((v instanceof Task) && ((Task<?>)v).isDone()) {
                    if (((Task<?>)v).isError()) {
                        out.append("ERROR in "+v);
                    } else {
                        try {
                            out.append(((Task<?>)v).get() + " (from "+v+")");
                        } catch (ExecutionException ee) {
                            throw new IllegalStateException("task "+v+" done and !isError, but threw exception on get", ee);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                } else out.append(""+v);
                out.append("\n");
            }
        }

        for (Sensor<?> it : sortSensors(e.getEntityType().getSensors())) {
            if (it instanceof AttributeSensor) {
                Object v = e.getAttribute((AttributeSensor<?>)it);
                if (!isTrivial(v)) {
                    out.append(currentIndentation+tab+tab+it.getName());
                    out.append(": ");
                    if (isSecret(it.getName())) out.append("xxxxxxxx");
                    else out.append(""+v);
                    out.append("\n");
                }
            }
        }

        if (e instanceof Group) {
            StringBuilder members = new StringBuilder();
            for (Entity it : ((Group)e).getMembers()) {
                if (members.length()>0) members.append(", ");
                members.append(it.getId());
            }
            out.append(currentIndentation+tab+tab+"Members: "+members.toString()+"\n");
        }

        if (!e.policies().isEmpty()) {
            out.append(currentIndentation+tab+tab+"Policies:\n");
            for (Policy policy : e.policies()) {
                dumpInfo(policy, out, currentIndentation+tab+tab+tab, tab);
            }
        }

        if (!e.enrichers().isEmpty()) {
            out.append(currentIndentation+tab+tab+"Enrichers:\n");
            for (Enricher enricher : e.enrichers()) {
                dumpInfo(enricher, out, currentIndentation+tab+tab+tab, tab);
            }
        }

        if (!((EntityInternal)e).feeds().getFeeds().isEmpty()) {
            out.append(currentIndentation+tab+tab+"Feeds:\n");
            for (Feed feed : ((EntityInternal)e).feeds().getFeeds()) {
                dumpInfo(feed, out, currentIndentation+tab+tab+tab, tab);
            }
        }

        for (Entity it : e.getChildren()) {
            dumpInfo(it, out, currentIndentation+tab, tab);
        }

        out.flush();
    }

    public static void dumpInfo(Location loc) {
        try {
            dumpInfo(loc, new PrintWriter(System.out), "", "  ");
        } catch (IOException exc) {
            // system.out throwing an exception is odd, so don't have IOException on signature
            throw new RuntimeException(exc);
        }
    }
    
    public static void dumpInfo(Location loc, Writer out) throws IOException {
        dumpInfo(loc, out, "", "  ");
    }
    
    static void dumpInfo(Location loc, String currentIndentation, String tab) throws IOException {
        dumpInfo(loc, new PrintWriter(System.out), currentIndentation, tab);
    }
    
    @SuppressWarnings("rawtypes")
    static void dumpInfo(Location loc, Writer out, String currentIndentation, String tab) throws IOException {
        out.append(currentIndentation+loc.toString()+"\n");

        for (Object entryO : ((LocationInternal)loc).config().getBag().getAllConfig().entrySet()) {
            Map.Entry entry = (Map.Entry)entryO;
            Object keyO = entry.getKey();
            String key =
                    keyO instanceof HasConfigKey ? ((HasConfigKey)keyO).getConfigKey().getName() :
                    keyO instanceof ConfigKey ? ((ConfigKey)keyO).getName() :
                    keyO == null ? null :
                    keyO.toString();
            Object val = entry.getValue();
            if (!isTrivial(val)) {
                out.append(currentIndentation+tab+tab+key);
                out.append(" = ");
                if (isSecret(key)) out.append("xxxxxxxx");
                else out.append(""+val);
                out.append("\n");
            }
        }

        for (Map.Entry<String,?> entry : sortMap(FlagUtils.getFieldsWithFlags(loc)).entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (!isTrivial(val)) {
                out.append(currentIndentation+tab+tab+key);
                out.append(" = ");
                if (isSecret(key)) out.append("xxxxxxxx");
                else out.append(""+val);
                out.append("\n");
            }
        }

        for (Location it : loc.getChildren()) {
            dumpInfo(it, out, currentIndentation+tab, tab);
        }

        out.flush();
    }

    public static void dumpInfo(Enricher enr) {
        try {
            dumpInfo(enr, new PrintWriter(System.out), "", "  ");
        } catch (IOException exc) {
            // system.out throwing an exception is odd, so don't have IOException on signature
            throw new RuntimeException(exc);
        }
    }
    
    public static void dumpInfo(Enricher enr, Writer out) throws IOException {
        dumpInfo(enr, out, "", "  ");
    }
    
    static void dumpInfo(Enricher enr, String currentIndentation, String tab) throws IOException {
        dumpInfo(enr, new PrintWriter(System.out), currentIndentation, tab);
    }
    
    static void dumpInfo(Enricher enr, Writer out, String currentIndentation, String tab) throws IOException {
        out.append(currentIndentation+enr.toString()+"\n");

        for (ConfigKey<?> key : sortConfigKeys(enr.getEnricherType().getConfigKeys())) {
            Maybe<Object> val = ((BrooklynObjectInternal)enr).config().getRaw(key);
            if (!isTrivial(val)) {
                out.append(currentIndentation+tab+tab+key);
                out.append(" = ");
                if (isSecret(key.getName())) out.append("xxxxxxxx");
                else out.append(""+val.get());
                out.append("\n");
            }
        }

        out.flush();
    }
    
    static void dumpInfo(Feed feed, String currentIndentation, String tab) throws IOException {
        dumpInfo(feed, new PrintWriter(System.out), currentIndentation, tab);
    }
    
    static void dumpInfo(Feed feed, Writer out, String currentIndentation, String tab) throws IOException {
        out.append(currentIndentation+feed.toString()+"\n");

        // TODO create a FeedType cf EnricherType ?
        for (ConfigKey<?> key : sortConfigKeys(((BrooklynObjectInternal)feed).config().getBag().getAllConfigAsConfigKeyMap().keySet())) {
            Maybe<Object> val = ((BrooklynObjectInternal)feed).config().getRaw(key);
            if (!isTrivial(val)) {
                out.append(currentIndentation+tab+tab+key);
                out.append(" = ");
                if (isSecret(key.getName())) out.append("xxxxxxxx");
                else out.append(""+val.get());
                out.append("\n");
            }
        }

        out.flush();
    }

    public static void dumpInfo(Policy pol) {
        try {
            dumpInfo(pol, new PrintWriter(System.out), "", "  ");
        } catch (IOException exc) {
            // system.out throwing an exception is odd, so don't have IOException on signature
            throw new RuntimeException(exc);
        }
    }
    
    public static void dumpInfo(Policy pol, Writer out) throws IOException {
        dumpInfo(pol, out, "", "  ");
    }
    
    static void dumpInfo(Policy pol, String currentIndentation, String tab) throws IOException {
        dumpInfo(pol, new PrintWriter(System.out), currentIndentation, tab);
    }
    
    static void dumpInfo(Policy pol, Writer out, String currentIndentation, String tab) throws IOException {
        out.append(currentIndentation+pol.toString()+"\n");

        for (ConfigKey<?> key : sortConfigKeys(pol.getPolicyType().getConfigKeys())) {
            Maybe<Object> val = ((BrooklynObjectInternal)pol).config().getRaw(key);
            if (!isTrivial(val)) {
                out.append(currentIndentation+tab+tab+key);
                out.append(" = ");
                if (isSecret(key.getName())) out.append("xxxxxxxx");
                else out.append(""+val.get());
                out.append("\n");
            }
        }

        out.flush();
    }
    
    public static void dumpInfo(Task<?> t) {
        try {
            dumpInfo(t, new PrintWriter(System.out), "", "  ");
        } catch (IOException exc) {
            // system.out throwing an exception is odd, so don't have IOException on signature
            throw new RuntimeException(exc);
        }
    }
    
    public static void dumpInfo(Task<?> t, Writer out) throws IOException {
        dumpInfo(t, out, "", "  ");
    }
    
    static void dumpInfo(Task<?> t, String currentIndentation, String tab) throws IOException {
        dumpInfo(t, new PrintWriter(System.out), currentIndentation, tab);
    }
    
    static void dumpInfo(Task<?> t, Writer out, String currentIndentation, String tab) throws IOException {
        out.append(currentIndentation+t+": "+t.getStatusDetail(false)+"\n");

        if (t instanceof HasTaskChildren) {
            for (Task<?> child: ((HasTaskChildren)t).getChildren()) {
                dumpInfo(child, out, currentIndentation+tab, tab);
            }
        }
        out.flush();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static List<Sensor<?>> sortSensors(Set<Sensor<?>> sensors) {
        List result = new ArrayList(sensors);
        Collections.sort(result, new Comparator<Sensor>() {
                    @Override
                    public int compare(Sensor arg0, Sensor arg1) {
                        return arg0.getName().compareTo(arg1.getName());
                    }

        });
        return result;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static List<ConfigKey<?>> sortConfigKeys(Set<ConfigKey<?>> configs) {
        List result = new ArrayList(configs);
        Collections.sort(result, new Comparator<ConfigKey>() {
                    @Override
                    public int compare(ConfigKey arg0, ConfigKey arg1) {
                        return arg0.getName().compareTo(arg1.getName());
                    }

        });
        return result;
    }
    
    static <T> Map<String, T> sortMap(Map<String, T> map) {
        Map<String,T> result = Maps.newLinkedHashMap();
        List<String> order = Lists.newArrayList(map.keySet());
        Collections.sort(order, String.CASE_INSENSITIVE_ORDER);

        for (String key : order) {
            result.put(key, map.get(key));
        }
        return result;
    }
    
    private static boolean isSecret(String name) {
        return Sanitizer.IS_SECRET_PREDICATE.apply(name);
    }
}
