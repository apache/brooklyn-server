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
package org.apache.brooklyn.core.config;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.internal.StringSystemProperty;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.StringEscapes.BashStringEscapes;
import org.apache.brooklyn.util.text.Strings;

public final class Sanitizer {

    public static final ConfigKey<List<String>> SENSITIVE_FIELDS_TOKENS = BrooklynServerConfig.SENSITIVE_FIELDS_TOKENS;
    public static final ConfigKey<Boolean> SENSITIVE_FIELDS_PLAINTEXT_BLOCKED = BrooklynServerConfig.SENSITIVE_FIELDS_PLAINTEXT_BLOCKED;

    /**
     * Names that, if they appear anywhere in an attribute/config/field
     * indicates that it may be private, so should not be logged etc.
     */
    public static final List<String> DEFAULT_SENSITIVE_FIELDS_TOKENS = ImmutableList.of(
            "password", 
            "passwd", 
            "credential", 
            "secret", 
            "private",
            "access.cert", 
            "access.key");

    /** @deprecated since 1.1 use {@link #DEFAULT_SENSITIVE_FIELDS_TOKENS} or {@link #getSensitiveFieldsTokens(Boolean)} */
    public static final List<String> SECRET_NAMES = DEFAULT_SENSITIVE_FIELDS_TOKENS;

    private static List<String> LAST_SENSITIVE_FIELDS_TOKENS = null;
    private static Boolean LAST_SENSITIVE_FIELDS_PLAINTEXT_BLOCKED = null;
    private static long LAST_SENSITIVE_FIELDS_LOAD_TIME = -1;
    private static long LAST_SENSITIVE_FIELDS_CACHE_MILLIS = 60*1000;

    private static final void refreshProperties(Boolean refresh) {
        if (Boolean.FALSE.equals(refresh) ||
                (refresh==null && (LAST_SENSITIVE_FIELDS_LOAD_TIME + LAST_SENSITIVE_FIELDS_CACHE_MILLIS > System.currentTimeMillis()))) {
            return;
        }
        synchronized (Sanitizer.class) {
            if (LAST_SENSITIVE_FIELDS_LOAD_TIME < 0) {
                refresh = true;
            }
            if (refresh == null) {
                refresh = LAST_SENSITIVE_FIELDS_LOAD_TIME + LAST_SENSITIVE_FIELDS_CACHE_MILLIS < System.currentTimeMillis();
            }
            if (refresh) {
                ManagementContext mgmt = Osgis.getManagementContext();
                List<String> tokens = null;
                Boolean plaintextBlocked = null;
                if (mgmt != null) {
                    tokens = mgmt.getConfig().getConfig(SENSITIVE_FIELDS_TOKENS);
                    plaintextBlocked = mgmt.getConfig().getConfig(SENSITIVE_FIELDS_PLAINTEXT_BLOCKED);
                }

                if (tokens==null) {
                    StringSystemProperty tokensSP = new StringSystemProperty(SENSITIVE_FIELDS_TOKENS.getName());
                    if (tokensSP.isNonEmpty()) {
                        tokens = TypeCoercions.coerce(tokensSP.getValue(), SENSITIVE_FIELDS_TOKENS.getTypeToken());
                    }
                }
                if (tokens==null) {
                    tokens = DEFAULT_SENSITIVE_FIELDS_TOKENS;
                }

                if (plaintextBlocked==null) {
                    StringSystemProperty plaintextSP = new StringSystemProperty(SENSITIVE_FIELDS_PLAINTEXT_BLOCKED.getName());
                    if (plaintextSP.isNonEmpty()) {
                        plaintextBlocked = TypeCoercions.coerce(plaintextSP.getValue(), SENSITIVE_FIELDS_PLAINTEXT_BLOCKED.getTypeToken());
                    }
                }
                if (plaintextBlocked==null) {
                    plaintextBlocked = Boolean.FALSE;
                }

                LAST_SENSITIVE_FIELDS_TOKENS = tokens.stream().map(String::toLowerCase).collect(Collectors.toList());
                LAST_SENSITIVE_FIELDS_PLAINTEXT_BLOCKED = plaintextBlocked;
                LAST_SENSITIVE_FIELDS_LOAD_TIME = System.currentTimeMillis();
            }
        }
    }

    public static List<String> getSensitiveFieldsTokens() {
        return getSensitiveFieldsTokens(null);
    }
    public static List<String> getSensitiveFieldsTokens(Boolean refresh) {
        refreshProperties(refresh);
        return LAST_SENSITIVE_FIELDS_TOKENS;
    }

    public static boolean isSensitiveFieldsPlaintextBlocked() {
        return isSensitiveFieldsPlaintextBlocked(null);
    }
    public static boolean isSensitiveFieldsPlaintextBlocked(Boolean refresh) {
        refreshProperties(refresh);
        return LAST_SENSITIVE_FIELDS_PLAINTEXT_BLOCKED;
    }

    public static final Predicate<Object> IS_SECRET_PREDICATE = new IsSecretPredicate();

    public static Object suppressIfSecret(Object key, Object value) {
        if (Sanitizer.IS_SECRET_PREDICATE.apply(key)) {
            return suppress(value);
        }
        return value;
    }

    public static String suppress(Object value) {
        // only include the first few chars so that malicious observers can't uniquely brute-force discover the source
        String md5Checksum = Strings.maxlen(Streams.getMd5Checksum(new ByteArrayInputStream(("" + value).getBytes())), 8);
        return "<suppressed> (MD5 hash: " + md5Checksum + ")";
    }

    public static String suppressIfSecret(Object key, String value) {
        if (Sanitizer.IS_SECRET_PREDICATE.apply(key)) {
            return suppress(value);
        }
        return value;
    }

    /** replace any line matching  'secret: value' or 'secret = value' with eg 'secret = <suppressed> [MD5 hash: ... ]' */
    public static String sanitizeMultilineString(String input) {
        if (input==null) return null;
        return Arrays.stream(input.split("\n")).map(line -> {
            Integer first = Arrays.asList(line.indexOf("="), line.indexOf(":")).stream().filter(x -> x>0).min(Integer::compare).orElse(null);
            if (first!=null) {
                String key = line.substring(0, first);
                if (IS_SECRET_PREDICATE.apply(key)) {
                    String value = line.substring(first+1);
                    return key + line.substring(first, first+1) +
                            (Strings.isBlank(value) ? value : " " + suppress(value.trim()));
                }
            }
            return line;
        }).collect(Collectors.joining("\n"));
    }

    public static void sanitizeMapToString(Map<?, ?> env, StringBuilder sb) {
        if (env!=null) {
            for (Map.Entry<?, ?> kv : env.entrySet()) {
                String stringValue = kv.getValue() != null ? kv.getValue().toString() : "";
                if (!stringValue.isEmpty()) {
                    stringValue = Sanitizer.suppressIfSecret(kv.getKey(), stringValue);
                    stringValue = BashStringEscapes.wrapBash(stringValue);
                }
                sb.append(kv.getKey()).append("=").append(stringValue).append("\n");
            }
        }
    }

    /** applies to strings, sets, lists, maps */
    public static <K> K sanitizeJsonTypes(K obj) {
        return Sanitizer.newInstance().apply(obj);
    }

    private static class IsSecretPredicate implements Predicate<Object> {
        @Override
        public boolean apply(Object name) {
            if (name == null) return false;
            String lowerName = name.toString().toLowerCase();
            for (String secretName : getSensitiveFieldsTokens()) {
                if (lowerName.contains(secretName))
                    return true;
            }
            return false;
        }
    }

    /**
     * Kept only in case this anonymous inner class has made it into any persisted state.
     * 
     * @deprecated since 0.7.0
     */
    @Deprecated
    @SuppressWarnings("unused")
    private static final Predicate<Object> IS_SECRET_PREDICATE_DEPRECATED = new Predicate<Object>() {
        @Override
        public boolean apply(Object name) {
            if (name == null) return false;
            String lowerName = name.toString().toLowerCase();
            for (String secretName : getSensitiveFieldsTokens()) {
                if (lowerName.contains(secretName))
                    return true;
            }
            return false;
        }
    };

    public static Sanitizer newInstance(Predicate<Object> sanitizingNeededCheck) {
        return new Sanitizer(sanitizingNeededCheck);
    }
    
    public static Sanitizer newInstance(){
        return newInstance(IS_SECRET_PREDICATE);
    }

    public static Map<String, Object> sanitize(ConfigBag input) {
        return (input == null) ? null : sanitize(input.getAllConfig());
    }

    public static <K> Map<K, Object> sanitize(Map<K, ?> input) {
        return (input == null) ? null : sanitize(input, Sets.newHashSet());
    }

    static <K> Map<K, Object> sanitize(Map<K, ?> input, Set<Object> visited) {
        return (input == null) ? null : (Map) newInstance().apply(input, visited);
    }
    
    private Predicate<Object> predicate;

    private Sanitizer(Predicate<Object> sanitizingNeededCheck) {
        predicate = sanitizingNeededCheck;
    }

    public <K> K apply(K input) {
        return (input == null) ? null : apply(input, Sets.newLinkedHashSet());
    }

    private <K> K apply(K input, Set<Object> visited) {
        if (input==null) return null;

        // avoid endless loops if object is self-referential
        if (visited.contains(System.identityHashCode(input))) {
            return input;
        }

        visited.add(System.identityHashCode(input));

        if (input instanceof Map) {
            return (K) applyMap((Map)input, visited);
        } else if (input instanceof List) {
            return (K) applyList((List<?>) input, visited);
        } else if (input instanceof Set) {
            return (K) applySet((Set) input, visited);
        } else if (input instanceof String) {
            return (K) sanitizeMultilineString((String) input);
        } else {
            return (K) input;
        }
    }

    private <K> Map<K, Object> applyMap(Map<K, ?> input, Set<Object> visited) {
        Map<K, Object> result = Maps.newLinkedHashMap();
        for (Map.Entry<K, ?> e : input.entrySet()) {
            if (e.getKey() != null && predicate.apply(e.getKey())){
                result.put(e.getKey(), suppress(e.getValue()));
                continue;
            } 
            result.put(e.getKey(), apply(e.getValue(), visited));
        }
        return result;
    }
    
    private List<Object> applyIterable(Iterable<?> input, Set<Object> visited){
        List<Object> result = Lists.newArrayList();
        for(Object o : input){
            if(visited.contains(System.identityHashCode(o))){
                result.add(o);
                continue;
            }

            visited.add(System.identityHashCode(o));
            if (o instanceof Map) {
                result.add(apply((Map<?, ?>) o, visited));
            } else if (o instanceof List) {
                result.add(applyList((List<?>) o, visited));
            } else if (o instanceof Set) {
                result.add(applySet((Set<?>) o, visited));
            } else {
                result.add(o);
            }

        }
        return result;
    }
    
    private List<Object> applyList(List<?> input, Set<Object> visited) {
       return applyIterable(input, visited);
    }
    
    private Set<Object> applySet(Set<?> input, Set<Object> visited) {
        return MutableSet.copyOf(applyIterable(input, visited));
    }
}
