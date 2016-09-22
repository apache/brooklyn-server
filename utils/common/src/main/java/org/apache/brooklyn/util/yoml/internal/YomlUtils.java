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
package org.apache.brooklyn.util.yoml.internal;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.FieldOrderings;
import org.apache.brooklyn.util.javalang.ReflectionPredicates;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;
import org.apache.brooklyn.util.yoml.annotations.DefaultKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

public class YomlUtils {

    private static final Logger log = LoggerFactory.getLogger(YomlUtils.class);
    
    /** true iff k1 and k2 are case-insensitively equal after removing all - and _.
     * Note that the definition of mangling may change.
     * TODO it should be stricter so that "ab" and "a-b" don't match but "aB" and "a-b" and "a_b" do */
    @Beta
    public static boolean mangleable(String k1, String k2) {
        if (k1==null || k2==null) return k1==k2;
        k1 = Strings.replaceAllNonRegex(k1, "-", "");
        k1 = Strings.replaceAllNonRegex(k1, "_", "");
        k2 = Strings.replaceAllNonRegex(k2, "-", "");
        k2 = Strings.replaceAllNonRegex(k2, "_", "");
        return k1.toLowerCase().equals(k2.toLowerCase());
    }

    /** type marker that value can be kept in its as-read form */
    public final static String TYPE_JSON = "json";
    
    public final static String TYPE_STRING = "string"; 
    public final static String TYPE_OBJECT = "object"; 
    public final static String TYPE_MAP = "map"; 
    public final static String TYPE_LIST = "list";
    public final static String TYPE_SET = "set";

    public final static class JsonMarker {
        public static final String TYPE = TYPE_JSON;

        /** true IFF o is a json primitive or map/iterable consisting of pure json items,
         * with the additional constraint that map keys must be strings */
        public static boolean isPureJson(Object o) {
            if (o==null || Boxing.isPrimitiveOrBoxedObject(o)) return true;
            if (o instanceof String) return true;
            if (o instanceof Iterable) {
                for (Object oi: ((Iterable<?>)o)) {
                    if (!isPureJson(oi)) return false;
                }
                return true;
            }
            if (o instanceof Map) {
                for (Map.Entry<?,?> oi: ((Map<?,?>)o).entrySet()) {
                    if (!(oi.getKey() instanceof String)) return false;
                    if (!isPureJson(oi.getValue())) return false;
                }
                return true;
            }
            return false;
        } 
    }

    /** parses a type string and if it is generic it gives access to the underlying types */
    public static class GenericsParse {
        public String warning;
        public boolean isGeneric = false;
        public String baseType;
        public List<String> subTypes = MutableList.of();
        
        public GenericsParse(String type) {
            if (type==null) return;
            
            baseType = type.trim();
            int genericStart = baseType.indexOf('<');
            if (genericStart > 0) {
                isGeneric = true;
                
                if (!parse(baseType.substring(genericStart))) {
                    warning = "Invalid generic type "+baseType;
                    return;
                }
                
                baseType = baseType.substring(0, genericStart);
            }
        }

        private boolean parse(String s) {
            int depth = 0;
            boolean inWord = false;
            int lastWordStart = -1;
            for (int i=0; i<s.length(); i++) {
                char c = s.charAt(i);
                if (c=='<') { depth++; continue; }
                if (Character.isWhitespace(c)) continue;
                if (c==',' || c=='>') {
                    if (c==',' && depth==0) return false;
                    if (c=='>') { depth--; }
                    if (depth>1) continue;
                    // depth 1 word end, either due to , or due to >
                    if (c==',' && !inWord) return false;
                    subTypes.add(s.substring(lastWordStart, i).trim());
                    inWord = false;
                    continue;
                }
                if (!inWord) {
                    if (depth!=1) return false;
                    inWord = true;
                    lastWordStart = i;
                }
            }
            // finished. expect depth 0 and not in word
            return depth==0 && !inWord;
        }

        public boolean isGeneric() { return isGeneric; }
        public int subTypeCount() { return subTypes.size(); }
    }

    public static String getTypeNameWithGenerics(TypeToken<?> t, YomlTypeRegistry tr) {
        return getTypeNameWithGenerics(t.getType(), tr);
    }
    
    @SuppressWarnings("serial") private static class CannotResolveGenerics extends IllegalStateException {}
    private final static Set<String> WARNED_ON_UNSUPPORTED_GENERICS = MutableSet.of();  
    
    public static String getTypeNameWithGenerics(Type t, YomlTypeRegistry tr) {
        if (t==null) return null;
        
        if (t instanceof ParameterizedType) {
            String result = getTypeNameWithGenerics( ((ParameterizedType)t).getRawType(), tr );
            try {
                StringBuilder sb = new StringBuilder(result);
                Type[] args = ((ParameterizedType)t).getActualTypeArguments();
                if (args==null || args.length==0) {
                    // nothing
                } else {
                    sb.append("<");
                    sb.append(getTypeNameWithGenerics( args[0], tr ));
                    for (int i=1; i<args.length; i++) {
                        sb.append(",");
                        sb.append(getTypeNameWithGenerics( args[i], tr ));
                    }
                    sb.append(">");
                }
                return sb.toString();
            } catch (CannotResolveGenerics e) {
                // fall back to non-generic
                return result;
            }
        }
        
        if (t instanceof Class) {
            return tr.getTypeNameOfClass((Class<?>)t);
        }
        
        // don't support WilcardType, BoundedType, or arrays
        String tn = t.getClass().getName();
        if (WARNED_ON_UNSUPPORTED_GENERICS.contains(tn)) {
            log.warn("Unsupported generic type: "+t+" ("+tn+"), falling back to raw type (only logging once)");
        }
        throw new CannotResolveGenerics();
    }
    
    public static <T> Map<String,Field> getAllNonTransientNonStaticFields(Class<T> type, T optionalInstanceToRequireNonNullFieldValue) {
        return getAllFields(type, optionalInstanceToRequireNonNullFieldValue,
            Predicates.and(ReflectionPredicates.IS_FIELD_NON_TRANSIENT, ReflectionPredicates.IS_FIELD_NON_STATIC));
    }
    public static <T> Map<String,Field> getAllNonTransientStaticFields(Class<T> type) {
        return getAllFields(type, null,
            Predicates.and(ReflectionPredicates.IS_FIELD_NON_TRANSIENT, ReflectionPredicates.IS_FIELD_STATIC));
    }
    
    /** Finds all fields on a type, including inherited, including statics from interfaces, subject to the optional filter,
     * and optionally requiring a non-null value for the field on a given instant.
     * These are ordered in {@link FieldOrderings#ALPHABETICAL_FIELD_THEN_SUB_BEST_FIRST} order,
     * with shadowed fields prefixed by the name of the superclass and ".".
     * 
     * @param type Class to scan
     * @param optionalInstanceToRequireNonNullFieldValue An instance, which if supplied, is used to exclude
     *   fields for which this instance has a null value
     * @param filter Filter to apply on fields
     * @return
     */
    public static <T> Map<String,Field> getAllFields(Class<T> type, @Nullable T optionalInstanceToRequireNonNullFieldValue, @Nullable Predicate<Field> filter) {
        Map<String,Field> result = MutableMap.of();
        List<Field> fields = Reflections.findFields(type, 
            null,
            FieldOrderings.ALPHABETICAL_FIELD_THEN_SUB_BEST_FIRST);
        Field lastF = null;
        for (Field f: fields) {
            if (filter==null || filter.apply(f)) {
                if (optionalInstanceToRequireNonNullFieldValue==null || 
                        Reflections.getFieldValueMaybe(optionalInstanceToRequireNonNullFieldValue, f).isPresentAndNonNull()) {
                    String name = f.getName();
                    if (lastF!=null && lastF.getName().equals(f.getName())) {
                        // if field is shadowed use FQN
                        String fqn = f.getDeclaringClass().getCanonicalName();
                        if (Strings.isBlank(fqn)) fqn = f.getDeclaringClass().getName();
                        name = fqn+"."+name;
                    }
                    result.put(name, f);
                }
            }
            lastF = f;
        }
        return result;
    }
    public static <T> List<String> getAllNonTransientNonStaticFieldNames(Class<T> type, T optionalInstanceToRequireNonNullFieldValue) {
        return MutableList.copyOf(getAllNonTransientNonStaticFields(type, optionalInstanceToRequireNonNullFieldValue).keySet());
    }

    @SuppressWarnings("unchecked")
    public static List<String> getAllNonTransientNonStaticFieldNamesUntyped(Class<?> type, Object optionalInstanceToRequireNonNullFieldValue) {
        return getAllNonTransientNonStaticFieldNames((Class<Object>)type, optionalInstanceToRequireNonNullFieldValue);
    }

    /**
     * Provides poor man's generics -- we decorate when looking at a field,
     * and strip when looking up in the registry.
     * <p>
     * It's not that bad as fields are the *only* place in java where generic information is available.
     * <p>
     * However we don't do them recursively at all (so eg a List<List<String>> becomes a List<List>).
     * TODO That wouldn't be hard to fix.
     */
    public static String getFieldTypeName(Field ff, YomlConfig config) {
        String baseTypeName = config.getTypeRegistry().getTypeNameOfClass(ff.getType());
        String typeName = baseTypeName;
        Type type = ff.getGenericType();
        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)type;
            if (pt.getActualTypeArguments().length>0) {
                typeName += "<";
                for (int i=0; i<pt.getActualTypeArguments().length; i++) {
                    if (i>0) typeName += ",";
                    Type ft = pt.getActualTypeArguments()[i];
                    Class<?> fc = null;
                    if (fc==null && ft instanceof ParameterizedType) ft = ((ParameterizedType)ft).getRawType();
                    if (fc==null && ft instanceof Class) fc = (Class<?>)ft;
                    String rfc = config.getTypeRegistry().getTypeNameOfClass(fc);
                    if (rfc==null) {
                        // cannot resolve generics
                        return baseTypeName;
                    }
                    typeName += rfc;
                }
                typeName += ">";
            }
        }
        return typeName;
    }

    /** add the given defaults to the target, ignoring any where the key is already present; returns number added */
    public static int addDefaults(Map<String, ? extends Object> defaults, Map<? super String, Object> target) {
        int i=0;
        if (defaults!=null) for (String key: defaults.keySet()) {
            if (!target.containsKey(key)) {
                target.put(key, defaults.get(key));
                i++;
            }
        }
        return i;
    }


    /** removes the given defaults from the target, where the key and value match,
     * ignoring any where the key is already present; returns number removed */
    public static int removeDefaults(Map<String, ? extends Object> defaults, Map<? super String, ? extends Object> target) {
        int i=0;
        if (defaults!=null && target!=null) for (String key: defaults.keySet()) {
            if (target.containsKey(key)) {
                Object v = target.get(key);
                Object dv = defaults.get(key);
                if (Objects.equal(v, dv)) {
                    target.remove(key);
                    i++;
                }
            }
        }
        return i;
    }
    
    public  static Map<String, Object> extractDefaultMap(DefaultKeyValue[] defaultValues) {
        if (defaultValues==null || defaultValues.length==0) return null;
        MutableMap<String,Object> result = MutableMap.of();
        for (DefaultKeyValue d: defaultValues) {
            Object v = d.val();
            if (d.valNeedsParsing()) {
                v = Iterables.getOnlyElement( Yamls.parseAll(d.val()) );
            }
            result.put(d.key(), v);
        }
        return result;
    }    
}
