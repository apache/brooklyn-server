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
package org.apache.brooklyn.util.javalang.coerce;

import com.google.common.annotations.Beta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.collections.QuorumCheck.QuorumChecks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.apache.brooklyn.util.net.Cidr;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.brooklyn.util.time.Timestamp;
import org.apache.brooklyn.util.yaml.Yamls;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;

public class CommonAdaptorTypeCoercions {

    // It might be nice to support key=value syntax when trying to parse things as maps.
    // We have never supported this, but a bug in testng made it look like we did.
    // This flags relevant areas of code and tests
    public static final boolean PARSE_MAPS_WITH_EQUALS_SYMBOL = false;

    @Beta public static final double DELTA_FOR_COERCION = 0.000001;

    private final TypeCoercerExtensible coercer;

    public CommonAdaptorTypeCoercions(TypeCoercerExtensible coercer) {
        this.coercer = coercer;
    }

    public CommonAdaptorTypeCoercions registerAllAdapters() {
        registerStandardAdapters();
        registerRecursiveIterableAdapters();
        registerClassForNameAdapters();
        registerCollectionJsonAdapters();
        
        return this;
    }
    
    /** Registers an adapter for use with type coercion. Returns any old adapter registered for this pair. */
    public synchronized <A,B> Function<? super A,B> registerAdapter(Class<A> sourceType, Class<B> targetType, Function<? super A,B> fn) {
        return coercer.registerAdapter(sourceType, targetType, fn);
    }
    /** Registers an adapter for use with type coercion. nameAndOrder is of the form NUM-NAME with the natural order prevailing (ordered by NUM numerically),
     * eg 1-x before 2-x before 9-x before 11-x;
     * negative indexed orders are processed after, with -1-x before -2-x before -11-x */
    public synchronized void registerAdapter(String nameAndOrder, TryCoercer coerceFn) {
        coercer.registerAdapter(nameAndOrder, coerceFn);
    }
    
    @SuppressWarnings("rawtypes")
    public void registerStandardAdapters() {
        registerAdapter(CharSequence.class, String.class, new Function<CharSequence,String>() {
            @Override
            public String apply(CharSequence input) {
                return input.toString();
            }
        });
        registerAdapter(byte[].class, String.class, new Function<byte[],String>() {
            @Override
            public String apply(byte[] input) {
                return new String(input);
            }
        });
        registerAdapter(Collection.class, Set.class, new Function<Collection,Set>() {
            @SuppressWarnings("unchecked")
            @Override
            public Set apply(Collection input) {
                return Sets.newLinkedHashSet(input);
            }
        });
        registerAdapter(Collection.class, List.class, new Function<Collection,List>() {
            @SuppressWarnings("unchecked")
            @Override
            public List apply(Collection input) {
                return Lists.newArrayList(input);
            }
        });
        registerAdapter(String.class, InetAddress.class, new Function<String,InetAddress>() {
            @Override
            public InetAddress apply(String input) {
                return Networking.getInetAddressWithFixedName(input);
            }
        });
        registerAdapter(String.class, HostAndPort.class, new Function<String,HostAndPort>() {
            @Override
            public HostAndPort apply(String input) {
                return HostAndPort.fromString(input);
            }
        });
        registerAdapter(String.class, UserAndHostAndPort.class, new Function<String,UserAndHostAndPort>() {
            @Override
            public UserAndHostAndPort apply(String input) {
                return UserAndHostAndPort.fromString(input);
            }
        });
        registerAdapter(String.class, Cidr.class, new Function<String,Cidr>() {
            @Override
            public Cidr apply(String input) {
                return new Cidr(input);
            }
        });
        registerAdapter(String.class, URL.class, new Function<String,URL>() {
            @Override
            public URL apply(String input) {
                try {
                    return new URL(input);
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }
        });
        registerAdapter(URL.class, String.class, new Function<URL,String>() {
            @Override
            public String apply(URL input) {
                return input.toString();
            }
        });
        registerAdapter(String.class, URI.class, new Function<String,URI>() {
            @Override
            public URI apply(String input) {
                return Strings.isNonBlank(input) ? URI.create(input) : null;
            }
        });
        registerAdapter(URI.class, String.class, new Function<URI,String>() {
            @Override
            public String apply(URI input) {
                return input.toString();
            }
        });
        registerAdapter(Object.class, Duration.class, new Function<Object,Duration>() {
            @Override
            public Duration apply(final Object input) {
                return org.apache.brooklyn.util.time.Duration.of(input);
            }
        });

        registerAdapter(Integer.class, AtomicLong.class, new Function<Integer,AtomicLong>() {
            @Override public AtomicLong apply(final Integer input) {
                return new AtomicLong(input);
            }
        });
        registerAdapter(Long.class, AtomicLong.class, new Function<Long,AtomicLong>() {
            @Override public AtomicLong apply(final Long input) {
                return new AtomicLong(input);
            }
        });
        registerAdapter(String.class, AtomicLong.class, new Function<String,AtomicLong>() {
            @Override public AtomicLong apply(final String input) {
                return new AtomicLong(Long.parseLong(input.trim()));
            }
        });
        registerAdapter(Integer.class, AtomicInteger.class, new Function<Integer,AtomicInteger>() {
            @Override public AtomicInteger apply(final Integer input) {
                return new AtomicInteger(input);
            }
        });
        registerAdapter(String.class, AtomicInteger.class, new Function<String,AtomicInteger>() {
            @Override public AtomicInteger apply(final String input) {
                return new AtomicInteger(Integer.parseInt(input.trim()));
            }
        });
        /** This always returns a {@link Double}, cast as a {@link Number}; 
         * however primitives and boxers get exact typing due to call in #stringToPrimitive */
        registerAdapter(String.class, Number.class, new Function<String,Number>() {
            @Override
            public Number apply(String input) {
                return Double.valueOf(input);
            }
        });
        registerAdapter(BigDecimal.class, Double.class, new Function<BigDecimal,Double>() {
            @Override
            public Double apply(BigDecimal input) {
                return checkValidForConversion(input, input.doubleValue());
            }
        });
        registerAdapter(BigInteger.class, Long.class, new Function<BigInteger,Long>() {
            @Override
            public Long apply(BigInteger input) {
                return input.longValueExact();
            }
        });
        registerAdapter(BigInteger.class, Integer.class, new Function<BigInteger,Integer>() {
            @Override
            public Integer apply(BigInteger input) {
                return input.intValueExact();
            }
        });
        registerAdapter(String.class, BigDecimal.class, new Function<String,BigDecimal>() {
            @Override
            public BigDecimal apply(String input) {
                return new BigDecimal(input);
            }
        });
        registerAdapter(Double.class, BigDecimal.class, new Function<Double,BigDecimal>() {
            @Override
            public BigDecimal apply(Double input) {
                return BigDecimal.valueOf(input);
            }
        });
        registerAdapter(String.class, BigInteger.class, new Function<String,BigInteger>() {
            @Override
            public BigInteger apply(String input) {
                return new BigInteger(input);
            }
        });
        registerAdapter(Long.class, BigInteger.class, new Function<Long,BigInteger>() {
            @Override
            public BigInteger apply(Long input) {
                return BigInteger.valueOf(input);
            }
        });
        registerAdapter(Integer.class, BigInteger.class, new Function<Integer,BigInteger>() {
            @Override
            public BigInteger apply(Integer input) {
                return BigInteger.valueOf(input);
            }
        });
        registerAdapter(String.class, Date.class, new Function<String,Date>() {
            @Override
            public Date apply(final String input) {
                return Time.parseDate(input);
            }
        });
        registerAdapter(String.class, Instant.class, new Function<String,Instant>() {
            @Override
            public Instant apply(final String input) {
                return Time.parseDate(input).toInstant();
            }
        });
        registerAdapter(String.class, Timestamp.class, new Function<String,Timestamp>() {
            @Override
            public Timestamp apply(final String input) {
                return new Timestamp(input);
            }
        });
        registerAdapter(Date.class, Timestamp.class, new Function<Date,Timestamp>() {
            @Override
            public Timestamp apply(final Date input) {
                return new Timestamp(input);
            }
        });
        registerAdapter(Instant.class, Timestamp.class, new Function<Instant,Timestamp>() {
            @Override
            public Timestamp apply(final Instant input) {
                return new Timestamp(input);
            }
        });
        registerAdapter(String.class, QuorumCheck.class, new Function<String,QuorumCheck>() {
            @Override
            public QuorumCheck apply(final String input) {
                return QuorumChecks.of(input);
            }
        });
        registerAdapter(Integer.class, QuorumCheck.class, new Function<Integer,QuorumCheck>() {
            @Override
            public QuorumCheck apply(final Integer input) {
                return QuorumChecks.of(input);
            }
        });
        registerAdapter(Collection.class, QuorumCheck.class, new Function<Collection,QuorumCheck>() {
            @Override
            public QuorumCheck apply(final Collection input) {
                return QuorumChecks.of(input);
            }
        });
        registerAdapter(String.class, TimeZone.class, new Function<String,TimeZone>() {
            @Override
            public TimeZone apply(final String input) {
                return TimeZone.getTimeZone(input);
            }
        });
        registerAdapter(Long.class, Date.class, new Function<Long,Date>() {
            @Override
            public Date apply(final Long input) {
                return new Date(input);
            }
        });
        registerAdapter(Integer.class, Date.class, new Function<Integer,Date>() {
            @Override
            public Date apply(final Integer input) {
                return new Date(input);
            }
        });
        registerAdapter(String.class, Predicate.class, new Function<String,Predicate>() {
            @Override
            public Predicate apply(final String input) {
                switch (input) {
                case "alwaysFalse" : return Predicates.alwaysFalse();
                case "alwaysTrue" :  return Predicates.alwaysTrue();
                case "isNull" :      return Predicates.isNull();
                case "notNull" :     return Predicates.notNull();
                default: throw new IllegalArgumentException("Cannot convert string '" + input + "' to predicate");
                }
            }
        });
        registerAdapter(String.class, Path.class, new Function<String,Path>() {
            @Override
            public Path apply(final String input) {
                return Paths.get(input);
            }
        });
    }

    @Beta
    public static double checkValidForConversion(BigDecimal input, double candidate) {
        if (input.subtract(BigDecimal.valueOf(candidate)).abs().compareTo(BigDecimal.valueOf(DELTA_FOR_COERCION))>0) {
            throw new IllegalStateException("Decimal value out of range; cannot convert "+ input +" to double");
        }
        return candidate;
    }

    @SuppressWarnings("rawtypes")
    public void registerRecursiveIterableAdapters() {
        
        // these refer to the coercer to recursively coerce;
        // they throw if there are errors (but the registry apply loop will catch and handle),
        // as currently the registry does not support Maybe or opting-out
        
        registerAdapter(Iterable.class, String[].class, new Function<Iterable, String[]>() {
            @Nullable
            @Override
            public String[] apply(@Nullable Iterable list) {
                if (list == null) return null;
                String[] result = new String[Iterables.size(list)];
                int count = 0;
                for (Object element : list) {
                    result[count++] = coercer.coerce(element, String.class);
                }
                return result;
            }
        });
        registerAdapter(Iterable.class, Integer[].class, new Function<Iterable, Integer[]>() {
            @Nullable
            @Override
            public Integer[] apply(@Nullable Iterable list) {
                if (list == null) return null;
                Integer[] result = new Integer[Iterables.size(list)];
                int count = 0;
                for (Object element : list) {
                    result[count++] = coercer.coerce(element, Integer.class);
                }
                return result;
            }
        });
        registerAdapter(Iterable.class, int[].class, new Function<Iterable, int[]>() {
            @Nullable
            @Override
            public int[] apply(@Nullable Iterable list) {
                if (list == null) return null;
                int[] result = new int[Iterables.size(list)];
                int count = 0;
                for (Object element : list) {
                    result[count++] = coercer.coerce(element, int.class);
                }
                return result;
            }
        });
    }
    
    @SuppressWarnings("rawtypes")
    public void registerClassForNameAdapters() {
        registerAdapter(String.class, Class.class, new Function<String,Class>() {
            @Override
            public Class apply(final String input) {
                try {
                    return Class.forName(input);
                } catch (ClassNotFoundException e) {
                    throw Exceptions.propagate(e);
                }
            }
        });        
    }
    
    public void registerCollectionJsonAdapters() {
        registerAdapter("20-strings-to-collections", new CoerceStringToCollections());
    }

    /** Does a rough coercion of the string to the indicated Collection or Map type.
     * Only looks at generics enough to choose the right parser.
     * Expects the caller {@link TypeCoercerExtensible} to recurse inside the collection/map.
     */
    public static class CoerceStringToCollections implements TryCoercer {
        @SuppressWarnings("unchecked")
        @Override
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
            if (!(input instanceof String)) return null;
            String inputS = (String)input;
            
            Class<? super T> rawType = TypeTokens.getRawRawType(type);

            if (Collection.class.isAssignableFrom(rawType)) {
                TypeToken<?> parameters[] = TypeTokens.getGenericParameterTypeTokensWhenUpcastToClassRaw(type, Collection.class);
                Maybe<?> resultM = null;
                Collection<?> result = null;
                if (parameters.length==1 && TypeTokens.isAssignableFromRaw(CharSequence.class, parameters[0])) {
                    // for list of strings, use special parse
                    result = JavaStringEscapes.unwrapJsonishListStringIfPossible(inputS);
                } else {
                    // any other type, use YAMLish parse
                    resultM = JavaStringEscapes.tryUnwrapJsonishList(inputS);
                    result = (Collection<?>) resultM.orNull();
                }
                if (result==null) {
                    if (resultM!=null) return Maybe.Absent.castAbsent(resultM);
                    return null;
                }
                if (rawType.isAssignableFrom(MutableList.class)) {
                    return Maybe.of((T) MutableList.copyOf(result).asUnmodifiable());
                }
                if (rawType.isAssignableFrom(MutableSet.class)) {
                    return Maybe.of((T) MutableSet.copyOf(result).asUnmodifiable());
                }
                if (rawType.isInstance(result)) {
                    return Maybe.of((T) result);
                }
                // the type is not a collection we can deal with
                return null;
            }
            
            if (Map.class.isAssignableFrom(rawType)) {
                
                Function<String,Maybe<Map<?,?>>> parseYaml = (in) -> {
                    try {
                        return Maybe.of(Yamls.getAs( Yamls.parseAll(in), Map.class ));
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        return Maybe.absent(new IllegalArgumentException("Cannot parse string as map with flexible YAML parsing; "+
                            (e instanceof ClassCastException ? "yaml treats it as a string" : 
                            (e instanceof IllegalArgumentException && Strings.isNonEmpty(e.getMessage())) ? e.getMessage() :
                            ""+e) ));
                    }
                };
                
                Maybe<Map<?, ?>> r1 = null;

                if (PARSE_MAPS_WITH_EQUALS_SYMBOL) {
                    // implement it here if we support it. could perhaps simply replace with ": " ?
                    // but ideally want more sophisticated quote processing and splitting
                    throw new IllegalStateException("Parsing maps with equals not currently supported");
                }

                // first try wrapping in braces if needed
                if (!inputS.trim().startsWith("{") && (inputS.contains(": "))) {
                    r1 = parseYaml.apply("{ "+inputS+" }");
                    if (r1.isPresent()) return (Maybe<T>) r1;
                    // fall back to parsing without braces, e.g. if it's multiline
                }
    
                Maybe<Map<?, ?>> r2 = parseYaml.apply(inputS);
                if (r2.isPresent()) return (Maybe<T>) r2;
                
                // absent - prefer the first error if it wasn't multiline
                return (Maybe<T>) ((r1!=null && inputS.indexOf('\n')==-1) ? r1 : r2);

                // NB: previously we supported this also, when we did json above;
                // yaml support is better as it supports quotes (and better than json because it allows dropping quotes)
                // snake-yaml, our parser, also accepts key=value -- although i'm not sure this is strictly yaml compliant;
                // our tests will catch it if snake behaviour changes, and we can reinstate this
                // (but note it doesn't do quotes; see http://code.google.com/p/guava-libraries/issues/detail?id=412 for that):
    //            return ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(input));
    
            }
            
            // other types not supported here
            return null;
        }
    }
}
