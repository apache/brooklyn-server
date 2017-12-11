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
package org.apache.brooklyn.enricher.stock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.reflect.TypeToken;

@Beta
public class MathAggregatorFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(MathAggregatorFunctions.class);

    private MathAggregatorFunctions() {}
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingSum(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, Class<T> type) {
        return computingSum(defaultValueForUnreportedSensors, valueToReportIfNoSensors, TypeToken.of(type));
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingSum(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
        return new ComputingSum<T>(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingAverage(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, Class<T> type) {
        return computingAverage(defaultValueForUnreportedSensors, valueToReportIfNoSensors, TypeToken.of(type));
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingAverage(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
        return new ComputingAverage<T>(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingMin(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, Class<T> type) {
        return computingMin(defaultValueForUnreportedSensors, valueToReportIfNoSensors, TypeToken.of(type));
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingMin(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
        return new ComputingMin<T>(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingMax(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, Class<T> type) {
        return computingMax(defaultValueForUnreportedSensors, valueToReportIfNoSensors, TypeToken.of(type));
    }
    
    @Beta
    public static <T extends Number> Function<Collection<? extends Number>, T> computingMax(
            Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
        return new ComputingMax<T>(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
    }

    @Beta
    protected abstract static class AbstractComputingNumber<T extends Number> implements Function<Collection<? extends Number>, T> {
        protected final Number defaultValueForUnreportedSensors;
        protected final Number valueToReportIfNoSensors;
        protected final TypeToken<T> typeToken;
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public AbstractComputingNumber(Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
            this.defaultValueForUnreportedSensors = defaultValueForUnreportedSensors;
            this.valueToReportIfNoSensors = valueToReportIfNoSensors;
            if (typeToken != null && TypeToken.of(Number.class).isAssignableFrom(typeToken.getType())) {
                this.typeToken = typeToken;
            } else if (typeToken == null || typeToken.isAssignableFrom(Number.class)) {
                // use double if e.g. Object is supplied
                this.typeToken = (TypeToken)TypeToken.of(Double.class);
            } else {
                throw new IllegalArgumentException("Type "+typeToken+" is not valid for "+this);
            }
        }
        
        @Override
        public abstract T apply(Collection<? extends Number> vals);
    }

    @Beta
    protected abstract static class BasicComputingNumber<T extends Number> extends AbstractComputingNumber<T> {
        public BasicComputingNumber(Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
            super(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
        }
        
        @Override
        public T apply(@Nullable Collection<? extends Number> vals) {
            List<Number> postProcessedVals = new ArrayList<>();
            int count = 0;
            if (vals != null) {
                for (Object val : vals) {
                    Maybe<Number> coercedVal = TypeCoercions.tryCoerce(val, Number.class);
                    if (coercedVal.isPresentAndNonNull()) {
                        postProcessedVals.add(coercedVal.get());
                        count++;
                    } else if (defaultValueForUnreportedSensors != null) {
                        postProcessedVals.add(defaultValueForUnreportedSensors);
                        count++;
                    }
                }
            }
            if (count==0) return cast(valueToReportIfNoSensors, typeToken);
            
            Number result = applyImpl(postProcessedVals);
            return cast(result, typeToken);
        }
        
        public abstract Number applyImpl(Collection<Number> vals);
    }

    @Beta
    protected static class ComputingSum<T extends Number> extends BasicComputingNumber<T> {
        public ComputingSum(Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
            super(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
        }
        @Override
        public Number applyImpl(Collection<Number> vals) {
            double result = 0d;
            for (Number val : vals) {
                result += val.doubleValue();
            }
            return result;
        }
    }

    @Beta
    protected static class ComputingAverage<T extends Number> extends BasicComputingNumber<T> {
        public ComputingAverage(Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
            super(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
        }
        @Override
        public Number applyImpl(Collection<Number> vals) {
            double sum = 0d;
            for (Number val : vals) {
                sum += val.doubleValue();
            }
            return (sum / vals.size());
        }
    }

    @Beta
    protected static class ComputingMin<T extends Number> extends BasicComputingNumber<T> {
        public ComputingMin(Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
            super(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
        }
        @Override
        public Number applyImpl(Collection<Number> vals) {
            Double result = null;
            for (Number val : vals) { 
                result = (result == null) ? val.doubleValue() : Math.min(result, val.doubleValue());
            }
            return result;
        }
    }

    @Beta
    protected static class ComputingMax<T extends Number> extends BasicComputingNumber<T> {
        public ComputingMax(Number defaultValueForUnreportedSensors, Number valueToReportIfNoSensors, TypeToken<T> typeToken) {
            super(defaultValueForUnreportedSensors, valueToReportIfNoSensors, typeToken);
        }
        @Override
        public Number applyImpl(Collection<Number> vals) {
            Double result = null;
            for (Number val : vals) { 
                result = (result == null) ? val.doubleValue() : Math.max(result, val.doubleValue());
            }
            return result;
        }
    }

    protected static <N extends Number> N cast(Number n, TypeToken<? extends N> numberType) {
        return (N) TypeCoercions.coerce(n, numberType);
    }
}
