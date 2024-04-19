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
package org.apache.brooklyn.util.math;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

public class NumberMath<T extends Number> {

    static final BigDecimal DEFAULT_TOLERANCE = new BigDecimal(0.00000001);

    final T number;
    final Class<T> desiredType;
    final Function<Number, T> handlerForUncastableType;
    final BigDecimal tolerance;

    public NumberMath(T number) {
        this(number, (Class<T>)number.getClass(), null);
    }
    /** callers can pass `Number` as second arg if they will accept any type; otherwise the input type is expected as the default */
    public NumberMath(T number, Class<T> desiredType) {
        this(number, desiredType, null);
    }
    public NumberMath(T number, Class<T> desiredType, Function<Number,T> handlerForUncastableType) {
        this(number, desiredType, handlerForUncastableType, null);
    }
    public NumberMath(T number, Class<T> desiredType, Function<Number,T> handlerForUncastableType, BigDecimal tolerance) {
        this.number = number;
        this.desiredType = desiredType;
        this.handlerForUncastableType = handlerForUncastableType==null ? (x -> { throw new IllegalArgumentException("Cannot cast "+x+" to "+number.getClass()); }) : handlerForUncastableType;
        this.tolerance = tolerance==null ? DEFAULT_TOLERANCE : null;
    }

    public BigDecimal asBigDecimal() { return asBigDecimal(number); }
    public Optional<BigInteger> asBigIntegerWithinTolerance() { return asBigIntegerWithinTolerance(number); }

    public <T extends Number> T asTypeForced(Class<T> desiredType) {
        return asTypeFirstMatching(number, desiredType, y -> withinTolerance(y));
    }
    public <T extends Number> Optional<T> asTypeWithinTolerance(Class<T> desiredType, Number tolerance) {
        return Optional.ofNullable(asTypeFirstMatching(number, desiredType, y -> withinTolerance(number, y, tolerance)));
    }

    public static boolean isPrimitiveWholeNumberType(Number number) {
        return number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte;
    }

    public static boolean isPrimitiveFloatingPointType(Number number) {
        return number instanceof Double || number instanceof Float;
    }

    public static boolean isPrimitiveNumberType(Number number) {
        return isPrimitiveFloatingPointType(number) || isPrimitiveWholeNumberType(number);
    }

    public static BigDecimal asBigDecimal(Number number) {
        if (number instanceof BigDecimal) return (BigDecimal) number;
        if (isPrimitiveFloatingPointType(number)) return new BigDecimal(number.doubleValue());
        if (isPrimitiveWholeNumberType(number)) return new BigDecimal(number.longValue());
        if (number instanceof BigInteger) return new BigDecimal((BigInteger) number);
        return new BigDecimal(""+number);
    }

    public Optional<BigInteger> asBigIntegerWithinTolerance(Number number) {
        BigInteger candidate = asBigIntegerForced(number);
        if (withinTolerance(candidate)) return Optional.of(candidate);
        return Optional.empty();
    }

    public static BigInteger asBigIntegerForced(Number number) {
        if (number instanceof BigInteger) return (BigInteger) number;
        if (isPrimitiveWholeNumberType(number)) return BigInteger.valueOf(number.longValue());
        return asBigDecimal(number).toBigInteger();
    }

    public static <T extends Number> T asTypeForced(Number x, Class<T> desiredType) {
        return asTypeFirstMatching(x, desiredType, y -> withinTolerance(x, y, DEFAULT_TOLERANCE));
    }
    public static <T extends Number> Optional<T> asTypeWithinTolerance(Number x, Class<T> desiredType, Number tolerance) {
        return Optional.ofNullable(asTypeFirstMatching(x, desiredType, y -> withinTolerance(x, y, tolerance)));
    }
    protected static <T extends Number> T asTypeFirstMatching(Number x, Class<T> desiredType, Predicate<T> check) {
        if (desiredType.isAssignableFrom(Integer.class)) { T candidate = (T) (Object) x.intValue(); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(Long.class)) { T candidate = (T) (Object) x.longValue(); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(Double.class)) { T candidate = (T) (Object) x.doubleValue(); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(Float.class)) { T candidate = (T) (Object) x.floatValue(); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(Short.class)) { T candidate = (T) (Object) x.shortValue(); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(Byte.class)) { T candidate = (T) (Object) x.byteValue(); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(BigInteger.class)) { T candidate = (T) asBigIntegerForced(x); if (check!=null && check.test(candidate)) return candidate; }
        if (desiredType.isAssignableFrom(BigDecimal.class)) { T candidate = (T) asBigDecimal(x); if (check!=null && check.test(candidate)) return candidate; }
        return null;
    }

    public boolean withinTolerance(Number b) {
        return withinTolerance(number, b, tolerance);
    }
    public static boolean withinTolerance(Number a, Number b, Number tolerance) {
        return asBigDecimal(a).subtract(asBigDecimal(b)).abs().compareTo(asBigDecimal(tolerance)) <= 0;
    }

    // from https://www.w3schools.com/java/java_type_casting.asp
//    In Java, there are two types of casting:
//
//    Widening Casting (automatically) - converting a smaller type to a larger type size
//    byte -> short -> char -> int -> long -> float -> double
//
//    Narrowing Casting (manually) - converting a larger type to a smaller size type
//    double -> float -> long -> int -> char -> short -> byte

    protected T attemptCast(Number candidate) {
        Optional<T> result = asTypeWithinTolerance(candidate, desiredType, tolerance);
        if (result.isPresent()) return result.get();
        return handlerForUncastableType.apply(candidate);
    }

    protected T attemptUnary(Function<Long,Long> intFn, Function<Double,Double> doubleFn, Function<BigInteger,BigInteger> bigIntegerFn, Function<BigDecimal,BigDecimal> bigDecimalFn) {
        if (isPrimitiveWholeNumberType(number)) return attemptCast(intFn.apply(number.longValue()));
        if (isPrimitiveNumberType(number)) return attemptCast(doubleFn.apply(number.doubleValue()));
        if (number instanceof BigInteger) return attemptCast(bigIntegerFn.apply((BigInteger)number));
        if (number instanceof BigDecimal) return attemptCast(bigDecimalFn.apply((BigDecimal)number));
        return attemptCast(bigDecimalFn.apply(asBigDecimal()));
    }

    protected T attemptBinary(T rhs, @Nullable BiFunction<Long,Long,Long> intFn, BiFunction<Double,Double,Double> doubleFn, @Nullable BiFunction<BigInteger,BigInteger,BigInteger> bigIntegerFn, BiFunction<BigDecimal,BigDecimal,BigDecimal> bigDecimalFn) {
        if (isPrimitiveWholeNumberType(number) && isPrimitiveWholeNumberType(rhs) && intFn!=null) return attemptCast(intFn.apply(number.longValue(), rhs.longValue()));
        if (isPrimitiveNumberType(number) && isPrimitiveNumberType(rhs)) return attemptCast(doubleFn.apply(number.doubleValue(), rhs.doubleValue()));
        if (number instanceof BigInteger && bigIntegerFn!=null) {
            BigInteger rhsI = asBigIntegerWithinTolerance(rhs).orElse(null);
            if (rhsI!=null) return attemptCast(bigIntegerFn.apply((BigInteger) number, rhsI));
        }
        if (number instanceof BigDecimal) {
            return attemptCast(bigDecimalFn.apply((BigDecimal) number, asBigDecimal(rhs)));
        }
        return attemptCast(bigDecimalFn.apply(asBigDecimal(), asBigDecimal(rhs)));
    }
    protected T attemptBinaryWithDecimalPrecision(T rhs, BiFunction<Double,Double,Double> doubleFn, BiFunction<BigDecimal,BigDecimal,BigDecimal> bigDecimalFn) {
        return attemptBinary(rhs, null, doubleFn, null, bigDecimalFn);
    }

    public static <T extends Number> T pairwise(BiFunction<NumberMath<T>,T,T> fn, T ...rhsAll) {
        T result = rhsAll[0];
        for (T rhs: rhsAll) result = fn.apply(new NumberMath<T>(result),rhs);
        return result;
    }

    public T abs() { return attemptUnary(x -> x<0 ? -x : x, x -> x<0 ? -x : x, BigInteger::abs, BigDecimal::abs); }
    public T round() { return attemptUnary(x -> x, d -> (double) Math.round(d), x -> x, x -> x.setScale(0, BigDecimal.ROUND_DOWN)); }
    public T ceil() { return attemptUnary(x -> x, d -> Math.ceil(d), x -> x, x -> x.setScale(0, BigDecimal.ROUND_CEILING)); }
    public T floor() { return attemptUnary(x -> x, d -> Math.floor(d), x -> x, x -> x.setScale(0, BigDecimal.ROUND_FLOOR)); }
    public T frac() { return attemptUnary(x -> 0L, d -> d - Math.floor(d), x -> BigInteger.ZERO, x -> x.subtract(x.setScale(0, BigDecimal.ROUND_FLOOR))); }
    public T negate() { return attemptUnary(x -> -x, x -> -x, BigInteger::negate, BigDecimal::negate); }

    public T add(T rhs) { return attemptBinary(rhs, (x,y) -> x+y, (x,y) -> x+y, BigInteger::add, BigDecimal::add); }
    public T subtract(T rhs) { return attemptBinary(rhs, (x,y) -> x-y, (x,y) -> x-y, BigInteger::subtract, BigDecimal::subtract); }
    public T multiply(T rhs) { return attemptBinary(rhs, (x,y) -> x*y, (x,y) -> x*y, BigInteger::multiply, BigDecimal::multiply); }
    public T divide(T rhs) { return attemptBinaryWithDecimalPrecision(rhs, (x,y) -> x/y, BigDecimal::divide); }

    public T max(T rhs) { return attemptBinary(rhs, (x,y) -> x>y ? x : y, (x,y) -> x>y ? x : y, BigInteger::max, BigDecimal::max); }
    public T min(T rhs) { return attemptBinary(rhs, (x,y) -> x<y ? x : y, (x,y) -> x<y ? x : y, BigInteger::min, BigDecimal::min); }

}
