package org.apache.brooklyn.util.yorml.serializers;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;

public class FieldsInFieldsMap extends YormlSerializerComposition {

    public FieldsInFieldsMap() { super(Worker.class); }
    
    public static class Worker extends YormlSerializerWorker {
        public YormlContinuation read() {
            if (!hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = getFromYamlMap("fields", Map.class);
            if (fields==null) return YormlContinuation.CONTINUE_UNCHANGED;
            
            for (Object f: ((Map<?,?>)fields).keySet()) {
                Object v = ((Map<?,?>)fields).get(f);
                try {
                    Field ff = Reflections.findField(getJavaObject().getClass(), Strings.toString(f));
                    if (ff==null) {
                        // just skip (could throw, but leave it in case something else recognises)
                    } else if (Modifier.isStatic(ff.getModifiers())) {
                        // as above
                    } else {
                        ff.setAccessible(true);
                        ff.set(getJavaObject(), v);
                        ((Map<?,?>)fields).remove(Strings.toString(f));
                        if (((Map<?,?>)fields).isEmpty()) {
                            ((Map<?,?>)context.getYamlObject()).remove("fields");
                        }
                    }
                } catch (Exception e) { throw Exceptions.propagate(e); }
            }
            return YormlContinuation.CONTINUE_CHANGED;
        }

        public YormlContinuation write() {
            if (!isYamlMap()) return YormlContinuation.CONTINUE_UNCHANGED;
            if (getFromYamlMap("fields", Map.class)!=null) return YormlContinuation.CONTINUE_UNCHANGED;
            FieldsInBlackboard fib = FieldsInBlackboard.peek(blackboard);
            if (fib==null || fib.fieldsToWriteFromJava.isEmpty()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            Map<String,Object> fields = MutableMap.of();
            
            for (String f: MutableList.copyOf(fib.fieldsToWriteFromJava)) {
                Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), f);
                if (v.isPresent()) {
                    fib.fieldsToWriteFromJava.remove(f);
                    if (v.get()!=null) {
                        // TODO assert null checks
                        fields.put(f, v.get());
                    }
                }
            }
            
            if (fields.isEmpty()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            setInYamlMap("fields", fields);
            return YormlContinuation.RESTART;
        }
    }

    /** Indicates that something has handled the type 
     * (on read, creating the java object, and on write, setting the `type` field in the yaml object)
     * and made a determination of what fields need to be handled */
    public static class FieldsInBlackboard {
        private static String KEY = "FIELDS_IN_BLACKBOARD";
        
        public static boolean isPresent(Map<Object,Object> blackboard) {
            return blackboard.containsKey(KEY);
        }
        public static FieldsInBlackboard peek(Map<Object,Object> blackboard) {
            return (FieldsInBlackboard) blackboard.get(KEY);
        }
        public static FieldsInBlackboard getOrCreate(Map<Object,Object> blackboard) {
            if (!isPresent(blackboard)) { blackboard.put(KEY, new FieldsInBlackboard()); }
            return peek(blackboard);
        }
        public static FieldsInBlackboard create(Map<Object,Object> blackboard) {
            if (isPresent(blackboard)) { throw new IllegalStateException("Already present"); }
            blackboard.put(KEY, new FieldsInBlackboard());
            return peek(blackboard);
        }
        
        List<String> fieldsToWriteFromJava;
        // TODO if fields are *required* ?
    }
    
}
