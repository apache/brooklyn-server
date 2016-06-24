package org.apache.brooklyn.util.yorml.serializers;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.FieldOrderings;
import org.apache.brooklyn.util.javalang.ReflectionPredicates;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;
import org.apache.brooklyn.util.yorml.serializers.FieldsInFieldsMap.FieldsInBlackboard;

public class InstantiateType extends YormlSerializerComposition {

    public InstantiateType() { super(Worker.class); }
    
    public static class Worker extends YormlSerializerWorker {
        public YormlContinuation read() {
            if (hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            // TODO check is primitive?
            // TODO if map and list?
            
            String type = getFromYamlMap("type", String.class);
            if (type==null) return YormlContinuation.CONTINUE_UNCHANGED;
            
            Object result = config.getTypeRegistry().newInstance((String)type);
            context.setJavaObject(result);
            
            return YormlContinuation.RESTART;
        }

        public YormlContinuation write() {
            if (hasYamlObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            if (!hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            if (FieldsInBlackboard.isPresent(blackboard)) return YormlContinuation.CONTINUE_UNCHANGED;
            
            FieldsInBlackboard fib = FieldsInBlackboard.create(blackboard);
            fib.fieldsToWriteFromJava = MutableList.of();
            
            Object jo = getJavaObject();
            if (Boxing.isPrimitiveOrBoxedObject(jo)) {
                context.setJavaObject(jo);
                return YormlContinuation.RESTART;
            }

            // TODO map+list -- here, or in separate serializers?
            
            MutableMap<Object, Object> map = MutableMap.of();
            context.setYamlObject(map);
            
            // TODO look up registry type
            // TODO support osgi
            
            map.put("type", "java:"+getJavaObject().getClass().getName());
            
            List<Field> fields = Reflections.findFields(getJavaObject().getClass(), 
                null,
                FieldOrderings.ALPHABETICAL_FIELD_THEN_SUB_BEST_FIRST);
            Field lastF = null;
            for (Field f: fields) {
                Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), f);
                if (ReflectionPredicates.IS_FIELD_NON_TRANSIENT.apply(f) && v.isPresentAndNonNull()) {
                    String name = f.getName();
                    if (lastF!=null && lastF.getName().equals(f.getName())) {
                        // if field is shadowed use FQN
                        name = f.getDeclaringClass().getCanonicalName()+"."+name;
                    }
                    fib.fieldsToWriteFromJava.add(name);
                }
                lastF = f;
            }
            
            return YormlContinuation.RESTART;
        }
    }
}
