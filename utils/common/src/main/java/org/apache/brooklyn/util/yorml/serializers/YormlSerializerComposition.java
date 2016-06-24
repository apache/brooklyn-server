package org.apache.brooklyn.util.yorml.serializers;

import java.util.Map;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.yorml.YormlConfig;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;
import org.apache.brooklyn.util.yorml.YormlReadContext;
import org.apache.brooklyn.util.yorml.YormlSerializer;

public class YormlSerializerComposition implements YormlSerializer {

    protected final Class<? extends YormlSerializerWorker> workerType;

    public YormlSerializerComposition(Class<? extends YormlSerializerWorker> workerType) {
        this.workerType = workerType;
    }
    
    public abstract static class YormlSerializerWorker {

        protected YormlContext context;
        protected YormlReadContext readContext;
        protected YormlConfig config;
        protected Map<Object, Object> blackboard;

        private void initRead(YormlReadContext context, YormlConfig config, Map<Object, Object> blackboard) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.readContext = context;
            this.config = config;
            this.blackboard = blackboard;
        }
        
        private void initWrite(YormlContext context, YormlConfig config, Map<Object,Object> blackboard) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.config = config;
            this.blackboard = blackboard;
        }
        
        public boolean hasJavaObject() { return context.getJavaObject()!=null; }
        public boolean hasYamlObject() { return context.getYamlObject()!=null; }
        public Object getJavaObject() { return context.getJavaObject(); }
        public Object getYamlObject() { return context.getYamlObject(); }

        public boolean isYamlMap() { return context.getYamlObject() instanceof Map; }
        @SuppressWarnings("unchecked")
        public Map<Object,Object> getYamlMap() { return (Map<Object,Object>)context.getYamlObject(); }
        /** Returns the value of the given key if present in the map and of the given type. 
         * If the YAML is not a map, or the key is not present, or the type is different, this returns null. */
        @SuppressWarnings("unchecked")
        public <T> T getFromYamlMap(String key, Class<T> type) {
            if (!isYamlMap()) return null;
            Object v = getYamlMap().get(key);
            if (v==null) return null;
            if (!type.isInstance(v)) return null;
            return (T) v;
        }
        protected void setInYamlMap(String key, Object value) {
            ((Map<Object,Object>)getYamlMap()).put(key, value);
        }
        
        public abstract YormlContinuation read();
        public abstract YormlContinuation write();
    }
    
    @Override
    public YormlContinuation read(YormlReadContext context, YormlConfig config, Map<Object,Object> blackboard) {
        YormlSerializerWorker worker;
        try {
            worker = workerType.newInstance();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initRead(context, config, blackboard);
        return worker.read();
    }

    @Override
    public YormlContinuation write(YormlContext context, YormlConfig config, Map<Object,Object> blackboard) {
        YormlSerializerWorker worker;
        try {
            worker = workerType.newInstance();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initWrite(context, config, blackboard);
        return worker.write();
    }

    @Override
    public String document(String type, YormlConfig config) {
        return null;
    }
}
