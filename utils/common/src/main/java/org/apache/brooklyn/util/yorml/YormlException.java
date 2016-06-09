package org.apache.brooklyn.util.yorml;

public class YormlException extends RuntimeException {

    private static final long serialVersionUID = 7825908737102292499L;
    
    YormlContext context;
    
    public YormlException(String message) { super(message); }
    public YormlException(String message, Throwable cause) { super(message, cause); }
    public YormlException(String message, YormlContext context) { this(message); this.context = context; }
    public YormlException(String message, YormlContext context, Throwable cause) { this(message, cause); this.context = context; }

    public YormlContext getContext() {
        return context;
    }
    
    @Override
    public String toString() {
        if (context==null) return super.toString();
        return super.toString() + " ("+context+")";
    }
    
}
