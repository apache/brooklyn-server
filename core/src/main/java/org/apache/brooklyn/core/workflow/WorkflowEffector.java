package org.apache.brooklyn.core.workflow;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.core.effector.AddEffectorInitializerAbstract;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;

import java.util.Map;

public class WorkflowEffector extends AddEffectorInitializerAbstract implements WorkflowCommonConfig {

    public WorkflowEffector() {}
    public WorkflowEffector(ConfigBag params) { super(params); }
    public WorkflowEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    @Override
    protected Effectors.EffectorBuilder<Object> newEffectorBuilder() {
        Effectors.EffectorBuilder<Object> eff = newAbstractEffectorBuilder(Object.class);
        eff.impl(new WorkflowEffector.Body(eff.buildAbstract(), initParams()));
        return eff;
    }

    protected static class Body extends EffectorBody<Object> {
        private final Effector<?> effector;
        private final ConfigBag params;

        public Body(Effector<?> eff, ConfigBag params) {
            this.effector = eff;
            this.params = params;

            WorkflowStepResolution.validateWorkflowParameters(entity(), params);
        }

        @Override
        public Object call(final ConfigBag params) {
            return DynamicTasks.queue( new WorkflowExecutionContext("Workflow for effector "+effector.getName(), entity(), this.params, params).asTask() ).getUnchecked();
        }
    }

}
