package com.proxyapp.temporal.workflow;

import com.proxyapp.model.CanonicalMessage;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface DeliverToEdgeWorkflow {

    @WorkflowMethod(name = "DeliverToEdge")
    void deliver(CanonicalMessage message);
}
