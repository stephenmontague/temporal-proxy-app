package com.proxyapp.temporal.workflow;

import com.proxyapp.model.CanonicalMessage;
import com.proxyapp.temporal.activity.DeliverToEdgeActivity;
import io.temporal.activity.ActivityOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;

@WorkflowImpl(taskQueues = "${proxy.task-queue}")
public class DeliverToEdgeWorkflowImpl implements DeliverToEdgeWorkflow {

    private static final Logger log = Workflow.getLogger(DeliverToEdgeWorkflowImpl.class);

    private final DeliverToEdgeActivity activity = Workflow.newActivityStub(
            DeliverToEdgeActivity.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .build());

    @Override
    public void deliver(CanonicalMessage message) {
        log.info("[PROXY] Workflow picked up by proxy worker — delivering {} to edge device",
                message.messageType() + "-" + message.businessId());
        Workflow.upsertMemo(Map.of("proxyPickup",
                "[PROXY] Picked up by proxy worker — delivering to edge device"));
        activity.deliver(message);
    }
}
