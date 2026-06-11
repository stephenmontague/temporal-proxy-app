package com.dummycloud;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The cloud app is a Temporal client only — it starts workflows and signals; no worker. */
@Configuration
public class TemporalClientConfig {

    @Bean(destroyMethod = "shutdown")
    public WorkflowServiceStubs serviceStubs(CloudProperties properties) {
        return WorkflowServiceStubs.newServiceStubs(WorkflowServiceStubsOptions.newBuilder()
                .setTarget(properties.temporal().target())
                .build());
    }

    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs, CloudProperties properties) {
        return WorkflowClient.newInstance(stubs, WorkflowClientOptions.newBuilder()
                .setNamespace(properties.temporal().namespace())
                .build());
    }
}
