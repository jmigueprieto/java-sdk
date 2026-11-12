/*
 * Copyright 2026 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.sdk.workflow.durable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.conductor.client.http.ConductorClient;
import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.sdk.workflow.durable.WorkflowExecutorWorker.Registration;
import com.netflix.conductor.sdk.workflow.executor.task.AnnotatedWorkerExecutor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Entry point for workflows written as code.
 *
 * <pre>
 * conductor.workers(new UserTasksImpl());
 * conductor.workflow("checkout", (ctx, order) -&gt; { ... });
 * conductor.start("checkout", input);
 * </pre>
 */
public class DurableExecution implements AutoCloseable {

    public static final String EXECUTOR_TASK = "workflow-executor";
    public static final String CLASSIFIER = "durable";

    private final WorkflowClient workflowClient;
    private final AnnotatedWorkerExecutor workerExecutor;
    private final ObjectMapper om;
    private final Map<String, Registration> registry = new ConcurrentHashMap<>();
    private boolean executorRegistered;

    public DurableExecution(ConductorClient client) {
        this(new WorkflowClient(client), new AnnotatedWorkerExecutor(new TaskClient(client)));
    }

    DurableExecution(WorkflowClient workflowClient, AnnotatedWorkerExecutor workerExecutor) {
        this.workflowClient = workflowClient;
        this.workerExecutor = workerExecutor;
        this.om = new ObjectMapperProvider().getObjectMapper();
        this.om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /** Registers task implementations and starts to poll for their tasks. */
    public synchronized void workers(Object... implementations) {
        for (Object implementation : implementations) {
            workerExecutor.addBean(implementation);
        }
        workerExecutor.startPolling();
    }

    /** Registers a workflow written as code whose input is a map. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <O> void workflow(String name, WorkflowFunction<Map<String, Object>, O> fn) {
        workflow(name, (Class) Map.class, (WorkflowFunction) fn);
    }

    /** Registers a workflow written as code and starts to poll for {@code workflow-executor} tasks. */
    @SuppressWarnings("unchecked")
    public synchronized <I, O> void workflow(String name, Class<I> inputType, WorkflowFunction<I, O> fn) {
        Registration registration = new Registration(inputType, (WorkflowFunction<Object, Object>) fn);
        if (registry.putIfAbsent(name, registration) != null) {
            throw new IllegalArgumentException("Workflow already registered: " + name);
        }
        if (!executorRegistered) {
            workerExecutor.addBean(new WorkflowExecutorWorker(workflowClient, registry, om));
            workerExecutor.startPolling();
            executorRegistered = true;
        }
    }

    /** Starts an execution of the named workflow and returns the workflow id. */
    @SuppressWarnings("unchecked")
    public String start(String name, Object input) {
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName(name);
        request.setVersion(1);
        request.setInput(input == null ? Map.of() : om.convertValue(input, Map.class));
        request.setWorkflowDef(DurableWorkflowDef.build(name));
        return workflowClient.startWorkflow(request);
    }

    @Override
    public void close() {
        workerExecutor.shutdown();
    }
}
