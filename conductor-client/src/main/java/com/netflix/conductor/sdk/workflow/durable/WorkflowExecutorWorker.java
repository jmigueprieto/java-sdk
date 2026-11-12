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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Worker for the {@code workflow-executor} task: runs the workflow code and returns the next tasks. */
public final class WorkflowExecutorWorker {

    record Registration(Class<?> inputType, WorkflowFunction<Object, Object> fn) {}

    private static final Set<String> SCAFFOLDING =
            Set.of("DO_WHILE", "FORK", "FORK_JOIN", "FORK_JOIN_DYNAMIC", "JOIN", "SWITCH");

    private final WorkflowClient workflowClient;
    private final Map<String, Registration> registry;
    private final ObjectMapper om;

    WorkflowExecutorWorker(WorkflowClient workflowClient, Map<String, Registration> registry, ObjectMapper om) {
        this.workflowClient = workflowClient;
        this.registry = registry;
        this.om = om;
    }

    @WorkerTask(DurableExecution.EXECUTOR_TASK)
    public TaskResult execute(Task task) {
        TaskResult result = new TaskResult(task);
        String function = String.valueOf(task.getInputData().get("function"));
        Registration registration = registry.get(function);
        if (registration == null) {
            // retryable: another instance may have the workflow
            result.setStatus(TaskResult.Status.FAILED);
            result.setReasonForIncompletion("Unknown workflow: " + function);
            return result;
        }

        try {
            Decision decision = TaskCallHandler.run(
                    registration.fn(),
                    input(task, registration.inputType()),
                    new TaskCallHandler(task.getWorkflowInstanceId(), history(task), om));
            result.setOutputData(output(decision));
            result.setStatus(TaskResult.Status.COMPLETED);
        } catch (RuntimeException e) {
            result.setStatus(TaskResult.Status.FAILED_WITH_TERMINAL_ERROR);
            result.setReasonForIncompletion(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return result;
    }

    private Object input(Task task, Class<?> inputType) {
        Object raw = task.getInputData().get("input");
        if (raw == null && Map.class.isAssignableFrom(inputType)) {
            return Map.of();
        }
        return om.convertValue(raw, inputType);
    }

    private List<Task> history(Task executorTask) {
        return workflowClient.getWorkflow(executorTask.getWorkflowInstanceId(), true).getTasks().stream()
                .filter(t -> t.getTaskType() == null || !SCAFFOLDING.contains(t.getTaskType()))
                .filter(t -> !DurableExecution.EXECUTOR_TASK.equals(t.getTaskDefName()))
                .toList();
    }

    private Map<String, Object> output(Decision decision) {
        List<Map<String, Object>> forkedTasks = new ArrayList<>();
        Map<String, Object> forkedTasksInputs = new LinkedHashMap<>();
        List<String> scheduled = new ArrayList<>();
        for (PendingTask pending : decision.pending()) {
            Map<String, Object> forked = new LinkedHashMap<>();
            forked.put("name", pending.name());
            forked.put("taskReferenceName", pending.referenceName());
            forked.put("type", "SIMPLE");
            // optional: a failed task reaches the workflow code as TaskFailedException
            forked.put("optional", true);
            forkedTasks.add(forked);
            forkedTasksInputs.put(pending.referenceName(), pending.input());
            scheduled.add(pending.referenceName());
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("done", decision.done());
        output.put("result", decision.result());
        output.put("forkedTasks", forkedTasks);
        output.put("forkedTasksInputs", forkedTasksInputs);
        output.put("scheduled", scheduled);
        return output;
    }
}
