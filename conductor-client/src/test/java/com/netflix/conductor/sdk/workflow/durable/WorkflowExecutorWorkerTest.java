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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.sdk.workflow.durable.WorkflowExecutorWorker.Registration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowExecutorWorkerTest {

    private final WorkflowClient workflowClient = mock(WorkflowClient.class);
    private final Map<String, Registration> registry = new HashMap<>();
    private final WorkflowExecutorWorker worker =
            new WorkflowExecutorWorker(workflowClient, registry, new ObjectMapperProvider().getObjectMapper());

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void register(String name, WorkflowFunction<Map<String, Object>, ?> fn) {
        registry.put(name, new Registration(Map.class, (WorkflowFunction) fn));
    }

    private TaskResult execute(String function, Task... history) {
        var workflow = new Workflow();
        workflow.setTasks(List.of(history));
        when(workflowClient.getWorkflow("wf-1", true)).thenReturn(workflow);

        var task = new Task();
        task.setWorkflowInstanceId("wf-1");
        task.setTaskId("t-1");
        task.setStatus(Task.Status.IN_PROGRESS);
        task.setInputData(Map.of("function", function, "input", Map.of("userId", "u1")));
        return worker.execute(task);
    }

    @Test
    @SuppressWarnings("unchecked")
    void schedulesNextTask() {
        register("checkout", TaskCallHandlerTest.CHECKOUT);

        var result = execute("checkout");

        assertEquals(TaskResult.Status.COMPLETED, result.getStatus());
        assertEquals(false, result.getOutputData().get("done"));
        var forkedTasks = (List<Map<String, Object>>) result.getOutputData().get("forkedTasks");
        assertEquals(1, forkedTasks.size());
        assertEquals("get_user", forkedTasks.get(0).get("name"));
        assertEquals("get_user_1", forkedTasks.get(0).get("taskReferenceName"));
        var inputs = (Map<String, Object>) result.getOutputData().get("forkedTasksInputs");
        assertEquals(Map.of("id", "u1"), inputs.get("get_user_1"));
    }

    @Test
    void ignoresScaffoldingTasksAndCompletes() {
        register("checkout", TaskCallHandlerTest.CHECKOUT);
        var loop = TaskCallHandlerTest.task("loop", Task.Status.IN_PROGRESS, Map.of());
        loop.setTaskType("DO_WHILE");
        var executor = TaskCallHandlerTest.task("workflow_executor__1", Task.Status.COMPLETED, Map.of());
        executor.setTaskDefName("workflow-executor");

        var result = execute("checkout",
                loop,
                executor,
                TaskCallHandlerTest.task("get_user_1__1", Task.Status.COMPLETED, Map.of("name", "Bo", "vip", false)),
                TaskCallHandlerTest.task("charge_2__2", Task.Status.COMPLETED, Map.of("result", "ok")));

        assertEquals(TaskResult.Status.COMPLETED, result.getStatus());
        assertEquals(true, result.getOutputData().get("done"));
        assertEquals("ok", result.getOutputData().get("result"));
        assertEquals(List.of(), result.getOutputData().get("forkedTasks"));
    }

    @Test
    void unknownFunctionFails() {
        var result = execute("nope");

        assertEquals(TaskResult.Status.FAILED, result.getStatus());
        assertTrue(result.getReasonForIncompletion().contains("nope"));
    }

    @Test
    void functionErrorIsTerminal() {
        register("broken", (ctx, in) -> {
            throw new IllegalStateException("bad");
        });

        var result = execute("broken");

        assertEquals(TaskResult.Status.FAILED_WITH_TERMINAL_ERROR, result.getStatus());
        assertTrue(result.getReasonForIncompletion().contains("bad"));
    }
}
