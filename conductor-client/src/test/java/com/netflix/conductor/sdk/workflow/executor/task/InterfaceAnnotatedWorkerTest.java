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
package com.netflix.conductor.sdk.workflow.executor.task;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class InterfaceAnnotatedWorkerTest {

    interface GreetTasks {
        @WorkerTask("greet")
        String greet(@InputParam("name") String name);
    }

    static class GreetTasksImpl implements GreetTasks {
        @Override
        public String greet(String name) {
            return "hello " + name;
        }
    }

    static class AnnotatedOnBoth implements GreetTasks {
        @Override
        @WorkerTask("greet")
        public String greet(@InputParam("name") String name) {
            return "hi " + name;
        }
    }

    @Test
    void registersWorkerDeclaredOnInterface() {
        var executor = new AnnotatedWorkerExecutor(mock(TaskClient.class));
        executor.addBean(new GreetTasksImpl());

        assertEquals(1, executor.workers.size());
        var worker = executor.workers.get(0);
        assertEquals("greet", worker.getTaskDefName());

        var task = new Task();
        task.setStatus(Task.Status.IN_PROGRESS);
        task.setInputData(Map.of("name", "Ana"));
        var result = worker.execute(task);

        assertEquals(TaskResult.Status.COMPLETED, result.getStatus());
        assertEquals("hello Ana", result.getOutputData().get("result"));
    }

    @Test
    void registersOnceWhenClassAndInterfaceAreAnnotated() {
        var executor = new AnnotatedWorkerExecutor(mock(TaskClient.class));
        executor.addBean(new AnnotatedOnBoth());

        assertEquals(1, executor.workers.size());
    }
}
