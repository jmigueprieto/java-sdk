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
package io.orkes.conductor.sdk;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.sdk.workflow.durable.DurableExecution;
import com.netflix.conductor.sdk.workflow.durable.TaskFailedException;
import com.netflix.conductor.sdk.workflow.executor.task.NonRetryableException;
import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

import io.orkes.conductor.client.util.ClientTestUtil;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurableExecutionTests {

    public record User(String id, boolean vip) {}

    public interface ShopTasks {
        @WorkerTask("durable_get_user")
        User getUser(@InputParam("id") String id);

        @WorkerTask("durable_apply_discount")
        void applyDiscount(@InputParam("userId") String userId);

        @WorkerTask("durable_charge")
        String charge(@InputParam("userId") String userId);
    }

    public static class ShopTasksImpl implements ShopTasks {
        @Override
        public User getUser(String id) {
            return new User(id, id.startsWith("vip"));
        }

        @Override
        public void applyDiscount(String userId) {}

        @Override
        public String charge(String userId) {
            return "charged:" + userId;
        }
    }

    public interface RiskyTasks {
        @WorkerTask("durable_risky")
        String risky(@InputParam("fail") boolean fail);
    }

    public static class RiskyTasksImpl implements RiskyTasks {
        @Override
        public String risky(boolean fail) {
            if (fail) {
                throw new NonRetryableException("risky failed");
            }
            return "fine";
        }
    }

    @Test
    void failedTaskReachesTheFunction() {
        var client = ClientTestUtil.getClient();
        var workflowClient = new WorkflowClient(client);
        try (var conductor = new DurableExecution(client)) {
            conductor.workers(new RiskyTasksImpl());
            conductor.workflow("durable_with_fallback", (ctx, in) -> {
                try {
                    return ctx.tasks(RiskyTasks.class).risky(true);
                } catch (TaskFailedException e) {
                    return "fallback";
                }
            });
            conductor.workflow("durable_no_fallback", (ctx, in) -> ctx.tasks(RiskyTasks.class).risky(true));

            var caughtId = conductor.start("durable_with_fallback", Map.of());
            var uncaughtId = conductor.start("durable_no_fallback", Map.of());

            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).until(() ->
                    workflowClient.getWorkflow(caughtId, false).getStatus().isTerminal()
                            && workflowClient.getWorkflow(uncaughtId, false).getStatus().isTerminal());

            var caught = workflowClient.getWorkflow(caughtId, false);
            assertEquals(Workflow.WorkflowStatus.COMPLETED, caught.getStatus());
            assertEquals("fallback", caught.getOutput().get("result"));

            var uncaught = workflowClient.getWorkflow(uncaughtId, false);
            assertEquals(Workflow.WorkflowStatus.FAILED, uncaught.getStatus());
            assertTrue(uncaught.getReasonForIncompletion().contains("risky failed"),
                    uncaught.getReasonForIncompletion());
        }
    }

    @Test
    void checkoutRunsToCompletion() {
        var client = ClientTestUtil.getClient();
        var workflowClient = new WorkflowClient(client);
        try (var conductor = new DurableExecution(client)) {
            conductor.workers(new ShopTasksImpl());
            conductor.workflow("durable_checkout", (ctx, in) -> {
                var shop = ctx.tasks(ShopTasks.class);
                var user = shop.getUser((String) in.get("userId"));
                if (user.vip()) {
                    shop.applyDiscount(user.id());
                }
                return shop.charge(user.id());
            });

            var vipId = conductor.start("durable_checkout", Map.of("userId", "vip-1"));
            var plainId = conductor.start("durable_checkout", Map.of("userId", "u-2"));

            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).until(() ->
                    workflowClient.getWorkflow(vipId, false).getStatus().isTerminal()
                            && workflowClient.getWorkflow(plainId, false).getStatus().isTerminal());

            var vip = workflowClient.getWorkflow(vipId, true);
            var plain = workflowClient.getWorkflow(plainId, true);

            assertEquals(Workflow.WorkflowStatus.COMPLETED, vip.getStatus());
            assertEquals(Workflow.WorkflowStatus.COMPLETED, plain.getStatus());
            assertEquals("charged:vip-1", vip.getOutput().get("result"));
            assertEquals("charged:u-2", plain.getOutput().get("result"));

            assertTrue(taskNames(vip).containsAll(
                    List.of("durable_get_user", "durable_apply_discount", "durable_charge")));
            assertFalse(taskNames(plain).contains("durable_apply_discount"));
            assertEquals("durable", vip.getWorkflowDefinition().getMetadata().get("classifier"));
        }
    }

    private static List<String> taskNames(Workflow workflow) {
        return workflow.getTasks().stream().map(Task::getTaskDefName).toList();
    }
}
