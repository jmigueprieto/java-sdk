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
package com.netflix.conductor.sdk.workflow.durable.example;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.client.http.ConductorClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.sdk.workflow.durable.DurableExecution;
import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;
import io.orkes.conductor.client.ApiClient;

import java.time.Duration;
import java.util.List;

public class CheckoutExample {

    // The workflow input
    public record Order(String userId, int amount) {}

    public record User(String id, String name, boolean vip) {}

    // Worker interfaces - Similar to Temporal's Activities.
    public interface UserTasks {
        @WorkerTask("get_user")
        User getUser(@InputParam("id") String id);
    }

    public interface PaymentTasks {
        @WorkerTask("apply_discount")
        int applyDiscount(@InputParam("amount") int amount);

        @WorkerTask("charge")
        String charge(@InputParam("userId") String userId, @InputParam("amount") int amount);
    }

    // The worker implementations
    public static class UserTasksImpl implements UserTasks {
        @Override
        public User getUser(String id) {
            return new User(id, "User " + id, id.startsWith("vip"));
        }
    }

    public static class PaymentTasksImpl implements PaymentTasks {
        @Override
        public int applyDiscount(int amount) {
            return amount * 90 / 100;
        }

        @Override
        public String charge(String userId, int amount) {
            return "charged " + amount + " to " + userId;
        }
    }

    // Our main method - The workflow runs here.
    public static void main(String[] args) throws InterruptedException {
        var client = getClient();
        var conductor = new DurableExecution(client);

        // Register workers
        conductor.workers(new UserTasksImpl(), new PaymentTasksImpl());
        // Define the workflow
        conductor.workflow("checkout", Order.class, (ctx, order) -> {
            User user = ctx.tasks(UserTasks.class).getUser(order.userId());
            PaymentTasks payments = ctx.tasks(PaymentTasks.class);
            int amount = order.amount();
            if (user.vip()) {
                amount = payments.applyDiscount(amount);
            }
            return payments.charge(user.id(), amount);
        });

        // start the workflows
        var ids = List.of(
                conductor.start("checkout", new Order("vip-1", 100)),
                conductor.start("checkout", new Order("u-2", 100)));

        var workflowClient = new WorkflowClient(client);
        for (String id : ids) {
            var workflow = workflowClient.getWorkflow(id, false);
            for (int i = 0; i < 60 && !workflow.getStatus().isTerminal(); i++) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(500));
                workflow = workflowClient.getWorkflow(id, false);
            }
            System.out.println(id + " " + workflow.getStatus() + " " + workflow.getOutput());
        }

        // the workers keep polling: a real application stays up, this example stops here
        System.exit(0);
    }

    private static ConductorClient getClient() {
        ConductorClient client = ApiClient.builder().useEnvVariables(true).build();
        return client;
    }
}
