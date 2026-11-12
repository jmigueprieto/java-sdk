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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.config.ObjectMapperProvider;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskCallHandlerTest {

    record User(String name, boolean vip) {}

    interface ShopTasks {
        @WorkerTask("get_user")
        User getUser(@InputParam("id") String id);

        @WorkerTask("apply_discount")
        void applyDiscount(@InputParam("userId") String userId);

        @WorkerTask("charge")
        String charge(@InputParam("userId") String userId);
    }

    static final WorkflowFunction<Map<String, Object>, String> CHECKOUT = (ctx, in) -> {
        var shop = ctx.tasks(ShopTasks.class);
        var userId = (String) in.get("userId");
        var user = shop.getUser(userId);
        if (user.vip()) {
            shop.applyDiscount(userId);
        }
        return shop.charge(userId);
    };

    private final ObjectMapper om = new ObjectMapperProvider().getObjectMapper();
    private final Map<String, Object> input = Map.of("userId", "u1");

    static Task task(String reference, Task.Status status, Map<String, Object> output) {
        var task = new Task();
        task.setReferenceTaskName(reference);
        task.setStatus(status);
        task.setOutputData(output);
        return task;
    }

    private Decision run(WorkflowFunction<Map<String, Object>, String> fn, Task... history) {
        return TaskCallHandler.run(fn, input, new TaskCallHandler("wf-1", List.of(history), om));
    }

    @Test
    void emptyHistorySchedulesFirstCall() {
        var decision = run(CHECKOUT);

        assertFalse(decision.done());
        assertEquals(List.of(new PendingTask("get_user", "get_user_1", Map.of("id", "u1"))), decision.pending());
    }

    @Test
    void vipSchedulesDiscount() {
        var decision = run(CHECKOUT,
                task("get_user_1", Task.Status.COMPLETED, Map.of("name", "Ana", "vip", true)));

        assertFalse(decision.done());
        assertEquals(1, decision.pending().size());
        assertEquals("apply_discount_2", decision.pending().get(0).referenceName());
    }

    @Test
    void nonVipSkipsDiscount() {
        var decision = run(CHECKOUT,
                task("get_user_1", Task.Status.COMPLETED, Map.of("name", "Bo", "vip", false)));

        assertFalse(decision.done());
        assertEquals("charge_2", decision.pending().get(0).referenceName());
    }

    @Test
    void completesWithResult() {
        var decision = run(CHECKOUT,
                task("get_user_1__1", Task.Status.COMPLETED, Map.of("name", "Bo", "vip", false)),
                task("charge_2__2", Task.Status.COMPLETED, Map.of("result", "ok")));

        assertTrue(decision.done());
        assertEquals("ok", decision.result());
        assertTrue(decision.pending().isEmpty());
    }

    @Test
    void failedTaskThrowsAtCallSite() {
        WorkflowFunction<Map<String, Object>, String> withFallback = (ctx, in) -> {
            try {
                return ctx.tasks(ShopTasks.class).getUser("u1").name();
            } catch (TaskFailedException e) {
                return "fallback:" + e.getReason();
            }
        };
        var failed = task("get_user_1", Task.Status.FAILED, Map.of());
        failed.setReasonForIncompletion("boom");

        var decision = run(withFallback, failed);

        assertTrue(decision.done());
        assertEquals("fallback:boom", decision.result());
    }

    @Test
    void optionalTaskThatFailedThrowsAtCallSite() {
        // the server marks a failed optional task as COMPLETED_WITH_ERRORS
        var failed = task("get_user_1__1", Task.Status.COMPLETED_WITH_ERRORS, Map.of());
        failed.setReasonForIncompletion("boom");

        var error = assertThrows(TaskFailedException.class, () -> run(CHECKOUT, failed));

        assertEquals("boom", error.getReason());
    }

    @Test
    void retriedTaskUsesLastAttempt() {
        var decision = run(CHECKOUT,
                task("get_user_1", Task.Status.FAILED, Map.of()),
                task("get_user_1", Task.Status.COMPLETED, Map.of("name", "Bo", "vip", false)));

        assertEquals("charge_2", decision.pending().get(0).referenceName());
    }

    @Test
    void historyMismatchFails() {
        var error = assertThrows(IllegalStateException.class,
                () -> run(CHECKOUT, task("charge_1", Task.Status.COMPLETED, Map.of("result", "ok"))));

        assertTrue(error.getMessage().contains("call 1"), error.getMessage());
    }
}
