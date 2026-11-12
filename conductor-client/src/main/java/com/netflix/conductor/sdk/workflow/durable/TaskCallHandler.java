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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.OutputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Runs workflow code against the tasks that the execution already ran. */
final class TaskCallHandler implements WorkflowContext {

    private static final Pattern ITERATION_SUFFIX = Pattern.compile("__\\d+$");
    private static final Pattern CALL_REFERENCE = Pattern.compile("^(.+)_(\\d+)$");

    private final String workflowId;
    private final ObjectMapper om;
    private final Map<String, Task> byReference = new HashMap<>();
    private final Map<Integer, String> taskNameBySeq = new HashMap<>();
    private final List<PendingTask> pending = new ArrayList<>();
    private int seq;

    TaskCallHandler(String workflowId, List<Task> history, ObjectMapper om) {
        this.workflowId = workflowId;
        this.om = om;
        for (Task task : history) {
            String reference = ITERATION_SUFFIX.matcher(task.getReferenceTaskName()).replaceFirst("");
            Matcher call = CALL_REFERENCE.matcher(reference);
            if (!call.matches()) {
                continue;
            }
            // a retry has the same reference name and comes later: the last one wins
            byReference.put(reference, task);
            taskNameBySeq.put(Integer.parseInt(call.group(2)), call.group(1));
        }
    }

    static <I, O> Decision run(WorkflowFunction<I, O> fn, I input, TaskCallHandler ctx) {
        try {
            O result = fn.run(ctx, input);
            return new Decision(true, result, List.of());
        } catch (TaskPending taskPending) {
            return new Decision(false, null, List.copyOf(ctx.pending));
        }
    }

    @Override
    public String workflowId() {
        return workflowId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T tasks(Class<T> taskInterface) {
        if (!taskInterface.isInterface()) {
            throw new IllegalArgumentException(taskInterface.getName() + " is not an interface");
        }
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> taskInterface.getSimpleName() + " task stub";
                };
            }
            WorkerTask workerTask = method.getAnnotation(WorkerTask.class);
            if (workerTask == null) {
                if (method.isDefault()) {
                    return InvocationHandler.invokeDefault(proxy, method, args);
                }
                throw new IllegalStateException("Method " + method + " has no @WorkerTask annotation");
            }
            return call(workerTask.value(), method, args);
        };
        return (T) Proxy.newProxyInstance(taskInterface.getClassLoader(), new Class<?>[] {taskInterface}, handler);
    }

    private Object call(String taskName, Method method, Object[] args) {
        int callSeq = ++seq;
        String reference = taskName + "_" + callSeq;

        String recordedName = taskNameBySeq.get(callSeq);
        if (recordedName != null && !recordedName.equals(taskName)) {
            throw new IllegalStateException(
                    "History does not match the workflow code at call " + callSeq
                            + ": expected " + taskName + ", found " + recordedName);
        }

        Task task = byReference.get(reference);
        if (task == null) {
            pending.add(new PendingTask(taskName, reference, input(method, args)));
            throw TaskPending.INSTANCE;
        }
        if (!task.getStatus().isTerminal()) {
            throw new IllegalStateException("Task " + reference + " is still " + task.getStatus());
        }
        // a failed optional task ends as COMPLETED_WITH_ERRORS, which the model counts as successful
        if (task.getStatus() != Task.Status.COMPLETED) {
            throw new TaskFailedException(taskName, reference, task.getReasonForIncompletion());
        }
        return result(method, task);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> input(Method method, Object[] args) {
        Parameter[] parameters = method.getParameters();
        Map<String, Object> input = new LinkedHashMap<>();
        if (parameters.length == 1 && parameters[0].getAnnotation(InputParam.class) == null) {
            if (args[0] != null) {
                input.putAll(om.convertValue(args[0], Map.class));
            }
            return input;
        }
        for (int i = 0; i < parameters.length; i++) {
            InputParam inputParam = parameters[i].getAnnotation(InputParam.class);
            if (inputParam == null) {
                throw new IllegalStateException(
                        "Parameter " + i + " of " + method + " has no @InputParam annotation");
            }
            input.put(inputParam.value(), args[i]);
        }
        return input;
    }

    /** Mirrors how AnnotatedWorker writes a return value to the task output. */
    private Object result(Method method, Task task) {
        Class<?> raw = method.getReturnType();
        if (raw == void.class || raw == Void.class) {
            return null;
        }
        Map<String, Object> output = task.getOutputData();
        Object value;
        OutputParam outputParam = method.getAnnotatedReturnType().getAnnotation(OutputParam.class);
        if (outputParam != null) {
            value = output.get(outputParam.value());
        } else if (raw.isPrimitive()
                || raw == String.class
                || raw == Boolean.class
                || Number.class.isAssignableFrom(raw)
                || List.class.isAssignableFrom(raw)) {
            value = output.get("result");
        } else {
            value = output;
        }
        return om.convertValue(value, om.constructType(method.getGenericReturnType()));
    }
}
