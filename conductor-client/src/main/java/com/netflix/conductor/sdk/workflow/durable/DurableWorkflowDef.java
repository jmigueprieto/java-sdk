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

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

/** Builds the generic definition that runs a workflow written as code. */
final class DurableWorkflowDef {

    static final String EXECUTOR_REF = "workflow_executor";
    static final String LOOP_REF = "loop";
    static final String FORK_REF = "fork";
    static final String JOIN_REF = "join";

    private DurableWorkflowDef() {}

    static WorkflowDef build(String name) {
        WorkflowTask executor = new WorkflowTask();
        executor.setName(DurableExecution.EXECUTOR_TASK);
        executor.setTaskReferenceName(EXECUTOR_REF);
        executor.setType(TaskType.SIMPLE.name());
        executor.setInputParameters(Map.of("function", name, "input", "${workflow.input}"));

        WorkflowTask fork = new WorkflowTask();
        fork.setName(FORK_REF);
        fork.setTaskReferenceName(FORK_REF);
        fork.setType(TaskType.FORK_JOIN_DYNAMIC.name());
        fork.setDynamicForkTasksParam("forkedTasks");
        fork.setDynamicForkTasksInputParamName("forkedTasksInputs");
        fork.setInputParameters(Map.of(
                "forkedTasks", "${" + EXECUTOR_REF + ".output.forkedTasks}",
                "forkedTasksInputs", "${" + EXECUTOR_REF + ".output.forkedTasksInputs}"));

        WorkflowTask join = new WorkflowTask();
        join.setName(JOIN_REF);
        join.setTaskReferenceName(JOIN_REF);
        join.setType(TaskType.JOIN.name());

        WorkflowTask loop = new WorkflowTask();
        loop.setName("durable_execution_loop");
        loop.setTaskReferenceName(LOOP_REF);
        loop.setType(TaskType.DO_WHILE.name());
        loop.setLoopCondition("if ($." + EXECUTOR_REF + "['done']) { false; } else { true; }");
        loop.setLoopOver(List.of(executor, fork, join));

        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(1);
        def.setDescription("Workflow written as code: " + name);
        def.setMetadata(Map.of("classifier", DurableExecution.CLASSIFIER, "sdk", "java"));
        def.setTasks(List.of(loop));
        def.setOutputParameters(Map.of("result", "${" + EXECUTOR_REF + ".output.result}"));
        return def;
    }
}
