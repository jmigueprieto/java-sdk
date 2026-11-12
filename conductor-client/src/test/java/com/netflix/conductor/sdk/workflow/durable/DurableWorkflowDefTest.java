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

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurableWorkflowDefTest {

    @Test
    void buildsLoopShape() {
        var def = DurableWorkflowDef.build("checkout");

        assertEquals("checkout", def.getName());
        assertEquals("durable", def.getMetadata().get("classifier"));
        assertEquals("${workflow_executor.output.result}", def.getOutputParameters().get("result"));

        assertEquals(1, def.getTasks().size());
        var loop = def.getTasks().get(0);
        assertEquals("DO_WHILE", loop.getType());
        assertEquals(
                List.of("SIMPLE", "FORK_JOIN_DYNAMIC", "JOIN"),
                loop.getLoopOver().stream().map(WorkflowTask::getType).toList());

        var executor = loop.getLoopOver().get(0);
        assertEquals("workflow-executor", executor.getName());
        assertEquals("workflow_executor", executor.getTaskReferenceName());
        assertEquals("checkout", executor.getInputParameters().get("function"));
    }
}
