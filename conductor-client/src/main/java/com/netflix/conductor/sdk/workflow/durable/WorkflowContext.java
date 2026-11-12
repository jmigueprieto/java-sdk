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

/** Handle that workflow code uses to call tasks. */
public interface WorkflowContext {

    /**
     * Returns a stub for an interface whose methods carry {@code @WorkerTask}. A call on the stub
     * schedules the task, or returns its recorded result on replay.
     */
    <T> T tasks(Class<T> taskInterface);

    String workflowId();
}
