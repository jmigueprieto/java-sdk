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

/** Thrown at the call site in workflow code when the task failed. */
public class TaskFailedException extends RuntimeException {

    private final String taskName;
    private final String referenceName;
    private final String reason;

    public TaskFailedException(String taskName, String referenceName, String reason) {
        super("Task " + taskName + " (" + referenceName + ") failed: " + reason);
        this.taskName = taskName;
        this.referenceName = referenceName;
        this.reason = reason;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public String getReason() {
        return reason;
    }
}
