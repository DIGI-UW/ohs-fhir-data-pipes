/*
 * Copyright 2020-2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

/** How the pipeline-controller executes a pipeline run. */
public enum PipelineExecutionMode {
  /**
   * Runs Beam/Flink in the controller's own JVM (the historical behavior). Simplest to operate but
   * the run shares memory and lifecycle with the web app: a run's off-heap growth or an OOM-kill
   * affects the controller itself.
   */
  IN_PROCESS,

  /**
   * Runs each pipeline stage in a short-lived child JVM that exits when the stage finishes. Isolates
   * the run's memory and temp files from the control plane, so an OOM-killed run is reported as a
   * failure while the web app stays up. Recommended for containerized/production deployments.
   */
  SUBPROCESS
}
