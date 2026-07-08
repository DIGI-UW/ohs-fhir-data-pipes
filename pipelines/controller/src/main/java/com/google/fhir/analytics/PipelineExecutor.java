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

import com.google.fhir.analytics.metrics.CumulativeMetrics;
import org.jspecify.annotations.Nullable;

/**
 * Strategy for running the two pipeline stages of a run. Implementations differ only in <em>where
 * the JVM boundary sits</em>: {@link InProcessPipelineExecutor} runs Beam in the controller JVM;
 * {@link SubprocessPipelineExecutor} runs each stage in a child JVM. Everything else about a run —
 * DWH bookkeeping, Hive tables, run status/details, error capture — is the same and stays in {@code
 * PipelineManager.PipelineThread}.
 */
interface PipelineExecutor {

  /**
   * Runs the ETL (fetch) stage for {@code options} and publishes its metrics.
   *
   * @return {@code true} if resources were processed; {@code false} if there was no work to do (no
   *     timestamp files were written and the DWH must not be advanced).
   */
  boolean runEtl(FhirEtlOptions options) throws Exception;

  /** Runs the incremental merge stage for {@code mergerOptions} and publishes its metrics. */
  void runMerger(ParquetMergerOptions mergerOptions) throws Exception;

  /** Live progress of the stage currently running, or {@code null} if none is available. */
  @Nullable
  CumulativeMetrics getProgress();

  /** Best-effort cancellation of any in-flight execution (used on controller shutdown). */
  void stop();
}
