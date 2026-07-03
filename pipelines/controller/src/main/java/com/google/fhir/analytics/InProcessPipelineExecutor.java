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
import com.google.fhir.analytics.metrics.PipelineMetrics;
import com.google.fhir.analytics.metrics.PipelineMetricsProvider;
import java.util.List;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.jspecify.annotations.Nullable;

/**
 * Runs pipeline stages in the controller's own JVM using an embedded Flink MiniCluster — the
 * historical execution path, relocated verbatim from {@code PipelineManager.PipelineThread}. Live
 * progress is read from the in-JVM Flink metrics singleton.
 */
class InProcessPipelineExecutor implements PipelineExecutor {

  private final PipelineManager manager;
  private final AvroConversionUtil avroConversionUtil;

  InProcessPipelineExecutor(PipelineManager manager, AvroConversionUtil avroConversionUtil) {
    this.manager = manager;
    this.avroConversionUtil = avroConversionUtil;
  }

  @Override
  public boolean runEtl(FhirEtlOptions options) throws Exception {
    List<Pipeline> pipelines = FhirEtl.setupAndBuildPipelines(options, avroConversionUtil);
    if (pipelines == null || pipelines.isEmpty()) {
      return false;
    }
    List<PipelineResult> pipelineResults =
        EtlUtils.runMultiplePipelinesWithTimestamp(pipelines, options);
    // Remove the metrics of the previous pipeline and register the new metrics.
    manager.removePipelineMetrics();
    pipelineResults.forEach(
        pipelineResult -> manager.publishPipelineMetrics(pipelineResult.metrics()));
    return true;
  }

  @Override
  public void runMerger(ParquetMergerOptions mergerOptions) throws Exception {
    List<Pipeline> mergerPipelines =
        ParquetMerger.createMergerPipelines(mergerOptions, avroConversionUtil);
    List<PipelineResult> mergerPipelineResults =
        EtlUtils.runMultipleMergerPipelinesWithTimestamp(mergerPipelines, mergerOptions);
    mergerPipelineResults.forEach(
        pipelineResult -> manager.publishPipelineMetrics(pipelineResult.metrics()));
  }

  @Override
  @Nullable
  public CumulativeMetrics getProgress() {
    PipelineMetrics pipelineMetrics = PipelineMetricsProvider.getPipelineMetrics(FlinkRunner.class);
    return pipelineMetrics != null ? pipelineMetrics.getCumulativeMetricsForOngoingBatch() : null;
  }

  @Override
  public void stop() {
    // No cancellation for in-process runs (matches the pre-existing behavior).
  }
}
