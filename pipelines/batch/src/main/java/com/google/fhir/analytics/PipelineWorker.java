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

import com.google.common.base.Preconditions;
import com.google.fhir.analytics.metrics.CumulativeMetrics;
import com.google.fhir.analytics.metrics.PipelineMetrics;
import com.google.fhir.analytics.metrics.PipelineMetricsProvider;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for a single pipeline run executed in its own JVM, speaking the {@link
 * WorkerProtocol} (JSON options in; progress and result files out) while reusing the exact same
 * build/run code as the in-process path ({@link FhirEtl}, {@link ParquetMerger}, {@link EtlUtils}).
 * See {@code PipelineExecutionMode} for why runs are isolated in a child process.
 */
public final class PipelineWorker {

  private static final Logger logger = LoggerFactory.getLogger(PipelineWorker.class);

  private PipelineWorker() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      logger.error("Usage: PipelineWorker <task-file>");
      System.exit(2);
    }
    System.exit(run(Paths.get(args[0])));
  }

  /**
   * Runs the task described by {@code taskFile} and writes {@code result.json} next to it. Never
   * throws — every failure is captured into the result file and mapped to a non-zero return code so
   * the parent can react deterministically.
   *
   * @return 0 if the protocol completed (result SUCCESS or NO_WORK), non-zero otherwise.
   */
  public static int run(Path taskFile) {
    Path workDir =
        Preconditions.checkNotNull(
            taskFile.toAbsolutePath().getParent(), "task file must have a parent directory");
    Path resultFile = workDir.resolve(WorkerProtocol.RESULT_FILE);
    Path progressFile = workDir.resolve(WorkerProtocol.PROGRESS_FILE);
    try {
      AvroConversionUtil.initializeAvroConverters();
      PipelineOptionsFactory.register(FhirEtlOptions.class);
      PipelineOptionsFactory.register(ParquetMergerOptions.class);

      WorkerProtocol.Task task = WorkerProtocol.readTask(taskFile);
      WorkerProtocol.TaskType taskType =
          Preconditions.checkNotNull(task.taskType, "task.taskType is required");
      logger.info("PipelineWorker starting task of type {}", taskType);

      switch (taskType) {
        case ETL:
          runEtl(task, resultFile, progressFile);
          return 0;
        case MERGE:
          runMerge(task, resultFile);
          return 0;
        default:
          throw new IllegalArgumentException("Unknown taskType: " + task.taskType);
      }
    } catch (Throwable t) {
      // Best-effort: record the failure so the controller can surface it, then fail.
      logger.error("PipelineWorker failed", t);
      writeErrorResult(resultFile, t);
      return 1;
    }
  }

  private static void runEtl(WorkerProtocol.Task task, Path resultFile, Path progressFile)
      throws Exception {
    FhirEtlOptions options =
        WorkerProtocol.MAPPER.treeToValue(task.options, PipelineOptions.class).as(FhirEtlOptions.class);
    // The runner Class does not always survive the JSON round-trip; set it explicitly. FileSystems
    // must be initialised per-JVM before any file-based source/sink (parity with FhirEtl.main and
    // the controller's @PostConstruct).
    options.setRunner(FlinkRunner.class);
    FileSystems.setDefaultPipelineOptions(options);
    FhirEtl.validateOptions(options);

    AvroConversionUtil avroConversionUtil =
        AvroConversionUtil.getInstance(
            options.getFhirVersion(),
            options.getStructureDefinitionsPath(),
            options.getRecursiveDepth());

    List<Pipeline> pipelines = FhirEtl.setupAndBuildPipelines(options, avroConversionUtil);
    if (pipelines == null || pipelines.isEmpty()) {
      logger.info("No resources found to be fetched; nothing to do.");
      WorkerProtocol.writeResult(
          resultFile, new WorkerProtocol.Result(WorkerProtocol.ResultStatus.NO_WORK, null, null));
      return;
    }

    ProgressReporter reporter = new ProgressReporter(progressFile);
    reporter.start();
    List<PipelineResult> results;
    try {
      results = EtlUtils.runMultiplePipelinesWithTimestamp(pipelines, options);
    } finally {
      reporter.stop();
    }
    WorkerProtocol.writeResult(
        resultFile,
        new WorkerProtocol.Result(
            WorkerProtocol.ResultStatus.SUCCESS, null, collectCounters(results)));
  }

  private static void runMerge(WorkerProtocol.Task task, Path resultFile) throws Exception {
    ParquetMergerOptions options =
        WorkerProtocol.MAPPER
            .treeToValue(task.options, PipelineOptions.class)
            .as(ParquetMergerOptions.class);
    options.setRunner(FlinkRunner.class);
    FileSystems.setDefaultPipelineOptions(options);

    AvroConversionUtil avroConversionUtil =
        AvroConversionUtil.getInstance(
            options.getFhirVersion(),
            options.getStructureDefinitionsPath(),
            options.getRecursiveDepth());

    List<Pipeline> pipelines = ParquetMerger.createMergerPipelines(options, avroConversionUtil);
    List<PipelineResult> results =
        EtlUtils.runMultipleMergerPipelinesWithTimestamp(pipelines, options);
    WorkerProtocol.writeResult(
        resultFile,
        new WorkerProtocol.Result(
            WorkerProtocol.ResultStatus.SUCCESS, null, collectCounters(results)));
  }

  /** Aggregates final Beam counters across pipelines, keyed exactly as the controller's gauges. */
  private static Map<String, Long> collectCounters(List<PipelineResult> results) {
    Map<String, Long> counters = new HashMap<>();
    for (PipelineResult result : results) {
      MetricQueryResults metrics = EtlUtils.getMetrics(result.metrics());
      for (MetricResult<Long> counter : metrics.getCounters()) {
        String key =
            counter.getName().getNamespace() + "_" + counter.getName().getName();
        counters.merge(key, counter.getAttempted(), Long::sum);
      }
    }
    return counters;
  }

  private static void writeErrorResult(Path resultFile, Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    try {
      WorkerProtocol.writeResult(
          resultFile,
          new WorkerProtocol.Result(WorkerProtocol.ResultStatus.ERROR, sw.toString(), null));
    } catch (Exception e) {
      logger.error("Could not write result file after failure", e);
    }
  }

  /**
   * Periodically snapshots the live Flink metrics (the same singleton the in-process controller
   * reads) into {@code progress.json}. Progress reporting must never fail the run, so every error is
   * swallowed. Runs as a daemon thread.
   */
  private static final class ProgressReporter {
    private static final long INTERVAL_MILLIS = 5000L;

    private final Path progressFile;
    private volatile boolean running = true;
    @Nullable private Thread thread;

    ProgressReporter(Path progressFile) {
      this.progressFile = progressFile;
    }

    void start() {
      thread =
          new Thread(
              () -> {
                while (running) {
                  writeOnce();
                  try {
                    Thread.sleep(INTERVAL_MILLIS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              },
              "pipeline-worker-progress");
      thread.setDaemon(true);
      thread.start();
    }

    void stop() {
      running = false;
      if (thread != null) {
        thread.interrupt();
      }
    }

    private void writeOnce() {
      try {
        PipelineMetrics pipelineMetrics =
            PipelineMetricsProvider.getPipelineMetrics(FlinkRunner.class);
        if (pipelineMetrics == null) {
          return;
        }
        CumulativeMetrics cm = pipelineMetrics.getCumulativeMetricsForOngoingBatch();
        if (cm == null) {
          return;
        }
        WorkerProtocol.writeProgress(
            progressFile,
            new WorkerProtocol.Progress(
                cm.getTotalResources(), cm.getFetchedResources(), cm.getMappedResources()));
      } catch (Throwable t) {
        logger.debug("Could not write progress file (non-fatal)", t);
      }
    }
  }
}
