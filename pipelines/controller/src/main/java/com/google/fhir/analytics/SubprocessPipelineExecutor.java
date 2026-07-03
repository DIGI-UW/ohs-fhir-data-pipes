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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.fhir.analytics.metrics.CumulativeMetrics;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs each pipeline stage in a short-lived child JVM ({@link PipelineWorker}) launched from the
 * controller's own Spring Boot fat jar via {@code PropertiesLauncher}. Options are handed over as a
 * JSON task file (see {@link WorkerProtocol}); the child's stdout/stderr are streamed to the
 * controller log and its terminal outcome is read back from {@code result.json}. When the child
 * exits, its heap, Flink off-heap memory and temp files go with it, which is the whole point: a
 * run's memory pressure can no longer take down the control plane.
 */
class SubprocessPipelineExecutor implements PipelineExecutor {

  private static final Logger logger = LoggerFactory.getLogger(SubprocessPipelineExecutor.class);

  private static final String WORKER_MAIN = "com.google.fhir.analytics.PipelineWorker";
  private static final String PROPERTIES_LAUNCHER =
      "org.springframework.boot.loader.launch.PropertiesLauncher";
  private static final int LOG_TAIL_LINES = 200;
  private static final int PROGRESS_INTERVAL_SECONDS = 5;

  private final PipelineManager manager;
  private final DataProperties dataProperties;

  @Nullable private volatile Process currentProcess;
  @Nullable private volatile Path currentProgressFile;

  SubprocessPipelineExecutor(PipelineManager manager, DataProperties dataProperties) {
    this.manager = manager;
    this.dataProperties = dataProperties;
  }

  @Override
  public boolean runEtl(FhirEtlOptions options) throws Exception {
    WorkerProtocol.Result result = runWorker(WorkerProtocol.TaskType.ETL, options);
    if (result.status == WorkerProtocol.ResultStatus.NO_WORK) {
      return false;
    }
    manager.removePipelineMetrics();
    if (result.counters != null) {
      manager.publishPipelineMetrics(result.counters);
    }
    return true;
  }

  @Override
  public void runMerger(ParquetMergerOptions mergerOptions) throws Exception {
    WorkerProtocol.Result result = runWorker(WorkerProtocol.TaskType.MERGE, mergerOptions);
    if (result.counters != null) {
      manager.publishPipelineMetrics(result.counters);
    }
  }

  @Override
  @Nullable
  public CumulativeMetrics getProgress() {
    Path progressFile = currentProgressFile;
    if (progressFile == null) {
      return null;
    }
    WorkerProtocol.Progress progress = WorkerProtocol.readProgressOrNull(progressFile);
    if (progress == null) {
      return null;
    }
    return new CumulativeMetrics(
        progress.totalResources, progress.fetchedResources, progress.mappedResources);
  }

  @Override
  public void stop() {
    Process process = currentProcess;
    if (process != null && process.isAlive()) {
      logger.info("Stopping pipeline worker process (pid {})", process.pid());
      process.destroy();
      try {
        if (!process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }
  }

  /** Serializes {@code options}, spawns the worker, waits for it, and returns its parsed result. */
  private WorkerProtocol.Result runWorker(WorkerProtocol.TaskType taskType, PipelineOptions options)
      throws Exception {
    Path workDir = createSecureWorkDir();
    Path taskFile = workDir.resolve(WorkerProtocol.TASK_FILE);
    Path resultFile = workDir.resolve(WorkerProtocol.RESULT_FILE);
    Path progressFile = workDir.resolve(WorkerProtocol.PROGRESS_FILE);
    try {
      WorkerProtocol.writeTask(
          taskFile,
          new WorkerProtocol.Task(
              taskType, WorkerProtocol.MAPPER.valueToTree(options), PROGRESS_INTERVAL_SECONDS));
      currentProgressFile = progressFile;

      List<String> command = buildCommand(taskFile);
      logger.info("Launching pipeline worker: {}", String.join(" ", command));
      ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
      Process process = pb.start();
      currentProcess = process;

      Deque<String> logTail = streamOutput(process);
      int exitCode = process.waitFor();
      logger.info("Pipeline worker exited with code {}", exitCode);

      WorkerProtocol.Result result = readResultOrNull(resultFile);
      if (exitCode != 0
          || result == null
          || result.status == WorkerProtocol.ResultStatus.ERROR) {
        throw new PipelineWorkerException(buildFailureMessage(exitCode, result, logTail));
      }
      return result;
    } finally {
      currentProcess = null;
      currentProgressFile = null;
      deleteQuietly(workDir);
    }
  }

  /** Builds the child JVM command. Package-private for testing. */
  @VisibleForTesting
  List<String> buildCommand(Path taskFile) {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    // Whitespace-separated JVM flags; embedded spaces in a single flag are not supported.
    String workerJavaOptions = dataProperties.getWorkerJavaOptions();
    if (!Strings.isNullOrEmpty(workerJavaOptions)) {
      for (String opt : Splitter.on(' ').omitEmptyStrings().split(workerJavaOptions.trim())) {
        command.add(opt);
      }
    }
    command.add("-Dloader.main=" + WORKER_MAIN);
    command.add("-cp");
    command.add(resolveWorkerJar());
    command.add(PROPERTIES_LAUNCHER);
    command.add(taskFile.toString());
    return command;
  }

  /**
   * Resolves the fat jar the worker JVM should run. Uses {@code workerJarPath} when set; otherwise
   * auto-detects it from {@code java.class.path}, which is the single repackaged jar when the
   * controller was started with {@code java -jar}. Fails fast (rather than at run time) if it cannot
   * be determined.
   */
  @VisibleForTesting
  String resolveWorkerJar() {
    String configured = dataProperties.getWorkerJarPath();
    if (!Strings.isNullOrEmpty(configured)) {
      return configured;
    }
    String classPath = System.getProperty("java.class.path", "");
    if (classPath.endsWith(".jar") && !classPath.contains(java.io.File.pathSeparator)) {
      return classPath;
    }
    throw new IllegalStateException(
        "Cannot auto-detect the controller jar for SUBPROCESS execution (java.class.path is not a"
            + " single jar). Set fhirdata.workerJarPath explicitly.");
  }

  private static Path createSecureWorkDir() throws IOException {
    // The task file carries FHIR/DB credentials, so keep the directory owner-only.
    try {
      return Files.createTempDirectory(
          "pipeline-worker-",
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    } catch (UnsupportedOperationException nonPosix) {
      return Files.createTempDirectory("pipeline-worker-");
    }
  }

  /** Reads and logs the child's merged stdout/stderr, keeping the last {@value #LOG_TAIL_LINES}. */
  private static Deque<String> streamOutput(Process process) {
    Deque<String> tail = new ArrayDeque<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        logger.info("[pipeline-worker] {}", line);
        tail.addLast(line);
        if (tail.size() > LOG_TAIL_LINES) {
          tail.removeFirst();
        }
      }
    } catch (IOException e) {
      logger.warn("Error reading pipeline worker output", e);
    }
    return tail;
  }

  private static WorkerProtocol.@Nullable Result readResultOrNull(Path resultFile) {
    try {
      if (!Files.exists(resultFile)) {
        return null;
      }
      return WorkerProtocol.readResult(resultFile);
    } catch (IOException e) {
      logger.warn("Could not read worker result file", e);
      return null;
    }
  }

  private static String buildFailureMessage(
      int exitCode, WorkerProtocol.@Nullable Result result, Deque<String> logTail) {
    StringBuilder sb = new StringBuilder("Pipeline worker failed (exit code ").append(exitCode);
    sb.append(").");
    if (result != null && !Strings.isNullOrEmpty(result.errorStackTrace)) {
      sb.append("\nWorker error:\n").append(result.errorStackTrace);
    } else {
      sb.append("\nNo result file was written. Last worker output:\n");
      sb.append(String.join("\n", logTail));
    }
    return sb.toString();
  }

  private static void deleteQuietly(Path dir) {
    try (var paths = Files.walk(dir)) {
      paths
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  logger.debug("Could not delete {}", p, e);
                }
              });
    } catch (IOException e) {
      logger.debug("Could not clean up worker work dir {}", dir, e);
    }
  }

  /** Signals a failed worker run; its message carries the child's stack trace or output tail. */
  static class PipelineWorkerException extends Exception {
    PipelineWorkerException(String message) {
      super(message);
    }
  }
}
