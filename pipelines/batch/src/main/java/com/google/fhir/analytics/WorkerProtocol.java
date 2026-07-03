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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.jspecify.annotations.Nullable;

/**
 * Wire format shared between the pipeline-controller (which spawns a worker JVM per run in {@code
 * SUBPROCESS} execution mode) and {@link PipelineWorker} (which executes the run). Three files are
 * exchanged in a per-run work directory:
 *
 * <ul>
 *   <li>{@code task.json} — controller → worker: what to run and the Beam options to run it with.
 *   <li>{@code progress.json} — worker → controller: live counters, overwritten atomically while
 *       the run is in flight so the control panel keeps its progress bar.
 *   <li>{@code result.json} — worker → controller: the terminal outcome, written exactly once.
 * </ul>
 *
 * <p>Options travel as a JSON tree (not command-line flags) so programmatically-set fields such as
 * {@code flinkConfDir}, {@code parallelism} and {@code since} survive losslessly and credentials
 * never appear on the child's command line (which is world-readable via {@code /proc}).
 */
public final class WorkerProtocol {

  private WorkerProtocol() {}

  public static final String TASK_FILE = "task.json";
  public static final String PROGRESS_FILE = "progress.json";
  public static final String RESULT_FILE = "result.json";

  /**
   * A Jackson mapper that understands Beam {@link PipelineOptions} (the same module registration
   * Beam uses to ship options to Dataflow). Reused for the whole protocol.
   */
  public static final ObjectMapper MAPPER =
      new ObjectMapper().registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  /** Which pipeline stage the worker should run. */
  public enum TaskType {
    ETL,
    MERGE
  }

  /**
   * Terminal outcome of a worker run. {@code NO_WORK} is distinct from {@code SUCCESS} because an
   * ETL run that finds nothing to fetch must NOT advance the controller's DWH bookkeeping — it
   * writes no timestamp files — whereas a real success does.
   */
  public enum ResultStatus {
    SUCCESS,
    NO_WORK,
    ERROR
  }

  /** {@code task.json}: the controller's instruction to the worker. */
  public static final class Task {
    @Nullable public TaskType taskType;
    // Beam PipelineOptions serialized as a JSON tree (FhirEtlOptions for ETL, ParquetMergerOptions
    // for MERGE). Deserialized with MAPPER.treeToValue(options, PipelineOptions.class).
    @Nullable public JsonNode options;
    public int progressIntervalSeconds = 5;

    public Task() {}

    public Task(TaskType taskType, JsonNode options, int progressIntervalSeconds) {
      this.taskType = taskType;
      this.options = options;
      this.progressIntervalSeconds = progressIntervalSeconds;
    }
  }

  /** {@code progress.json}: mirrors {@code CumulativeMetrics} plus a freshness stamp. */
  public static final class Progress {
    public long totalResources;
    public long fetchedResources;
    public long mappedResources;
    public long epochMillis;

    public Progress() {}

    public Progress(
        long totalResources, long fetchedResources, long mappedResources, long epochMillis) {
      this.totalResources = totalResources;
      this.fetchedResources = fetchedResources;
      this.mappedResources = mappedResources;
      this.epochMillis = epochMillis;
    }
  }

  /** {@code result.json}: the terminal outcome, written once before the worker exits. */
  public static final class Result {
    @Nullable public ResultStatus status;
    // Populated only when status == ERROR.
    @Nullable public String errorStackTrace;
    // Final Beam counters, keyed as "<namespace>_<name>" (matches the controller's actuator gauges).
    @Nullable public Map<String, Long> counters;

    public Result() {}

    public Result(
        ResultStatus status,
        @Nullable String errorStackTrace,
        @Nullable Map<String, Long> counters) {
      this.status = status;
      this.errorStackTrace = errorStackTrace;
      this.counters = counters;
    }
  }

  public static void writeTask(Path taskFile, Task task) throws IOException {
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(taskFile.toFile(), task);
  }

  public static Task readTask(Path taskFile) throws IOException {
    return MAPPER.readValue(taskFile.toFile(), Task.class);
  }

  public static void writeResult(Path resultFile, Result result) throws IOException {
    writeAtomically(resultFile, MAPPER.writeValueAsBytes(result));
  }

  public static Result readResult(Path resultFile) throws IOException {
    return MAPPER.readValue(resultFile.toFile(), Result.class);
  }

  public static void writeProgress(Path progressFile, Progress progress) throws IOException {
    writeAtomically(progressFile, MAPPER.writeValueAsBytes(progress));
  }

  /**
   * Reads {@code progress.json}, returning {@code null} on any error (missing/partial file). The
   * controller treats a null progress as "no live stats yet" and shows a generic running state.
   */
  @Nullable
  public static Progress readProgressOrNull(Path progressFile) {
    try {
      if (!Files.exists(progressFile)) {
        return null;
      }
      return MAPPER.readValue(progressFile.toFile(), Progress.class);
    } catch (IOException e) {
      return null;
    }
  }

  /** Writes via a sibling temp file + atomic move so a concurrent reader never sees a torn file. */
  private static void writeAtomically(Path target, byte[] bytes) throws IOException {
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    Files.write(tmp, bytes);
    try {
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException atomicUnsupported) {
      // Fall back to a non-atomic move on filesystems without atomic-move support.
      Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
