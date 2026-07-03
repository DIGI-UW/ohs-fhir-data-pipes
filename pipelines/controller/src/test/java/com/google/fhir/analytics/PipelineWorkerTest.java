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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the {@link WorkerProtocol} wire format and {@link PipelineWorker#run} error
 * handling. The happy-path pipeline execution (a real Flink run producing Parquet + timestamp
 * files) is covered end-to-end by the docker controller-spark e2e in SUBPROCESS mode and by the
 * controller's SubprocessPipelineExecutor integration test; those exercise a real child JVM, which
 * is the right level for that behavior. These tests live in the controller module (not batch)
 * because the batch surefire runs 4-way parallel (junit47 provider), which is unsafe to mix with
 * this class's multi-second worker invocations. These tests pin the serialization contract and the
 * fail-safe result-writing that the whole subprocess design depends on.
 */
@SuppressWarnings("NullAway")
public class PipelineWorkerTest {

  @TempDir Path tempDir;

  @Test
  public void testEtlOptionsJsonRoundTrip() {
    PipelineOptionsFactory.register(FhirEtlOptions.class);
    FhirEtlOptions original = PipelineOptionsFactory.as(FhirEtlOptions.class);
    original.setFhirFetchMode(FhirFetchMode.FHIR_SEARCH);
    original.setFhirServerUrl("http://example/fhir");
    original.setFhirServerPassword("s3cr3t"); // must survive so the worker can authenticate
    original.setResourceList("Patient,Observation");
    original.setOutputParquetPath("/dwh/run_TIMESTAMP_X");
    original.setSince("2026-01-02T03:04:05Z");
    original.setFhirVersion(FhirVersionEnum.R4);
    original.setRecursiveDepth(1);
    // Flink options set programmatically by the controller must round-trip too.
    FlinkPipelineOptions flink = original.as(FlinkPipelineOptions.class);
    flink.setFlinkConfDir("/tmp/flink-conf");
    flink.setParallelism(3);

    JsonNode tree = WorkerProtocol.MAPPER.valueToTree(original);
    FhirEtlOptions restored =
        deserialize(tree).as(FhirEtlOptions.class);

    assertThat(restored.getFhirFetchMode(), is(FhirFetchMode.FHIR_SEARCH));
    assertThat(restored.getFhirServerUrl(), equalTo("http://example/fhir"));
    assertThat(restored.getFhirServerPassword(), equalTo("s3cr3t"));
    assertThat(restored.getResourceList(), equalTo("Patient,Observation"));
    assertThat(restored.getOutputParquetPath(), equalTo("/dwh/run_TIMESTAMP_X"));
    assertThat(restored.getSince(), equalTo("2026-01-02T03:04:05Z"));
    assertThat(restored.getFhirVersion(), is(FhirVersionEnum.R4));
    assertThat(restored.getRecursiveDepth(), equalTo(1));
    assertThat(restored.as(FlinkPipelineOptions.class).getFlinkConfDir(), equalTo("/tmp/flink-conf"));
    assertThat(restored.as(FlinkPipelineOptions.class).getParallelism(), equalTo(3));
  }

  @Test
  public void testMergerOptionsJsonRoundTrip() {
    PipelineOptionsFactory.register(ParquetMergerOptions.class);
    ParquetMergerOptions original = PipelineOptionsFactory.as(ParquetMergerOptions.class);
    original.setDwh1("/dwh/full");
    original.setDwh2("/dwh/incremental");
    original.setMergedDwh("/dwh/merged");
    original.setNumShards(7);
    original.setViewDefinitionsDir("/config/views");
    original.setFhirVersion(FhirVersionEnum.R4);
    FlinkPipelineOptions flink = original.as(FlinkPipelineOptions.class);
    flink.setFasterCopy(true);
    flink.setParallelism(2);

    JsonNode tree = WorkerProtocol.MAPPER.valueToTree(original);
    ParquetMergerOptions restored = deserialize(tree).as(ParquetMergerOptions.class);

    assertThat(restored.getDwh1(), equalTo("/dwh/full"));
    assertThat(restored.getDwh2(), equalTo("/dwh/incremental"));
    assertThat(restored.getMergedDwh(), equalTo("/dwh/merged"));
    assertThat(restored.getNumShards(), equalTo(7));
    assertThat(restored.getViewDefinitionsDir(), equalTo("/config/views"));
    assertThat(restored.getFhirVersion(), is(FhirVersionEnum.R4));
    assertThat(restored.as(FlinkPipelineOptions.class).getFasterCopy(), is(true));
    assertThat(restored.as(FlinkPipelineOptions.class).getParallelism(), equalTo(2));
  }

  @Test
  public void testTaskAndResultFileRoundTrip() throws Exception {
    PipelineOptionsFactory.register(FhirEtlOptions.class);
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirFetchMode(FhirFetchMode.PARQUET);
    options.setFhirVersion(FhirVersionEnum.R4);

    Path taskFile = tempDir.resolve(WorkerProtocol.TASK_FILE);
    WorkerProtocol.writeTask(
        taskFile,
        new WorkerProtocol.Task(
            WorkerProtocol.TaskType.ETL, WorkerProtocol.MAPPER.valueToTree(options), 5));
    WorkerProtocol.Task readBack = WorkerProtocol.readTask(taskFile);
    assertThat(readBack.taskType, is(WorkerProtocol.TaskType.ETL));
    assertThat(readBack.progressIntervalSeconds, is(5));
    assertThat(
        deserialize(readBack.options).as(FhirEtlOptions.class).getFhirFetchMode(),
        is(FhirFetchMode.PARQUET));

    Path resultFile = tempDir.resolve(WorkerProtocol.RESULT_FILE);
    WorkerProtocol.writeResult(
        resultFile,
        new WorkerProtocol.Result(
            WorkerProtocol.ResultStatus.SUCCESS, null, Map.of("PipelineMetrics_x", 42L)));
    WorkerProtocol.Result result = WorkerProtocol.readResult(resultFile);
    assertThat(result.status, is(WorkerProtocol.ResultStatus.SUCCESS));
    assertThat(result.errorStackTrace, is(nullValue()));
    assertThat(result.counters.get("PipelineMetrics_x"), equalTo(42L));
  }

  @Test
  public void testProgressAtomicWriteAndRead() throws Exception {
    Path progressFile = tempDir.resolve(WorkerProtocol.PROGRESS_FILE);
    WorkerProtocol.writeProgress(progressFile, new WorkerProtocol.Progress(100, 40, 30, 1_700_000L));
    WorkerProtocol.Progress progress = WorkerProtocol.readProgressOrNull(progressFile);
    assertThat(progress, is(notNullValue()));
    assertThat(progress.totalResources, equalTo(100L));
    assertThat(progress.fetchedResources, equalTo(40L));
    assertThat(progress.mappedResources, equalTo(30L));
  }

  @Test
  public void testReadProgressReturnsNullOnMissingOrGarbageFile() throws Exception {
    Path missing = tempDir.resolve("nope.json");
    assertThat(WorkerProtocol.readProgressOrNull(missing), is(nullValue()));
    Path garbage = Files.createFile(tempDir.resolve("garbage.json"));
    Files.writeString(garbage, "{not valid json");
    assertThat(WorkerProtocol.readProgressOrNull(garbage), is(nullValue()));
  }

  @Test
  public void testRunWithBrokenOptionsWritesErrorResultAndReturnsNonZero() throws Exception {
    // ETL task whose options will fail during setup (FHIR_SEARCH with no server URL) — the worker
    // must capture the failure into result.json and return non-zero, never throw.
    PipelineOptionsFactory.register(FhirEtlOptions.class);
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirFetchMode(FhirFetchMode.FHIR_SEARCH);
    options.setFhirVersion(FhirVersionEnum.R4);
    // Intentionally leave fhirServerUrl unset -> validateOptions/setup fails.

    Path taskFile = tempDir.resolve(WorkerProtocol.TASK_FILE);
    WorkerProtocol.writeTask(
        taskFile,
        new WorkerProtocol.Task(
            WorkerProtocol.TaskType.ETL, WorkerProtocol.MAPPER.valueToTree(options), 5));

    int exit = PipelineWorker.run(taskFile);

    assertThat(exit, is(not(0)));
    WorkerProtocol.Result result =
        WorkerProtocol.readResult(tempDir.resolve(WorkerProtocol.RESULT_FILE));
    assertThat(result.status, is(WorkerProtocol.ResultStatus.ERROR));
    assertThat(result.errorStackTrace, is(notNullValue()));
  }

  @Test
  public void testRunWithUnreadableTaskFileReturnsNonZero() {
    Path missing = tempDir.resolve("does-not-exist.json");
    assertThat(PipelineWorker.run(missing), is(not(0)));
  }

  // --- helpers ---

  private static PipelineOptions deserialize(JsonNode tree) {
    PipelineOptionsFactory.register(FhirEtlOptions.class);
    PipelineOptionsFactory.register(ParquetMergerOptions.class);
    try {
      return WorkerProtocol.MAPPER.treeToValue(tree, PipelineOptions.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
