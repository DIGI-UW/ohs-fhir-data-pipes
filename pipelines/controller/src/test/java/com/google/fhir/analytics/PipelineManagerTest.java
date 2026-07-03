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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.fhir.analytics.metrics.CumulativeMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.support.CronExpression;

@SuppressWarnings("NullAway")
@ExtendWith(MockitoExtension.class)
public class PipelineManagerTest {

  @Mock DwhFilesManager dwhFilesManager;

  private PipelineManager pipelineManager;

  private final LocalDateTime lastRunEndTimestamp = LocalDateTime.of(2025, 12, 29, 10, 0);

  private DataProperties dataProperties;

  @BeforeEach
  void setUp() {
    dataProperties = mock(DataProperties.class);
    MeterRegistry meterRegistry = mock(MeterRegistry.class);
    pipelineManager =
        Mockito.spy(new PipelineManager(dataProperties, dwhFilesManager, meterRegistry));
  }

  /**
   * Prepares the spy manager so runBatchPipeline(false) can start a real PipelineThread whose
   * stages run on the given executor: FULL-mode options rooted in {@code dwhRoot}, a bare
   * FlinkConfiguration (null conf dir is skipped), and executor injection via createExecutor().
   */
  private void wireForFullRun(PipelineExecutor executor, Path dwhRoot) {
    org.springframework.test.util.ReflectionTestUtils.setField(
        pipelineManager, "flinkConfiguration", new FlinkConfiguration());
    FhirEtlOptions options =
        org.apache.beam.sdk.options.PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirFetchMode(FhirFetchMode.FHIR_SEARCH);
    options.setFhirServerUrl("http://localhost:9091/fhir");
    options.setFhirVersion(ca.uhn.fhir.context.FhirVersionEnum.R4);
    options.setOutputParquetPath(dwhRoot.toString());
    Mockito.when(dataProperties.createBatchOptions())
        .thenReturn(PipelineConfig.builder().fhirEtlOptions(options).build());
    Mockito.doReturn(executor).when(pipelineManager).createExecutor();
  }

  private void awaitPipelineEnd() throws InterruptedException {
    long deadline = System.currentTimeMillis() + 20000;
    while (pipelineManager.isRunning() && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
  }

  @Test
  public void testIncrementalModeTriggeredAtRightTime() throws Exception {
    // Mock current time to be after next scheduled time
    LocalDateTime currentTime = LocalDateTime.now();
    Mockito.when(pipelineManager.getNextIncrementalTime()).thenReturn(currentTime.minusMinutes(5));
    Mockito.when(dwhFilesManager.getCurrentTime()).thenReturn(currentTime);

    IllegalStateException illegalStateException =
        assertThrows(
            IllegalStateException.class,
            () -> {
              // We have wrapped in assertThrows because runIncrementalPipeline throws due to
              // unmocked dependencies
              pipelineManager.checkSchedule();

              // The incremental pipeline should be triggered since current time is after next
              // time
              // Note: In a real scenario, currentPipeline would be set, but in test,
              // runIncrementalPipeline will fail due to unmocked dependencies
              // The log message "Incremental run triggered" indicates the triggering logic worked
              // For this test, we assert that the exception message is as expected. We can only
              // get that message if the pipeline was triggered, i.e. runIncrementalPipeline() was
              // invoked.

            });
    assertThat(
        illegalStateException.getMessage(),
        equalTo(
            "cannot start the incremental pipeline while there are no DWHs; run full pipeline"));

    Mockito.verify(pipelineManager, Mockito.times(1)).runIncrementalPipeline();
  }

  @Test
  public void testIncrementalModeNotTriggeredBeforeTime() throws Exception {

    LocalDateTime currentTime = LocalDateTime.now();
    Mockito.when(pipelineManager.getNextIncrementalTime()).thenReturn(currentTime.plusMinutes(5));
    Mockito.when(dwhFilesManager.getCurrentTime()).thenReturn(currentTime);
    pipelineManager.checkSchedule();

    // The incremental pipeline should not be triggered since current time is before next time
    // Assert that currentPipeline is not running
    assertThat(pipelineManager.isRunning(), is(false));
  }

  @Test
  public void testGetNextIncrementalTime() {
    Mockito.when(dwhFilesManager.getCurrentTime()).thenReturn(lastRunEndTimestamp);
    pipelineManager.setLastRunStatus(PipelineManager.LastRunStatus.SUCCESS);
    pipelineManager.setCron(CronExpression.parse("0 * * * * *"));
    LocalDateTime next = pipelineManager.getNextIncrementalTime();
    // Since lastRunEnd is 10:00, next should be 10:01
    assertThat(next, is(equalTo(LocalDateTime.of(2025, 12, 29, 10, 1))));
  }

  @Test
  public void testGetNextIncrementalTimeWhenNoPreviousRun() throws Exception {
    // PipelineManager.lastRunEnd is null at this point since there is no previous run, so
    // getNextIncrementalTime should return null
    LocalDateTime next = pipelineManager.getNextIncrementalTime();
    assertThat(next, is(nullValue()));
  }

  @Test
  public void testPipelineThreadDelegatesEtlToExecutor(@TempDir Path dwhRoot) throws Exception {
    // Guards the WIRING: if PipelineThread.run() stops delegating to the executor (e.g. a refactor
    // calls FhirEtl directly again), this fails. Uses the no-work path so no bookkeeping runs.
    PipelineExecutor executor = mock(PipelineExecutor.class);
    Mockito.when(executor.runEtl(Mockito.any())).thenReturn(false);
    wireForFullRun(executor, dwhRoot);

    pipelineManager.runBatchPipeline(false);
    awaitPipelineEnd();

    Mockito.verify(executor, Mockito.timeout(10000)).runEtl(Mockito.any());
  }

  @Test
  public void testPipelineThreadMapsExecutorFailureToFailedRun(@TempDir Path dwhRoot)
      throws Exception {
    // An executor failure (e.g. the worker JVM was OOM-killed) must flow through the existing
    // error-capture path: error.log written into the run's DWH root and the run marked FAILURE.
    PipelineExecutor executor = mock(PipelineExecutor.class);
    Mockito.when(executor.runEtl(Mockito.any()))
        .thenThrow(new RuntimeException("worker OOM-killed"));
    wireForFullRun(executor, dwhRoot);
    Mockito.doNothing().when(pipelineManager).setLastRunDetails(Mockito.any(), Mockito.any());

    pipelineManager.runBatchPipeline(false);
    awaitPipelineEnd();

    Mockito.verify(pipelineManager, Mockito.timeout(10000))
        .setLastRunDetails(Mockito.any(), Mockito.eq("FAILURE"));
    assertThat(Files.exists(dwhRoot.resolve("error.log")), is(true));
    assertThat(Files.readString(dwhRoot.resolve("error.log")), containsString("worker OOM-killed"));
  }

  @Test
  public void testLiveProgressDelegatesToExecutorWhileRunning(@TempDir Path dwhRoot)
      throws Exception {
    // getCumulativeMetrics() must read progress from the run's executor while a FULL run is live.
    PipelineExecutor executor = mock(PipelineExecutor.class);
    java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    CumulativeMetrics sentinel = new CumulativeMetrics(100, 50, 40);
    Mockito.when(executor.getProgress()).thenReturn(sentinel);
    Mockito.when(executor.runEtl(Mockito.any()))
        .thenAnswer(
            inv -> {
              latch.await();
              return false; // no-work: skips all post-run bookkeeping
            });
    wireForFullRun(executor, dwhRoot);

    pipelineManager.runBatchPipeline(false);
    try {
      long deadline = System.currentTimeMillis() + 10000;
      CumulativeMetrics seen = null;
      while (seen == null && System.currentTimeMillis() < deadline) {
        seen = pipelineManager.getCumulativeMetrics();
        Thread.sleep(50);
      }
      assertThat(seen, is(sentinel));
    } finally {
      latch.countDown();
      awaitPipelineEnd();
    }
  }

  @Test
  public void testDockerShippedConfigBindsAndSelectsSubprocessMode() throws Exception {
    // The shipped docker config is the only thing that turns SUBPROCESS on in production; guard
    // that the file exists, binds to DataProperties, and actually selects SUBPROCESS.
    Path dir = Path.of("").toAbsolutePath();
    Path yaml = null;
    while (dir != null) {
      Path candidate = dir.resolve("docker/config/application.yaml");
      if (Files.exists(candidate)) {
        yaml = candidate;
        break;
      }
      dir = dir.getParent();
    }
    assertThat("docker/config/application.yaml not found above " + Path.of("").toAbsolutePath(),
        yaml, is(org.hamcrest.Matchers.notNullValue()));

    java.util.List<org.springframework.core.env.PropertySource<?>> sources =
        new org.springframework.boot.env.YamlPropertySourceLoader()
            .load("docker-config", new org.springframework.core.io.FileSystemResource(yaml.toFile()));
    org.springframework.core.env.MutablePropertySources mps =
        new org.springframework.core.env.MutablePropertySources();
    sources.forEach(mps::addLast);
    DataProperties bound =
        new org.springframework.boot.context.properties.bind.Binder(
                org.springframework.boot.context.properties.source.ConfigurationPropertySources
                    .from(mps))
            .bind(
                "fhirdata",
                org.springframework.boot.context.properties.bind.Bindable.of(DataProperties.class))
            .get();
    assertThat(bound.getPipelineExecutionMode(), is(PipelineExecutionMode.SUBPROCESS));
  }

  @Test
  public void testExecutorSelectionByMode() {
    DataProperties dp = mock(DataProperties.class);
    PipelineManager manager = new PipelineManager(dp, dwhFilesManager, mock(MeterRegistry.class));

    when(dp.getPipelineExecutionMode()).thenReturn(PipelineExecutionMode.SUBPROCESS);
    assertThat(manager.createExecutor(), instanceOf(SubprocessPipelineExecutor.class));

    when(dp.getPipelineExecutionMode()).thenReturn(PipelineExecutionMode.IN_PROCESS);
    assertThat(manager.createExecutor(), instanceOf(InProcessPipelineExecutor.class));
  }

  @Test
  public void testShutdownIsNoOpWhenNothingRunning() {
    // No pipeline started -> @PreDestroy shutdown() must not throw.
    PipelineManager manager =
        new PipelineManager(mock(DataProperties.class), dwhFilesManager, mock(MeterRegistry.class));
    manager.shutdown();
  }

  @Test
  public void testCleanStaleFlinkRpcJars(@TempDir Path tmpDir) throws Exception {
    long cutoff = System.currentTimeMillis();

    // Stale jars (last-modified before cutoff) — should be deleted.
    Path stale1 = tmpDir.resolve("flink-rpc-akka_aaa-111.jar");
    Path stale2 = tmpDir.resolve("flink-rpc-akka_bbb-222.jar");
    Files.createFile(stale1);
    Files.createFile(stale2);
    stale1.toFile().setLastModified(cutoff - 2000);
    stale2.toFile().setLastModified(cutoff - 1000);

    // Current run's jar (last-modified after cutoff) — must be preserved.
    Path current = tmpDir.resolve("flink-rpc-akka_current.jar");
    Files.createFile(current);
    current.toFile().setLastModified(cutoff + 1000);

    // Unrelated file (name does not match pattern) — must not be deleted.
    Path unrelated = tmpDir.resolve("other-flink-file.jar");
    Files.createFile(unrelated);
    unrelated.toFile().setLastModified(cutoff - 1000);

    int deleted = PipelineManager.cleanStaleFlinkRpcJars(tmpDir, cutoff);

    assertThat(deleted, is(2));
    assertThat(Files.exists(stale1), is(false));
    assertThat(Files.exists(stale2), is(false));
    assertThat(Files.exists(current), is(true));
    assertThat(Files.exists(unrelated), is(true));
  }
}
