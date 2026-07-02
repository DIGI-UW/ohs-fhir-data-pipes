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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

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

  @BeforeEach
  void setUp() {
    DataProperties dataProperties = mock(DataProperties.class);
    MeterRegistry meterRegistry = mock(MeterRegistry.class);
    pipelineManager =
        Mockito.spy(new PipelineManager(dataProperties, dwhFilesManager, meterRegistry));
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

  // NOTE: these tests guard the HELPER's behavior only (name filter, strict '<' cutoff, delete,
  // missing-dir safety). They call the static method directly and would stay green even if both
  // production call-sites were removed; the wiring is guarded by FlinkRpcJarStartupSweepTest
  // (startup) and docker/validate_flink_tmp_jar_sweep.sh (post-run, on-demand).
  @Test
  public void testCleanStaleFlinkRpcJars(@TempDir Path tmpDir) throws Exception {
    // Boundary jar: last-modified exactly equal to cutoff — must be PRESERVED. The comparison is
    // strictly '<'; the post-run sweep passes cutoff = run start time, and on filesystems with
    // second-granularity mtimes the live run's own jar can carry an mtime equal to that start.
    // A regression to '<=' would delete a jar out from under a running MiniCluster. The cutoff is
    // read back from the file (not taken from the clock) so mtime truncation cannot skew it.
    Path boundary = tmpDir.resolve("flink-rpc-akka_boundary.jar");
    Files.createFile(boundary);
    assertThat(boundary.toFile().setLastModified(System.currentTimeMillis()), is(true));
    long cutoff = boundary.toFile().lastModified();

    // Stale jars (last-modified strictly before cutoff) — must be deleted. These model the
    // orphans left behind when a previous JVM was OOM-killed or restarted mid-run (MG-89).
    Path stale1 = tmpDir.resolve("flink-rpc-akka_aaa-111.jar");
    Path stale2 = tmpDir.resolve("flink-rpc-akka_bbb-222.jar");
    Files.createFile(stale1);
    Files.createFile(stale2);
    assertThat(stale1.toFile().setLastModified(cutoff - 2000), is(true));
    assertThat(stale2.toFile().setLastModified(cutoff - 1000), is(true));

    // Newer than cutoff (the current run's jar) — must be preserved.
    Path current = tmpDir.resolve("flink-rpc-akka_current.jar");
    Files.createFile(current);
    assertThat(current.toFile().setLastModified(cutoff + 1000), is(true));

    // Old file whose name does not match flink-rpc-akka*.jar — must be preserved.
    Path unrelated = tmpDir.resolve("other-flink-file.jar");
    Files.createFile(unrelated);
    assertThat(unrelated.toFile().setLastModified(cutoff - 1000), is(true));

    PipelineManager.cleanStaleFlinkRpcJars(tmpDir, cutoff);

    assertThat(Files.exists(stale1), is(false));
    assertThat(Files.exists(stale2), is(false));
    assertThat(Files.exists(boundary), is(true));
    assertThat(Files.exists(current), is(true));
    assertThat(Files.exists(unrelated), is(true));
  }

  @Test
  public void testCleanStaleFlinkRpcJarsMissingDirIsNoOp(@TempDir Path tmpDir) {
    // File.listFiles() returns null for a nonexistent directory. The helper is called from
    // @PostConstruct and from the pipeline thread's finally block; throwing here would abort
    // Spring startup or mask a pipeline failure, so it must degrade to a no-op.
    PipelineManager.cleanStaleFlinkRpcJars(
        tmpDir.resolve("does-not-exist"), System.currentTimeMillis());
  }
}
