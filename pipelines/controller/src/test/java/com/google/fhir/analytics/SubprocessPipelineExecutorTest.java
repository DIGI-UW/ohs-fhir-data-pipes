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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirVersionEnum;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests the process-handling of {@link SubprocessPipelineExecutor} by driving a real child JVM
 * ({@link WorkerTestStub}) through every terminal outcome. This avoids Flink entirely while still
 * exercising the actual spawn/wait/read-result/exception-mapping path — the behavior the whole
 * SUBPROCESS mode hinges on. Full pipeline execution is covered by the docker e2e.
 */
public class SubprocessPipelineExecutorTest {

  private FhirEtlOptions minimalEtlOptions() {
    PipelineOptionsFactory.register(FhirEtlOptions.class);
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setFhirFetchMode(FhirFetchMode.PARQUET);
    options.setFhirVersion(FhirVersionEnum.R4);
    return options;
  }

  /** A DataProperties stub with fixed worker settings; buildCommand is overridden in tests. */
  private DataProperties dataPropsWith(String javaOpts, String jarPath) {
    DataProperties dp = mock(DataProperties.class);
    when(dp.getWorkerJavaOptions()).thenReturn(javaOpts);
    when(dp.getWorkerJarPath()).thenReturn(jarPath);
    return dp;
  }

  /** Runs {@link WorkerTestStub} on the current test classpath with the given behavior. */
  private SubprocessPipelineExecutor stubExecutor(PipelineManager manager, String behavior) {
    return new SubprocessPipelineExecutor(manager, mock(DataProperties.class)) {
      @Override
      List<String> buildCommand(Path taskFile) {
        return List.of(
            Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("java.class.path"),
            WorkerTestStub.class.getName(),
            taskFile.toString(),
            behavior);
      }
    };
  }

  @Test
  public void testBuildCommandIncludesJavaOptsLoaderMainAndTaskFile() {
    SubprocessPipelineExecutor executor =
        new SubprocessPipelineExecutor(
            mock(PipelineManager.class),
            dataPropsWith("-XX:MaxRAMPercentage=50.0 -Xss2m", "/app/controller-bundled.jar"));
    List<String> command = executor.buildCommand(Paths.get("/work/task.json"));

    assertThat(command, hasItem("-XX:MaxRAMPercentage=50.0"));
    assertThat(command, hasItem("-Xss2m")); // whitespace-split into a separate token
    assertThat(command, hasItem("-Dloader.main=com.google.fhir.analytics.PipelineWorker"));
    assertThat(
        command, hasItem("org.springframework.boot.loader.launch.PropertiesLauncher"));
    assertThat(command, hasItem("/app/controller-bundled.jar"));
    assertThat(command, hasItem("/work/task.json"));
    // -cp must immediately precede the jar.
    int cpIndex = command.indexOf("-cp");
    assertThat(command.get(cpIndex + 1), is("/app/controller-bundled.jar"));
  }

  @Test
  public void testResolveWorkerJarUsesConfiguredPath() {
    SubprocessPipelineExecutor executor =
        new SubprocessPipelineExecutor(
            mock(PipelineManager.class), dataPropsWith("", "/custom/app.jar"));
    assertThat(executor.resolveWorkerJar(), is("/custom/app.jar"));
  }

  @Test
  public void testResolveWorkerJarFailsFastOnMultiEntryClasspath() {
    // Under surefire the test classpath has many entries, so auto-detection must fail fast.
    SubprocessPipelineExecutor executor =
        new SubprocessPipelineExecutor(mock(PipelineManager.class), dataPropsWith("", ""));
    IllegalStateException e =
        assertThrows(IllegalStateException.class, executor::resolveWorkerJar);
    assertThat(e.getMessage(), containsString("workerJarPath"));
  }

  @Test
  public void testRunEtlSuccessPublishesCountersAndReturnsTrue() throws Exception {
    PipelineManager manager = mock(PipelineManager.class);
    SubprocessPipelineExecutor executor = stubExecutor(manager, "SUCCESS");

    boolean workDone = executor.runEtl(minimalEtlOptions());

    assertTrue(workDone);
    verify(manager).removePipelineMetrics();
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, Long>> captor = ArgumentCaptor.forClass(Map.class);
    verify(manager).publishPipelineMetrics(captor.capture());
    assertThat(
        captor.getValue().get("PipelineMetrics_numFetchedResources_Patient"), is(5L));
  }

  @Test
  public void testRunEtlNoWorkReturnsFalseAndPublishesNothing() throws Exception {
    PipelineManager manager = mock(PipelineManager.class);
    SubprocessPipelineExecutor executor = stubExecutor(manager, "NO_WORK");

    boolean workDone = executor.runEtl(minimalEtlOptions());

    assertFalse(workDone);
    verify(manager, never()).publishPipelineMetrics(org.mockito.ArgumentMatchers.<Map<String, Long>>any());
  }

  @Test
  public void testRunEtlErrorThrowsWithWorkerStackTrace() {
    PipelineManager manager = mock(PipelineManager.class);
    SubprocessPipelineExecutor executor = stubExecutor(manager, "ERROR");

    SubprocessPipelineExecutor.PipelineWorkerException e =
        assertThrows(
            SubprocessPipelineExecutor.PipelineWorkerException.class,
            () -> executor.runEtl(minimalEtlOptions()));
    assertThat(e.getMessage(), containsString("exit code 1"));
    assertThat(e.getMessage(), containsString("boom"));
  }

  @Test
  public void testRunEtlCrashWithoutResultThrowsWithLogTail() {
    PipelineManager manager = mock(PipelineManager.class);
    SubprocessPipelineExecutor executor = stubExecutor(manager, "CRASH_NO_RESULT");

    SubprocessPipelineExecutor.PipelineWorkerException e =
        assertThrows(
            SubprocessPipelineExecutor.PipelineWorkerException.class,
            () -> executor.runEtl(minimalEtlOptions()));
    assertThat(e.getMessage(), containsString("137"));
    assertThat(e.getMessage(), containsString("worker line 2 CRASH"));
  }

  @Test
  public void testGetProgressReturnsNullWhenIdle() {
    SubprocessPipelineExecutor executor =
        new SubprocessPipelineExecutor(mock(PipelineManager.class), dataPropsWith("", ""));
    assertThat(executor.getProgress(), is(nullValue()));
  }

  @Test
  public void testStopTerminatesRunningWorker() throws Exception {
    PipelineManager manager = mock(PipelineManager.class);
    SubprocessPipelineExecutor executor = stubExecutor(manager, "SLEEP");

    // runEtl blocks in waitFor() while the stub sleeps 60s; stop() must cut it short.
    CompletableFuture<Void> run =
        CompletableFuture.runAsync(
            () -> {
              try {
                executor.runEtl(minimalEtlOptions());
              } catch (Exception expected) {
                // The killed child yields a non-zero exit -> PipelineWorkerException; that's fine.
              }
            });
    // Give the child time to start, then stop it.
    Thread.sleep(3000);
    executor.stop();

    // Without stop() this would hang ~60s; assert it unblocks quickly.
    run.get(20, TimeUnit.SECONDS);
    assertTrue(run.isDone());
  }
}
