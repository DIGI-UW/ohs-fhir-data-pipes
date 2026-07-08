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
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Wiring test for the startup sweep of orphaned {@code flink-rpc-akka_*.jar} files (see the sweep
 * call at the top of {@link PipelineManager}'s {@code @PostConstruct initDwhStatus()}).
 *
 * <p>{@code PipelineManagerTest#testCleanStaleFlinkRpcJars*} already pins the HELPER semantics
 * against a real filesystem. What nothing else guards is the WIRING: that the helper is actually
 * invoked during application startup, before any pipeline can run. A refactor of {@code
 * initDwhStatus()} could silently drop that call and every unit test would stay green — while
 * production hosts in a crash loop would again fill their disks (Jira MG-89). This test boots the
 * real Spring context (same setup as {@link ControlPanelApplicationTests}) with a fake orphaned
 * jar pre-planted in the real {@code java.io.tmpdir}, and asserts the jar is gone once the context
 * is up.
 *
 * <p>Ordering guarantees this relies on (both already relied on by {@code
 * ControlPanelApplicationTests}, whose mock FHIR server must be up before {@code @PostConstruct}
 * validates the FHIR source):
 *
 * <ul>
 *   <li>JUnit runs {@code @BeforeAll} before the SpringExtension loads the application context, so
 *       the jar is planted before the sweep can run.
 *   <li>The extra inline property below changes the context cache key, forcing a FRESH context
 *       (and hence a fresh {@code @PostConstruct} run) even when another {@code @SpringBootTest}
 *       in the same JVM already loaded one with {@code application-test.properties}.
 * </ul>
 *
 * <p>Attribution: no pipeline run is ever triggered while this context loads (no DWH exists, so
 * the scheduler cannot start an incremental run), so the only code path that can delete the
 * planted jar is the {@code @PostConstruct} sweep. On upstream/master — no sweep anywhere — this
 * test fails.
 *
 * <p>Honest gap: this does NOT guard the second call-site, the post-run sweep in {@code
 * PipelineThread}'s {@code finally} block. Exercising that in-JVM requires driving a real Flink
 * MiniCluster run (or reflection into a private nested class), which is the domain of the manual
 * docker-compose E2E (compose-controller-spark-sql-single.yaml + POST /run + docker kill). The
 * startup sweep alone bounds disk usage — at most one jar is orphaned per JVM death and it is
 * removed at the next boot — so the unguarded finally-block sweep is defense in depth, not the
 * primary fix.
 */
@SpringBootTest
@AutoConfigureObservability
@TestPropertySource(
    locations = "classpath:application-test.properties",
    // Distinct cache key: guarantees a fresh context, i.e. @PostConstruct runs AFTER @BeforeAll.
    properties = "test.flink-rpc-jar-startup-sweep.marker=true")
class FlinkRpcJarStartupSweepTest {

  private static MockWebServer mockFhirServer;
  private static Path plantedStaleJar;
  private static Path plantedControlFile;

  @BeforeAll
  static void plantOrphanedJarAndStartMockFhirServer() throws IOException {
    // Same mock FHIR source as ControlPanelApplicationTests; @PostConstruct validates it.
    mockFhirServer = new MockWebServer();
    mockFhirServer.start(9091);
    mockFhirServer.enqueue(MockUtil.getMockResponse("data/fhir-metadata-sample.json"));
    mockFhirServer.enqueue(MockUtil.getMockResponse("data/fhir-metadata-sample.json"));
    mockFhirServer.enqueue(MockUtil.getMockResponse("data/patient-count-sample.json"));

    // Simulate the jar a previous, OOM-killed JVM left behind — in the REAL java.io.tmpdir, the
    // exact directory the production sweep reads. Unique name so parallel/leftover state on a dev
    // machine cannot collide; mtime firmly in the past so it is unambiguously stale.
    Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
    String unique = UUID.randomUUID().toString();
    long tenMinutesAgo = System.currentTimeMillis() - 600_000L;

    plantedStaleJar = tmpDir.resolve("flink-rpc-akka_" + unique + ".jar");
    Files.createFile(plantedStaleJar);
    assertThat(
        "test setup: could not backdate the planted jar",
        plantedStaleJar.toFile().setLastModified(tenMinutesAgo),
        is(true));

    // Control file: equally old, but name does not match the sweep pattern; must survive. The
    // sweep filter excludes it by name alone, so its mtime is not load-bearing for this test, but
    // we still check the return value for consistency with the planted jar above.
    plantedControlFile = tmpDir.resolve("unrelated-" + unique + ".jar");
    Files.createFile(plantedControlFile);
    assertThat(
        "test setup: could not backdate the control file",
        plantedControlFile.toFile().setLastModified(tenMinutesAgo),
        is(true));
  }

  @Test
  void startupSweepDeletesOrphanedJarBeforeAnyRun() {
    // If the cleanStaleFlinkRpcJars call is dropped from @PostConstruct (upstream/master
    // behavior), the planted jar survives context startup and this assertion fails.
    assertThat(
        "orphaned flink-rpc-akka jar should be swept during @PostConstruct",
        Files.exists(plantedStaleJar),
        is(false));
    assertThat(
        "non-matching file must not be touched by the startup sweep",
        Files.exists(plantedControlFile),
        is(true));
  }

  @AfterAll
  static void tearDown() throws IOException {
    mockFhirServer.shutdown();
    // The jar should already be gone; delete defensively so a failing run leaves no litter.
    Files.deleteIfExists(plantedStaleJar);
    Files.deleteIfExists(plantedControlFile);
  }
}
