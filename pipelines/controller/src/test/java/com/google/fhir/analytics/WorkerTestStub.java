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

import java.nio.file.Path;
import java.util.Map;

/**
 * A stand-in for {@link PipelineWorker} used by {@link SubprocessPipelineExecutorTest}. It speaks
 * the {@link WorkerProtocol} but does no pipeline work, so tests can drive a <em>real child JVM</em>
 * through every terminal outcome (success, no-work, error, crash-without-result) deterministically
 * and fast. Invoked as: {@code java WorkerTestStub <taskFile> <behavior>}.
 */
public final class WorkerTestStub {

  private WorkerTestStub() {}

  @SuppressWarnings("NullAway") // test stub; the task file always has a parent
  public static void main(String[] args) throws Exception {
    Path workDir = Path.of(args[0]).toAbsolutePath().getParent();
    Path resultFile = workDir.resolve(WorkerProtocol.RESULT_FILE);
    String behavior = args.length > 1 ? args[1] : "SUCCESS";

    switch (behavior) {
      case "SUCCESS":
        WorkerProtocol.writeResult(
            resultFile,
            new WorkerProtocol.Result(
                WorkerProtocol.ResultStatus.SUCCESS,
                null,
                Map.of("PipelineMetrics_numFetchedResources_Patient", 5L)));
        System.exit(0);
        break;
      case "NO_WORK":
        WorkerProtocol.writeResult(
            resultFile,
            new WorkerProtocol.Result(WorkerProtocol.ResultStatus.NO_WORK, null, null));
        System.exit(0);
        break;
      case "ERROR":
        WorkerProtocol.writeResult(
            resultFile,
            new WorkerProtocol.Result(
                WorkerProtocol.ResultStatus.ERROR, "java.lang.RuntimeException: boom", null));
        System.exit(1);
        break;
      case "SUCCESS_THEN_DIE":
        // Wrote a valid SUCCESS result, then died during teardown (e.g. OOM-killed after the
        // work finished). The executor must treat a dirty exit as failure, result or not.
        WorkerProtocol.writeResult(
            resultFile,
            new WorkerProtocol.Result(
                WorkerProtocol.ResultStatus.SUCCESS,
                null,
                Map.of("PipelineMetrics_numFetchedResources_Patient", 5L)));
        System.exit(3);
        break;
      case "EXIT_ZERO_NO_RESULT":
        // A clean exit that violated the protocol (no result file) must never read as success.
        System.out.println("worker finished without writing a result");
        System.exit(0);
        break;
      case "WRITE_PROGRESS_THEN_SLEEP":
        WorkerProtocol.writeProgress(
            workDir.resolve(WorkerProtocol.PROGRESS_FILE), new WorkerProtocol.Progress(10, 4, 2));
        Thread.sleep(60_000);
        System.exit(0);
        break;
      case "CRASH_NO_RESULT":
        // Simulate an OOM-kill: emit output and die without writing a result file.
        System.out.println("worker line 1");
        System.out.println("worker line 2 CRASH");
        System.exit(137);
        break;
      case "SLEEP":
        // Stays alive so a test can exercise stop().
        Thread.sleep(60_000);
        System.exit(0);
        break;
      default:
        System.err.println("unknown behavior: " + behavior);
        System.exit(2);
    }
  }
}
