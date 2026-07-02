#!/usr/bin/env bash
#
# validate_flink_tmp_jar_sweep.sh — MG-89 repeatable validation (NOT run in CI).
#
# Validates that the pipeline controller bounds java.io.tmpdir usage by sweeping
# orphaned flink-rpc-akka_*.jar files (~35 MiB each), both at startup
# (@PostConstruct in PipelineManager.initDwhStatus) and after each pipeline run
# (PipelineThread finally block). See PipelineManager.cleanStaleFlinkRpcJars.
#
# WHY THIS IS A SCRIPT AND NOT A CI JOB: the crash phase needs a SIGKILL timed
# between Flink's jar extraction and graceful MiniCluster shutdown; on small CI
# datasets that window can close before a poller sees the jar, which makes a CI
# gate flaky. The sweep WIRING is asserted deterministically here via planted
# jars (Phases A1/A2); the real crash mechanism is best-effort (Phase B) and
# SKIPs loudly instead of flaking. Run this before releases that touch the
# pipeline lifecycle, and after any Flink upgrade (jar name pattern may change).
#
# PREREQUISITES (same stack as the manual e2e in e2e-tests/controller-spark;
# also requires curl and jq on the host):
#   1. Source HAPI FHIR server with test data, e.g.:
#        docker compose -f docker/hapi-compose.yml up -d   (+ upload synthea data)
#   2. Controller stack:
#        PIPELINE_CONFIG=... DWH_ROOT=... \
#          docker compose -f docker/compose-controller-spark-sql-single.yaml up -d --build
#
# USAGE:
#   ./docker/validate_flink_tmp_jar_sweep.sh
#   CONTROLLER_URL=http://localhost:8090 CONTAINER=pipeline-controller ./docker/validate_flink_tmp_jar_sweep.sh
#
# TMP_PATH must match the container's java.io.tmpdir (default /tmp); override it
# if the deployment sets -Djava.io.tmpdir.
#
# EXIT CODE: 0 = all executed phases passed (Phase B may SKIP), 1 = regression.

set -euo pipefail

CONTROLLER_URL="${CONTROLLER_URL:-http://localhost:8090}"
CONTAINER="${CONTAINER:-pipeline-controller}"
TMP_PATH="${TMP_PATH:-/tmp}"          # the container's java.io.tmpdir
BOOT_TIMEOUT="${BOOT_TIMEOUT:-300}"   # seconds; @PostConstruct does heavy init (Flink conf, FHIR ctx, DWH scan)
RUN_TIMEOUT="${RUN_TIMEOUT:-1800}"    # seconds; full pipeline run
PHASE_B_SKIPPED=0

log()  { echo "[jar-sweep $(date +%H:%M:%S)] $*"; }
fail() { log "FAIL: $*"; exit 1; }

jar_count() {
  docker exec "${CONTAINER}" sh -c "ls ${TMP_PATH}/flink-rpc-akka*.jar 2>/dev/null | wc -l" | tr -d '[:space:]'
}

jar_names() {
  docker exec "${CONTAINER}" sh -c "ls ${TMP_PATH}/flink-rpc-akka*.jar 2>/dev/null" || true
}

# Creates an empty jar matching the sweep pattern inside the container's real
# java.io.tmpdir. Its mtime is "now", i.e. naturally OLDER than any cutoff the
# sweeps later compute (boot time / run start time) — no touch -d fragility.
plant_jar() {
  docker exec "${CONTAINER}" sh -c "touch ${TMP_PATH}/$1"
  [[ "$(jar_count)" -ge 1 ]] || fail "could not plant $1 in container ${TMP_PATH}"
}

wait_for_controller() {
  local deadline=$((SECONDS + BOOT_TIMEOUT))
  until curl -sf "${CONTROLLER_URL}/status" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || fail "controller not answering ${CONTROLLER_URL}/status within ${BOOT_TIMEOUT}s"
    sleep 2
  done
}

pipeline_status() {
  curl -sf "${CONTROLLER_URL}/status" 2>/dev/null | jq -r '.pipelineStatus // "UNKNOWN"' || echo "UNREACHABLE"
}

trigger_full_run() {
  curl -sf -X POST "${CONTROLLER_URL}/run?runMode=FULL" >/dev/null \
    || fail "POST /run?runMode=FULL was rejected (is another run in progress?)"
}

wait_for_run_end() {
  local deadline=$((SECONDS + RUN_TIMEOUT))
  while [[ "$(pipeline_status)" == "RUNNING" ]]; do
    (( SECONDS < deadline )) || fail "pipeline still RUNNING after ${RUN_TIMEOUT}s"
    sleep 5
  done
}

###############################################################################
log "Phase A1: startup-sweep wiring (deterministic — no kill-timing race)"
###############################################################################
wait_for_controller
[[ "$(pipeline_status)" == "RUNNING" ]] && fail "a pipeline is already running; start from an idle controller"

plant_jar "flink-rpc-akka_planted-startup.jar"
log "planted jar present before crash: $(jar_names)"

log "SIGKILLing controller JVM (docker kill), then restarting..."
docker kill "${CONTAINER}" >/dev/null
RESTART_EPOCH=$(date +%s)
docker start "${CONTAINER}" >/dev/null
wait_for_controller

COUNT=$(jar_count)
[[ "${COUNT}" == "0" ]] || fail "startup sweep NOT wired: ${COUNT} flink-rpc-akka jar(s) survived reboot: $(jar_names)"
docker logs --since "${RESTART_EPOCH}" "${CONTAINER}" 2>&1 | grep -q "Deleted stale Flink tmp jar" \
  || fail "planted jar disappeared but controller log has no 'Deleted stale Flink tmp jar' line — something other than the sweep removed it"
log "Phase A1 PASS: @PostConstruct sweep removed the planted jar at boot"

###############################################################################
log "Phase A2: post-run-sweep wiring (deterministic)"
###############################################################################
plant_jar "flink-rpc-akka_planted-postrun.jar"
# The run-start cutoff is captured when the pipeline thread starts; sleep BEFORE
# triggering the run so mtime(planted) < cutoff is guaranteed, not incidental.
sleep 2
trigger_full_run
log "FULL run started; waiting for completion (Flink's own graceful cleanup only removes the jar IT extracted — only our finally-block sweep can remove the planted one)..."
wait_for_run_end

COUNT=$(jar_count)
[[ "${COUNT}" == "0" ]] || fail "post-run sweep NOT wired: ${COUNT} jar(s) remain after a completed run: $(jar_names)"
log "Phase A2 PASS: finally-block sweep removed the planted jar after a completed run"

###############################################################################
log "Phase B: real crash mechanism (best-effort — SKIPs if run outpaces poller)"
###############################################################################
trigger_full_run
OBSERVED_JAR=""
while [[ "$(pipeline_status)" == "RUNNING" ]]; do
  OBSERVED_JAR="$(jar_names | head -n1)"
  [[ -n "${OBSERVED_JAR}" ]] && break
  sleep 0.5
done

# Close the false-FAIL window: the run may have finished gracefully between the
# observation above and the kill below, in which case Flink already deleted its
# own jar and there is nothing to orphan — that is a SKIP, not a failure.
if [[ -n "${OBSERVED_JAR}" ]] && ! docker exec "${CONTAINER}" sh -c "test -f '${OBSERVED_JAR}'"; then
  OBSERVED_JAR=""
fi

if [[ -z "${OBSERVED_JAR}" ]]; then
  PHASE_B_SKIPPED=1
  log "Phase B SKIPPED (not passed): run completed before a Flink-extracted jar could be orphaned."
  log "  The real orphaning mechanism was NOT exercised — use a larger dataset to close this gap."
  wait_for_run_end # leave the stack idle
else
  log "observed real Flink-extracted jar mid-run: ${OBSERVED_JAR}"
  log "SIGKILLing mid-run to orphan it, then restarting..."
  docker kill "${CONTAINER}" >/dev/null
  RESTART_EPOCH=$(date +%s)
  docker start "${CONTAINER}" >/dev/null
  wait_for_controller
  COUNT=$(jar_count)
  [[ "${COUNT}" == "0" ]] || fail "real orphaned jar survived reboot (${COUNT} left: $(jar_names)) — sweep pattern may not match real Flink jar names"
  docker logs --since "${RESTART_EPOCH}" "${CONTAINER}" 2>&1 | grep -q "Deleted stale Flink tmp jar" \
    || fail "orphaned jar gone but no sweep log line found since restart"
  log "Phase B PASS: real crash-orphaned jar removed by startup sweep"
fi

###############################################################################
if [[ "${PHASE_B_SKIPPED}" == "1" ]]; then
  log "RESULT: PASS (Phases A1, A2) with Phase B SKIPPED — wiring verified, real-crash mechanism not exercised this run"
else
  log "RESULT: PASS (Phases A1, A2, B) — ${TMP_PATH} jar usage is bounded across crash, reboot, and completed runs"
fi
exit 0
