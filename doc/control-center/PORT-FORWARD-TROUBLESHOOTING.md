# Troubleshooting Control Center port-forward connection refused

This guide documents a real CFK troubleshooting session in which `kubectl port-forward` successfully opened local port `9021`, but failed as soon as a browser tried to reach Control Center.

The investigation showed that `kubectl port-forward` was working correctly. Control Center had not opened port `9021` inside its Pod and was stuck in a Kafka consumer-group coordinator loop. Kubernetes then failed the liveness probe, killed the container, and restarted it repeatedly.

The leading underlying-cause hypothesis is the new KIP-848 group coordinator that is enabled by default in Confluent Platform 7.9.0 and 7.9.1. Confluent documents that it can cause unintended side effects and recommends explicitly disabling it when affected. The workaround must be applied and the recovery checks must pass before this hypothesis is considered confirmed for this incident.

## Incident status

At the time this guide was created:

- The immediate failure mechanism was confirmed from the live Pod, events, probes, process, and logs.
- Kafka and the `__consumer_offsets` topic were verified as available at inspection time.
- The Confluent-documented broker workaround was identified.
- The workaround still needs to be applied and verified using the recovery procedure in this guide.

Do not describe the incident as resolved until all checks in [Verification after recovery](#verification-after-recovery) pass.

## Origin of the problem

The original command was:

```bash
kubectl -n confluent port-forward pod/controlcenter-0 9021:9021
```

The command initially printed:

```text
Forwarding from 127.0.0.1:9021 -> 9021
Forwarding from [::1]:9021 -> 9021
Handling connection for 9021
```

It then failed with:

```text
failed to connect to localhost:9021 inside namespace ...
IPv4: dial tcp4 127.0.0.1:9021: connect: connection refused
IPv6: dial tcp6 [::1]:9021: connect: connection refused
error: lost connection to pod
```

The first two `Forwarding` messages prove that `kubectl` successfully bound local port `9021`. The failure happened at the last hop, inside the Pod network namespace:

```text
Browser
  -> 127.0.0.1:9021 on the Mac
  -> kubectl port-forward tunnel
  -> Kubernetes API server and kubelet
  -> controlcenter-0 network namespace
  -> 127.0.0.1:9021 inside the Pod
  -> connection refused because Control Center is not listening
```

This distinction matters. Changing from Pod forwarding to Service forwarding cannot repair an application that has not opened its port.

## Problem statement

The expected behavior was:

1. `controlcenter-0` becomes `1/1 Running`.
2. Control Center binds `0.0.0.0:9021` inside the Pod.
3. The health endpoint responds successfully.
4. `kubectl port-forward` carries browser traffic to that listener.
5. The Control Center UI opens and its configured Basic authentication is enforced.

The actual behavior was:

1. The Pod remained `0/1`.
2. The Control Center container entered `CrashLoopBackOff`.
3. Port `9021` remained closed.
4. The liveness probe received `connection refused`.
5. Kubelet killed and restarted the container.
6. The active port-forward lost its target connection.

## Lab environment

| Item | Observed value |
|---|---|
| Namespace | `confluent` |
| Control Center CR | `controlcenter` |
| Control Center Pod | `controlcenter-0` |
| Control Center image | `confluentinc/cp-enterprise-control-center:7.9.1` |
| CFK init image | `confluentinc/confluent-init-container:2.11.1` |
| Kafka image | `confluentinc/cp-server:7.9.1` |
| Kafka Pod | `kafka-0` |
| Kafka internal listener | `kafka.confluent.svc.cluster.local:9071` |
| Control Center listener | `http://0.0.0.0:9021` |
| Kubernetes node | `desktop-control-plane` |
| Node allocatable memory | Approximately `7.75 GiB` |
| Deployment topology | One Kafka broker and one KRaft controller; lab only |

Relevant manifests:

- [cp-singlenode.yaml](../../july-13/cp-singlenode.yaml)
- [control-centre.yaml](../../july-13/control-centre.yaml)
- [Basic authentication troubleshooting](./BASIC-AUTH-TROUBLESHOOTING.md)

## What the live investigation found

### 1. Control Center was not ready

```bash
kubectl -n confluent get pod controlcenter-0 -o wide
kubectl -n confluent get controlcenter controlcenter
```

The Pod was observed as:

```text
READY   STATUS
0/1     CrashLoopBackOff
```

The Control Center CR remained in `PROVISIONING` because zero replicas were ready.

### 2. Kubernetes was restarting the container

Inspect the current and previous container state:

```bash
kubectl -n confluent get pod controlcenter-0 \
  -o jsonpath='{range .status.containerStatuses[?(@.name=="controlcenter")]}ready={.ready} restarts={.restartCount} current={.state.waiting.reason} lastReason={.lastState.terminated.reason} exitCode={.lastState.terminated.exitCode} started={.lastState.terminated.startedAt} finished={.lastState.terminated.finishedAt}{"\n"}{end}'
```

During the incident, the restart count continued increasing and the previous container showed:

```text
lastReason=Error
exitCode=137
```

Exit code `137` means the process received `SIGKILL`. It does not, by itself, prove an out-of-memory kill.

### 3. Events identified the initiator

```bash
kubectl -n confluent get events \
  --field-selector involvedObject.name=controlcenter-0 \
  --sort-by='.lastTimestamp'
```

The important events were:

```text
Readiness probe failed: dial tcp ...:9021: connect: connection refused
Container controlcenter failed liveness probe, will be restarted
Back-off restarting failed container controlcenter
```

This correlation shows that kubelet restarted the container after its liveness probe failed. A readiness failure only removes the Pod from ready Service endpoints; a liveness failure can restart the container.

### 4. The probe allowed only a short startup window

Inspect the generated probes:

```bash
kubectl -n confluent get pod controlcenter-0 \
  -o jsonpath='{range .spec.containers[?(@.name=="controlcenter")]}liveness={.livenessProbe}{"\nreadiness="}{.readinessProbe}{"\n"}{end}podTerminationGrace={.spec.terminationGracePeriodSeconds}{"\n"}'
```

The generated probe and Pod lifecycle settings were:

| Setting | Value |
|---|---:|
| Path | `/2.0/status/app_info` |
| Port | `9021` |
| Initial delay | `60` seconds |
| Period | `10` seconds |
| Failure threshold | `5` |
| Pod termination grace period | `30` seconds |

The observed container lifetime of approximately two minutes matched the liveness-failure and termination sequence.

### 5. Nothing was listening on port 9021

While the container was in its `Running` interval, the listener was tested from inside the container:

```bash
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'curl -sS -o /dev/null \
  -w "HTTP %{http_code}\n" \
  --connect-timeout 2 \
  http://127.0.0.1:9021/2.0/status/app_info'
```

The result was a direct connection refusal. The generated Control Center configuration nevertheless contained the correct intended listener:

```properties
confluent.controlcenter.rest.listeners=http://0.0.0.0:9021
```

Therefore, this was not a missing listener configuration. The application had not reached the point where it could start the listener.

### 6. Control Center was stuck on Kafka group coordination

Inspect current and previous logs:

```bash
kubectl -n confluent logs controlcenter-0 \
  -c controlcenter \
  --timestamps \
  --tail=500

kubectl -n confluent logs controlcenter-0 \
  -c controlcenter \
  --previous \
  --timestamps \
  --tail=500
```

The same messages repeated across Control Center Kafka Streams threads:

```text
Group coordinator ... is unavailable
error response NOT_COORDINATOR
This is not the correct coordinator
Rediscovery will be attempted
```

The affected group was:

```text
_confluent-controlcenter-7-9-1-0
```

The log volume was extreme. During one short container lifetime, hundreds of thousands of coordinator-related lines were generated.

### 7. Kafka and the offsets topic were available

Kafka itself was `RUNNING` and `kafka-0` was ready:

```bash
kubectl -n confluent get kafka,pod -o wide
```

The consumer offsets topic was then inspected:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic __consumer_offsets
```

All 50 partitions had:

```text
Leader: 0
Replicas: 0
Isr: 0
```

There were no unavailable or under-replicated partitions:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --unavailable-partitions

kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --under-replicated-partitions
```

Empty output from both commands is healthy.

This showed that `__consumer_offsets` was available at inspection time. The evidence did not support offsets-topic unavailability or the earlier PVC-loss experiment as the immediate cause of this incident. It does not prove that every broker log, topic, PVC, or earlier storage event was healthy.

### 8. The evidence did not indicate an OOM kill

The Pod reported:

```text
reason=Error
exitCode=137
```

It did not report:

```text
reason=OOMKilled
```

The node also reported no active `MemoryPressure`. During one running interval, the Java process used approximately `724 MiB` resident memory.

These observations do not guarantee that the environment has sufficient capacity, but they show that the confirmed restart trigger in this incident was the failed liveness probe—not an observed Kubernetes OOM kill.

## Root cause

There are two levels to the root cause.

### Confirmed immediate cause

Control Center did not bind port `9021`. Its liveness probe repeatedly received `connection refused`, so kubelet terminated the container. The port-forward then failed because its destination listener did not exist.

### Leading underlying-cause hypothesis

Control Center 7.9.1 was continuously failing to join its Kafka Streams consumer group with `NOT_COORDINATOR` responses. The Kafka CR did not explicitly configure:

```properties
group.coordinator.new.enable=false
```

Confluent Platform 7.9.0 and 7.9.1 enable the new KIP-848 group coordinator by default. Confluent documents that it can cause unintended side effects and gives the configuration above as the resolution.

When the broker startup log is still retained, identify which coordinator started:

```bash
kubectl -n confluent logs kafka-0 -c kafka | \
rg 'GroupCoordinatorService|kafka\.coordinator\.group\.GroupCoordinator'
```

Interpret the class name:

| Log signature | Coordinator implementation |
|---|---|
| `org.apache.kafka.coordinator.group.GroupCoordinatorService` | New KIP-848 coordinator |
| `kafka.coordinator.group.GroupCoordinator` | Classic coordinator |

If neither line is returned, the relevant startup log may have rotated. Absence of the line is not proof that either implementation is active.

The combination of the exact affected version, the absent override, the available offsets partitions, and the continuous coordinator errors makes this the strongest current hypothesis. `NOT_COORDINATOR` is not unique to KIP-848, so the diagnosis becomes confirmed only after the workaround is applied and the verification checks pass.

## Solution

### Step 1: Persist the Kafka coordinator workaround

In the `Kafka` resource in [cp-singlenode.yaml](../../july-13/cp-singlenode.yaml), preserve the existing metrics setting and add the coordinator setting:

```yaml
apiVersion: platform.confluent.io/v1beta1
kind: Kafka
metadata:
  name: kafka
  namespace: confluent
spec:
  configOverrides:
    server:
      - confluent.metrics.reporter.topic.replicas=1
      - group.coordinator.new.enable=false
```

The setting belongs to the Kafka broker resource. Do not put it under `ControlCenter`, `KRaftController`, `KafkaRestClass`, or `KafkaTopic`.

Apply the manifest:

```bash
kubectl apply \
  -f cfk-learning-journey/july-13/cp-singlenode.yaml
```

This is a broker configuration change. CFK should reconcile it and roll the Kafka Pod. Because this lab has only one broker, Kafka will be completely unavailable during that restart.

### Step 2: Observe the Kafka reconciliation

Watch the Kafka CR:

```bash
kubectl -n confluent get kafka kafka -w
```

In a second terminal, watch the Pod:

```bash
kubectl -n confluent get pod kafka-0 -w
```

Wait for readiness after the restart begins:

```bash
kubectl -n confluent wait \
  --for=condition=Ready \
  pod/kafka-0 \
  --timeout=10m
```

If CFK does not reconcile, check whether reconciliation was previously blocked:

```bash
kubectl -n confluent get kafka kafka \
  -o jsonpath='{.metadata.annotations.platform\.confluent\.io/block-reconcile}{"\n"}'
```

If and only if the output is `true`, remove the block:

```bash
kubectl -n confluent annotate kafka kafka \
  platform.confluent.io/block-reconcile-
```

### Step 3: Verify the effective broker configuration

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  grep '^group.coordinator.new.enable=' \
  /mnt/config/shared/kafka.properties
```

Expected:

```text
group.coordinator.new.enable=false
```

Also verify Kafka health again:

```bash
kubectl -n confluent get kafka kafka

kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --unavailable-partitions
```

The Kafka CR should report `RUNNING`, and the unavailable-partitions command should return no partitions.

### Step 4: Recreate only the Control Center Pod

After Kafka is healthy, delete the failed Control Center Pod:

```bash
kubectl -n confluent delete pod controlcenter-0
```

The CFK-managed StatefulSet recreates the Pod. This action does not delete the Control Center PVC.

Watch its recovery:

```bash
kubectl -n confluent get pod controlcenter-0 -w
```

Wait for readiness:

```bash
kubectl -n confluent wait \
  --for=condition=Ready \
  pod/controlcenter-0 \
  --timeout=10m
```

### Step 5: Confirm that the coordinator loop has stopped

```bash
kubectl -n confluent logs controlcenter-0 \
  -c controlcenter \
  --since=5m \
  --timestamps | \
rg -m 30 'NOT_COORDINATOR|coordinator unavailable|not the correct coordinator'
```

No continuing high-frequency loop should be present.

Confirm that the main Control Center group can be described:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group _confluent-controlcenter-7-9-1-0
```

The exact group can vary if `confluent.controlcenter.name` or the Control Center ID is changed. List groups first if necessary:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --list
```

### Step 6: Verify the listener before port-forwarding

```bash
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'curl -sS -o /dev/null \
  -w "HTTP %{http_code}\n" \
  http://127.0.0.1:9021/2.0/status/app_info'
```

Expected:

```text
HTTP 200
```

The health endpoint is deliberately excluded from Control Center Basic authentication so Kubernetes can probe it without credentials.

### Step 7: Start a new port-forward

Prefer the Service after the Pod is ready:

```bash
kubectl -n confluent port-forward \
  service/controlcenter \
  9021:9021
```

Then open:

```text
http://127.0.0.1:9021
```

The Pod form also works after Control Center is healthy:

```bash
kubectl -n confluent port-forward \
  pod/controlcenter-0 \
  9021:9021
```

## Verification after recovery

Do not consider the issue resolved merely because `kubectl port-forward` prints `Forwarding from`.

Run all of these checks:

```bash
# 1. Pod readiness and restart count
kubectl -n confluent get pod controlcenter-0 \
  -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount,STATE:.status.containerStatuses[0].state.running.startedAt'

# 2. Control Center CR state
kubectl -n confluent get controlcenter controlcenter

# 3. Internal health endpoint
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  curl -fsS http://127.0.0.1:9021/2.0/status/app_info

# 4. Recent Kubernetes events
kubectl -n confluent get events \
  --field-selector involvedObject.name=controlcenter-0 \
  --sort-by='.lastTimestamp'

# 5. Recent coordinator failures
kubectl -n confluent logs controlcenter-0 -c controlcenter \
  --since=5m | \
rg -m 30 'NOT_COORDINATOR|coordinator unavailable|not the correct coordinator'
```

Success means:

- `controlcenter-0` remains `1/1 Running`.
- The restart count stops increasing.
- The Control Center CR reports `RUNNING` with one ready replica.
- The internal health endpoint returns successfully.
- No new liveness `Killing` event appears.
- The coordinator messages do not continue in a tight loop.
- The browser can reach the UI through a fresh port-forward.

## If Control Center still starts too slowly

Fix the coordinator problem first. Extending probes while the coordinator loop continues only delays the next restart.

If the coordinator problem is gone but Control Center legitimately needs more startup time, first check what the installed Control Center CRD supports:

```bash
kubectl explain controlcenter.spec.podTemplate.probe \
  --api-version=platform.confluent.io/v1beta1 \
  --recursive
```

Only if the output explicitly includes `startup`, add this startup probe to [control-centre.yaml](../../july-13/control-centre.yaml):

```yaml
spec:
  podTemplate:
    probe:
      startup:
        path: /2.0/status/app_info
        port: 9021
        initialDelaySeconds: 30
        periodSeconds: 10
        timeoutSeconds: 10
        failureThreshold: 30
```

This gives the application approximately five minutes to become healthy before liveness checking takes control.

Apply and observe:

```bash
kubectl apply \
  -f cfk-learning-journey/july-13/control-centre.yaml

kubectl -n confluent get controlcenter,pod -w
```

A startup probe is protection for slow startup. It is not a substitute for fixing fatal configuration, Kafka dependency, authentication, or storage errors.

## Capacity and local Docker considerations

The Kubernetes node exposed approximately `7.75 GiB` of allocatable memory for the entire environment:

- Kubernetes system components
- CFK operator
- KRaft controller
- Kafka broker
- Connect
- Control Center

The Control Center Pod had no explicit resource requests or limits. The current incident did not show `OOMKilled`, but this node is still undersized compared with Confluent's documented sizing for legacy Control Center and may suffer slow startup or instability.

For a local lab:

- Increase Docker Desktop CPU and memory allocation when the host permits it.
- Avoid running unnecessary Confluent components simultaneously.
- Consider reduced infrastructure or management mode if monitoring is not required.
- Do not treat the single-broker deployment as production sizing.

Management mode can reduce the Control Center workload, but disables monitoring and metrics views:

```yaml
spec:
  configOverrides:
    server:
      - confluent.controlcenter.mode.enable=management
```

Preserve all other required `server` overrides when adding this setting.

## What not to do

### Do not change the port-forward syntax first

Both Pod and Service forwarding require Control Center to listen on port `9021`. Switching targets does not solve an application-level refusal.

### Do not troubleshoot Basic authentication first

Authentication occurs after a TCP connection reaches the HTTP server. A bad username or password normally produces an HTTP `401`, not `connection refused`.

### Do not delete Kafka or Control Center PVCs

No evidence indicated corrupt storage or an unavailable offsets topic. Deleting PVCs risks permanent data loss and does not address a coordinator protocol problem.

### Do not delete `__consumer_offsets`

This internal topic contains committed consumer-group offsets. Its partitions were healthy in this incident. Deleting it would destroy consumer progress and would not be an appropriate first response.

### Do not assume exit code 137 always means OOM

Check all three together:

```text
lastState.terminated.reason
Pod events
node memory-pressure and OOM evidence
```

In this incident, `reason=Error` plus the liveness `Killing` event identified the restart path.

### Do not leave a temporary probe relaxation forever

An excessively tolerant liveness or startup probe can hide real failures and extend recovery time. Revisit temporary values after the application is stable.

## Production recommendations

This lab has one broker, one controller, replication factor `1`, and local Docker-backed storage. It demonstrates behavior, but it does not provide production availability.

For a production-like environment:

- Use a supported multi-broker and multi-controller topology.
- Size Control Center, Kafka, Connect, and Kubernetes separately.
- Set explicit resource requests and limits based on measured workload and Confluent guidance.
- Monitor Pod restarts, probe failures, JVM memory, CPU throttling, consumer-group health, and internal-topic availability.
- Use supported Confluent Platform and CFK patch versions.
- Plan an upgrade away from Confluent Platform 7.9.1 rather than relying indefinitely on a known-issue workaround.
- Test broker configuration changes in pre-production before applying them to production.
- Remember that a one-broker rolling restart causes a complete outage.

## Fast diagnostic sequence

Use this sequence when port-forwarding reports an in-Pod connection refusal:

```bash
# 1. Is the Pod ready and stable?
kubectl -n confluent get pod controlcenter-0 -o wide

# 2. Why did the previous container stop?
kubectl -n confluent get pod controlcenter-0 \
  -o jsonpath='{range .status.containerStatuses[?(@.name=="controlcenter")]}restarts={.restartCount} current={.state.waiting.reason} lastReason={.lastState.terminated.reason} exitCode={.lastState.terminated.exitCode}{"\n"}{end}'

# 3. What did Kubernetes observe?
kubectl -n confluent get events \
  --field-selector involvedObject.name=controlcenter-0 \
  --sort-by='.lastTimestamp'

# 4. What did the previous process report?
kubectl -n confluent logs controlcenter-0 \
  -c controlcenter \
  --previous \
  --tail=500

# 5. Is the listener open inside the Pod?
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  curl -sS -o /dev/null \
  -w 'HTTP %{http_code}\n' \
  --connect-timeout 2 \
  http://127.0.0.1:9021/2.0/status/app_info

# 6. Is Kafka healthy?
kubectl -n confluent get kafka,pod

# 7. Are any Kafka partitions unavailable?
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --unavailable-partitions

# 8. Is the documented coordinator workaround effective?
kubectl -n confluent exec kafka-0 -c kafka -- \
  grep '^group.coordinator.new.enable=' \
  /mnt/config/shared/kafka.properties
```

## Troubleshooting decision table

| Observation | Meaning | Next action |
|---|---|---|
| Local `Forwarding from` appears, then in-Pod `connection refused` | Local bind and tunnel setup worked; destination port is closed | Inspect Pod readiness, probes, events, and application logs |
| Pod is `1/1 Running`, internal `curl` succeeds | Application listener is healthy | Retry port-forward and check local port conflicts or browser behavior |
| Pod is `0/1`, readiness fails only | Pod is excluded from ready Service endpoints | Diagnose why the health endpoint is unhealthy |
| Liveness event says the container will restart | Kubelet is initiating restarts | Correlate probe timing with application startup logs |
| `reason=OOMKilled` | Kubernetes recorded an OOM kill | Increase/tune memory and JVM settings; inspect node capacity |
| `reason=Error`, exit `137`, plus liveness `Killing` event | Liveness-triggered termination is the supported conclusion | Fix why the listener never became healthy |
| `NOT_COORDINATOR` loop on CP 7.9.1 | Strong match for the documented KIP-848 issue | Set `group.coordinator.new.enable=false`, roll Kafka, and verify |
| `__consumer_offsets` has no leader or ISR | Consumer coordination cannot work reliably | Repair Kafka replica/storage health before Control Center |
| HTTP `401` after the listener opens | Network and listener are healthy; authentication rejected the request | Troubleshoot credentials and Basic-auth configuration separately |

## What this issue teaches

1. `kubectl port-forward` can be healthy while the target application is unhealthy.
2. `Running` is only a Pod phase; `1/1 Ready` is the meaningful application signal.
3. A readiness failure and a liveness failure have different consequences.
4. Exit code `137` requires event correlation before calling it an OOM.
5. A healthy Kafka CR does not prove every Kafka protocol workflow is healthy.
6. Inspect internal topics and consumer groups before blaming PVCs or DNS.
7. Exact component patch versions matter when diagnosing protocol behavior.
8. Probe relaxation can provide time, but cannot repair a dependency or protocol loop.
9. Generated runtime configuration is the final proof that a CFK override reached the process.
10. A fix is complete only when restarts stop and end-to-end access succeeds.

## References

- [Confluent Platform 7.9 release notes and KIP-848 known issue](https://docs.confluent.io/platform/7.9/release-notes/index.html)
- [Troubleshoot Control Center Legacy](https://docs.confluent.io/platform/7.9/control-center/installation/troubleshooting.html)
- [Control Center Legacy configuration reference](https://docs.confluent.io/platform/7.9/control-center/installation/configuration.html)
- [CFK resource configuration](https://docs.confluent.io/operator/current/co-resources.html)
- [CFK 2.11 deployment planning and sizing](https://docs.confluent.io/operator/2.11/co-plan.html)
- [Kubernetes liveness, readiness, and startup probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/)
- [Kubernetes port-forward reference](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_port-forward/)
