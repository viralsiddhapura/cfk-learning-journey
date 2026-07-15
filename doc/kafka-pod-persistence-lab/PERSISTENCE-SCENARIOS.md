# Lab: Simulate every Kafka persistence outcome

This companion lab provides a controlled experiment for every row in the persistence outcome matrix. The normal `kafka-0` Pod-deletion experiment is already covered in [the primary persistence lab](./README.md); this file covers the remaining scenarios.

The commands assume this local learning environment:

| Resource | Value |
|---|---|
| Kubernetes context | `docker-desktop` |
| Namespace | `confluent` |
| Kafka custom resource | `kafka` |
| Kafka StatefulSet | `kafka` |
| Kafka Pod | `kafka-0` |
| Kafka PVC | `data0-kafka-0` |
| Bootstrap Service | `kafka:9092` |
| Kafka replicas | `1` |
| Topic replication factor | `1` |

This one-broker, one-controller topology is useful for failure learning because every record has only one copy. It is not a supported high-availability or production design.

## Read this before running anything

These experiments have intentionally different blast radiuses:

| Scenario | Expected outcome | Risk | Rebuild required afterward? |
|---|---|---:|---:|
| Delete `kafka-0` normally | Records persist | Temporary outage | No |
| Crash the Kafka container | Records persist | Temporary outage | No |
| Restart Docker Desktop | Records normally persist | Cluster-wide temporary outage | No |
| Delete a `KafkaTopic` | That topic's records are deleted | Topic-destructive | Recreate the empty topic |
| Let Kafka retention expire | Expired records are deleted | Topic-destructive | Delete/recreate the test topic |
| Delete `data0-kafka-0` | Broker data is unavailable or deleted | Storage-destructive | Yes |
| Delete `Kafka/kafka` with a `Delete` PV | Kafka broker storage is deleted | Deployment-destructive | Yes |
| Reset the Docker Desktop cluster | The entire local cluster is deleted | Cluster-destructive | Yes |

Run the scenarios in this order:

1. Normal Pod deletion in the primary lab.
2. Kafka process termination and container restart.
3. Docker Desktop restart.
4. KafkaTopic deletion.
5. Kafka retention expiry.
6. PVC deletion.
7. Kafka custom-resource deletion.
8. Docker Desktop Kubernetes reset.

Scenarios 6, 7, and 8 are not ordinary troubleshooting actions. Run them only on this disposable single-broker learning cluster. Rebuild and verify the environment before moving from one of those scenarios to the next.

Do not use these commands against production. Do not add `--force` or `--grace-period=0`.

## Lab index

- [Scenario 1: normal Pod deletion](#scenario-1-delete-kafka-0-normally)
- [Scenario 2: Kafka container restart](#scenario-2-terminate-kafka-and-observe-its-container-restart)
- [Scenario 3: Docker Desktop restart](#scenario-3-restart-docker-desktop-without-resetting-kubernetes)
- [Scenario 4: KafkaTopic deletion](#scenario-4-delete-a-kafkatopic-custom-resource)
- [Scenario 5: Kafka retention expiry](#scenario-5-let-kafka-retention-expire)
- [Scenario 6: Kafka PVC deletion](#scenario-6-delete-kafkas-pvc)
- [Scenario 7: Kafka custom-resource deletion](#scenario-7-delete-the-kafka-custom-resource-with-a-delete-pv)
- [Scenario 8: Docker Desktop Kubernetes reset](#scenario-8-reset-the-docker-desktop-kubernetes-cluster)
- [Troubleshooting fingerprints](#troubleshooting-fingerprints)

## Understand the two independent retention policies

Check the StatefulSet's PVC retention policy:

```bash
kubectl -n confluent get statefulset kafka \
  -o jsonpath='whenDeleted={.spec.persistentVolumeClaimRetentionPolicy.whenDeleted}{" whenScaled="}{.spec.persistentVolumeClaimRetentionPolicy.whenScaled}{"\n"}'
```

Check the PV reclaim policy:

```bash
PV=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.spec.volumeName}')

kubectl get pv "$PV" \
  -o custom-columns='PV:.metadata.name,RECLAIM:.spec.persistentVolumeReclaimPolicy,STATUS:.status.phase,CLAIM:.spec.claimRef.name'
```

The current local cluster was observed with:

```text
StatefulSet: whenDeleted=Retain, whenScaled=Retain
PV:          persistentVolumeReclaimPolicy=Delete
```

These settings answer different questions:

- The StatefulSet policy controls whether the StatefulSet removes its PVC when the workload is deleted or scaled.
- The PV reclaim policy controls what happens to the backing PV after its PVC is deleted.

Therefore, deleting only `kafka-0` keeps `data0-kafka-0`. Explicitly deleting `data0-kafka-0`, however, allows the dynamically provisioned PV and its backing local storage to be deleted.

## Common baseline

Before every scenario, confirm that the cluster is ready:

```bash
kubectl config current-context
kubectl -n confluent get kafka kafka
kubectl -n confluent get pod kafka-0
kubectl -n confluent get pvc data0-kafka-0
```

Expected state:

```text
Context: docker-desktop
Kafka:   READY
Pod:     1/1 Running
PVC:     Bound
```

The preservation scenarios use the `persistence-lab` topic created by the primary lab. Confirm that it exists:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic persistence-lab
```

Produce a fresh, identifiable baseline if necessary:

```bash
printf 'baseline-A\nbaseline-B\nbaseline-C\n' | \
  kubectl -n confluent exec -i kafka-0 -c kafka -- \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic persistence-lab
```

Verify it:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic persistence-lab \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.partition=true \
  --property print.offset=true \
  --property print.value=true
```

The consumer may finish with a timeout message after printing all available records. That only means no additional record arrived before `--timeout-ms` elapsed.

---

## Scenario 1: Delete `kafka-0` normally

Use [the primary persistence lab](./README.md). Its expected result is:

| Observation | Expected value |
|---|---|
| Pod name | Same: `kafka-0` |
| Pod UID | New |
| PVC UID | Same |
| PV name | Same |
| Records | Still readable |

---

## Scenario 2: Terminate Kafka and observe its container restart

### What this tests

This makes the Kafka container's main process exit without deleting the Pod. Because the Pod restart policy is `Always`, the kubelet starts a new Kafka container inside the same Pod.

This environment runs the Kafka Java process as PID 1. Linux treats PID 1 as the init process of the container's PID namespace. An uncatchable `SIGKILL` sent from another process inside that same namespace can be ineffective, which is why `kill -KILL 1` left this Pod at `RESTARTS=0`. The portable in-container trigger below sends `SIGTERM`, which the JVM handles by exiting. This is a controlled process termination, not a hard power-loss simulation.

Expected matrix row:

```text
Kafka PID 1 exits -> same Pod restarts its container -> same PVC -> records persist
```

### Step 1: Record identities and restart count

```bash
POD_UID_BEFORE=$(kubectl -n confluent get pod kafka-0 \
  -o jsonpath='{.metadata.uid}')

PVC_UID_BEFORE=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.metadata.uid}')

RESTARTS_BEFORE=$(kubectl -n confluent get pod kafka-0 \
  -o jsonpath='{.status.containerStatuses[?(@.name=="kafka")].restartCount}')

CONTAINER_BEFORE=$(kubectl -n confluent get pod kafka-0 \
  -o jsonpath='{.status.containerStatuses[?(@.name=="kafka")].containerID}')

echo "POD_UID_BEFORE=$POD_UID_BEFORE"
echo "PVC_UID_BEFORE=$PVC_UID_BEFORE"
echo "RESTARTS_BEFORE=$RESTARTS_BEFORE"
echo "CONTAINER_BEFORE=$CONTAINER_BEFORE"

kubectl -n confluent get pod kafka-0 \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[0].restartCount,READY:.status.containerStatuses[0].ready,CONTAINER:.status.containerStatuses[0].containerID'

kubectl -n confluent get pvc data0-kafka-0 \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,PV:.spec.volumeName'
```

Keep this output.

### Step 2: Watch the Pod

In a second terminal, use a focused Pod watch that includes the container ID:

```bash
kubectl -n confluent get pod kafka-0 \
  --watch \
  --output-watch-events \
  -o custom-columns='EVENT:.type,READY:.object.status.containerStatuses[0].ready,RESTARTS:.object.status.containerStatuses[0].restartCount,CONTAINER_ID:.object.status.containerStatuses[0].containerID'
```

This focused output is more useful than the default table because a fast restart can make `STATUS=Running` appear unchanged. Optionally watch Kubernetes Events in a third terminal, but do not rely on Events alone; a single fast restart may only update or aggregate existing `Created`/`Started` events.

```bash
kubectl -n confluent events --for pod/kafka-0 --watch
```

### Step 3: Ask the Kafka PID 1 process to terminate

Run this once:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  sh -c 'kill -TERM 1'
```

The exec session can disconnect or report a non-zero exit as the container ends. That is expected.

### Step 4: Wait for Kafka to become ready again

```bash
RESTART_OBSERVED=false

for attempt in {1..300}; do
  RESTARTS_NOW=$(kubectl -n confluent get pod kafka-0 \
    -o jsonpath='{.status.containerStatuses[?(@.name=="kafka")].restartCount}')

  if [ "$RESTARTS_NOW" -gt "$RESTARTS_BEFORE" ]; then
    RESTART_OBSERVED=true
    break
  fi

  echo 'Waiting for the Kafka container restart...'
  sleep 1
done

if [ "$RESTART_OBSERVED" = true ]; then
  kubectl -n confluent wait \
    --for=condition=Ready \
    pod/kafka-0 \
    --timeout=5m
else
  echo 'STOP: restartCount did not increase within five minutes.' >&2
fi
```

### Step 5: Compare state

```bash
kubectl -n confluent get pod kafka-0 \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[0].restartCount,READY:.status.containerStatuses[0].ready,CONTAINER:.status.containerStatuses[0].containerID'

kubectl -n confluent get pvc data0-kafka-0 \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,PV:.spec.volumeName'
```

Expected:

- Pod UID is unchanged.
- `RESTARTS` increased by at least one.
- The Kafka container ID changed.
- PVC UID and PV name are unchanged.
- The Kafka records are still readable with the common-baseline consumer.

Inspect the terminated container instance:

```bash
kubectl -n confluent get pod kafka-0 \
  -o jsonpath='{range .status.containerStatuses[?(@.name=="kafka")]}lastReason={.lastState.terminated.reason}{" exitCode="}{.lastState.terminated.exitCode}{" signal="}{.lastState.terminated.signal}{" finishedAt="}{.lastState.terminated.finishedAt}{"\n"}{end}'

kubectl -n confluent logs kafka-0 -c kafka --previous --tail=100
```

`--previous` works here because the container restarted inside the same Pod. It generally cannot retrieve logs from a Pod object that has already been deleted.

---

## Scenario 3: Restart Docker Desktop without resetting Kubernetes

### What this tests

This stops and starts Docker Desktop and temporarily makes the Kubernetes API and workloads unavailable. It does not intentionally reset the Kubernetes cluster.

Expected matrix row:

```text
Restart Docker Desktop -> workloads recover -> existing Kubernetes storage remains -> records normally persist
```

Docker does not publish a per-resource persistence guarantee for every Desktop restart path, so this scenario verifies the result rather than assuming it.

Run this scenario from a normal macOS host terminal, not a terminal embedded in Docker Desktop. Restarting Desktop can close its own UI sessions.

For a Docker Desktop `kind` cluster, use Docker Desktop 4.48 or newer. Version 4.48 fixed a problem that could reset `kind` cluster state during Desktop restart. Check the installed version before continuing:

```bash
docker desktop version
```

### Step 1: Record storage and data state

```bash
kubectl -n confluent get pod kafka-0 \
  -o custom-columns='POD:.metadata.name,POD_UID:.metadata.uid,RESTARTS:.status.containerStatuses[0].restartCount'

kubectl -n confluent get pvc data0-kafka-0 \
  -o custom-columns='PVC:.metadata.name,PVC_UID:.metadata.uid,PV:.spec.volumeName'
```

Consume the common baseline once before restarting.

Create a small Kubernetes API-state canary:

```bash
kubectl create namespace dd-restart-lab \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n dd-restart-lab create configmap restart-marker \
  --from-literal=token="$(date -u +%Y%m%dT%H%M%SZ)" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n dd-restart-lab get configmap restart-marker \
  -o custom-columns='UID:.metadata.uid,TOKEN:.data.token'

CM_UID_BEFORE=$(kubectl -n dd-restart-lab get configmap restart-marker \
  -o jsonpath='{.metadata.uid}')

CM_TOKEN_BEFORE=$(kubectl -n dd-restart-lab get configmap restart-marker \
  -o jsonpath='{.data.token}')

PVC_UID_BEFORE=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.metadata.uid}')

PV_BEFORE=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.spec.volumeName}')

echo "CM_UID_BEFORE=$CM_UID_BEFORE"
echo "CM_TOKEN_BEFORE=$CM_TOKEN_BEFORE"
echo "PVC_UID_BEFORE=$PVC_UID_BEFORE"
echo "PV_BEFORE=$PV_BEFORE"
```

Keep this external terminal open so the before-values remain available after Desktop restarts.

### Step 2: Restart Docker Desktop

Use this command if the Docker Desktop CLI is available:

```bash
docker desktop restart
```

UI alternative:

```text
Docker Desktop -> Troubleshoot -> Restart Docker Desktop
```

If Docker Desktop offers **Update and restart**, do not use that option for this controlled experiment. A product upgrade would add another variable to the persistence test.

Do not select any of these:

- Reset Kubernetes cluster
- Clean / Purge data
- Reset to factory defaults
- Edit cluster and change its version, type, or node count

### Step 3: Wait for the API and node

Temporary `connection refused`, EOF, and timeout errors are expected while Desktop restarts.

```bash
kubectl config use-context docker-desktop

API_READY=false

for attempt in {1..60}; do
  if kubectl --context docker-desktop get --raw='/readyz' >/dev/null 2>&1; then
    API_READY=true
    break
  fi

  echo 'Waiting for the Kubernetes API...'
  sleep 5
done

if [ "$API_READY" = true ]; then
  kubectl --context docker-desktop wait \
    --for=condition=Ready \
    nodes \
    --all \
    --timeout=5m

  kubectl --context docker-desktop -n confluent wait \
    --for=create \
    pod/kafka-0 \
    --timeout=10m

  kubectl --context docker-desktop -n confluent wait \
    --for=condition=Ready \
    pod/kafka-0 \
    --timeout=10m
else
  echo 'STOP: the Kubernetes API did not become ready within five minutes.' >&2
fi
```

### Step 4: Verify persistence

```bash
CM_UID_AFTER=$(kubectl --context docker-desktop -n dd-restart-lab \
  get configmap restart-marker -o jsonpath='{.metadata.uid}')

CM_TOKEN_AFTER=$(kubectl --context docker-desktop -n dd-restart-lab \
  get configmap restart-marker -o jsonpath='{.data.token}')

PVC_UID_AFTER=$(kubectl --context docker-desktop -n confluent \
  get pvc data0-kafka-0 -o jsonpath='{.metadata.uid}')

PV_AFTER=$(kubectl --context docker-desktop -n confluent \
  get pvc data0-kafka-0 -o jsonpath='{.spec.volumeName}')

echo "CM_UID_AFTER=$CM_UID_AFTER"
echo "CM_TOKEN_AFTER=$CM_TOKEN_AFTER"
echo "PVC_UID_AFTER=$PVC_UID_AFTER"
echo "PV_AFTER=$PV_AFTER"

if [ "$CM_UID_BEFORE" = "$CM_UID_AFTER" ] && \
   [ "$CM_TOKEN_BEFORE" = "$CM_TOKEN_AFTER" ] && \
   [ "$PVC_UID_BEFORE" = "$PVC_UID_AFTER" ] && \
   [ "$PV_BEFORE" = "$PV_AFTER" ]; then
  echo 'PASS: API state and Kafka storage identities survived the Desktop restart.'
else
  echo 'FAIL: at least one API/storage identity changed; investigate before claiming persistence.' >&2
fi
```

Expected:

- The ConfigMap UID and token are unchanged.
- The PVC UID and PV name are unchanged.
- `kafka-0` becomes Ready.
- The common-baseline records remain readable.
- The Pod UID may stay the same or change; it is not the persistence criterion for this scenario.

Clean up only the canary:

```bash
kubectl --context docker-desktop delete namespace dd-restart-lab
```

---

## Scenario 4: Delete a KafkaTopic custom resource

### What this tests

Deleting a CFK-managed `KafkaTopic` is a logical Kafka-data deletion. CFK uses the resource finalizer to delete the actual topic from Kafka. The Kafka broker and its PVC remain.

Expected matrix row:

```text
Delete KafkaTopic/persistence-lab -> broker remains -> topic is logically deleted -> records become inaccessible and are lost
```

### Step 1: Establish evidence

Consume the common baseline, then record the PVC identity:

```bash
kubectl -n confluent get pvc data0-kafka-0 \
  -o custom-columns='PVC:.metadata.name,UID:.metadata.uid,PV:.spec.volumeName'
```

### Step 2: Delete only the KafkaTopic resource

```bash
kubectl -n confluent delete kafkatopic persistence-lab
```

Do not remove the resource's finalizer manually. The finalizer is what allows CFK to delete the real Kafka topic before the Kubernetes object disappears.

### Step 3: Verify the outcome

```bash
kubectl -n confluent get pod kafka-0
kubectl -n confluent get pvc data0-kafka-0

TOPIC_GONE=false

for attempt in {1..60}; do
  if ! kubectl -n confluent exec kafka-0 -c kafka -- \
    kafka-topics \
    --bootstrap-server kafka:9092 \
    --describe \
    --topic persistence-lab >/dev/null 2>&1; then
    TOPIC_GONE=true
    break
  fi

  echo 'Waiting for Kafka topic deletion...'
  sleep 2
done

if [ "$TOPIC_GONE" = true ]; then
  echo 'PASS: Kafka no longer describes persistence-lab.'
else
  echo 'STOP: Kafka still describes persistence-lab after two minutes.' >&2
fi
```

Expected:

- `kafka-0` remains Ready.
- The PVC UID and PV remain unchanged.
- Kafka reports that `persistence-lab` does not exist.
- All records previously stored in that topic are gone.

Kafka makes the topic and its records inaccessible before every physical segment file is necessarily removed. File cleanup is asynchronous.

If the custom resource remains in `DELETING`, inspect CFK before taking any emergency action:

```bash
kubectl -n confluent get kafkatopic persistence-lab -o yaml
kubectl -n confluent logs deployment/confluent-operator --since=10m
```

### Step 4: Recreate an empty topic

```bash
kubectl apply -f - <<'EOF'
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: persistence-lab
  namespace: confluent
spec:
  kafkaRestClassRef:
    name: krc-cfk
  replicas: 1
  partitionCount: 4
  configs:
    cleanup.policy: "delete"
EOF
```

Wait for it to become ready. Recreating a topic with the same name creates a new empty topic; it does not recover the deleted records.

---

## Scenario 5: Let Kafka retention expire

### What this tests

Kafka retention deletes old closed log segments while keeping the topic, broker, PVC, and PV. Retention works on segment files, not individual records.

Expected matrix row:

```text
Kafka retention expires -> broker remains -> old segments are deleted -> expired records are lost
```

### Step 1: Create a dedicated short-retention topic

```bash
kubectl apply -f - <<'EOF'
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: retention-lab
  namespace: confluent
spec:
  kafkaRestClassRef:
    name: krc-cfk
  replicas: 1
  partitionCount: 1
  configs:
    cleanup.policy: "delete"
    retention.ms: "15000"
    segment.ms: "5000"
    file.delete.delay.ms: "1000"
EOF
```

Wait until CFK reports the topic ready:

```bash
kubectl -n confluent get kafkatopic retention-lab -w
```

Press `Ctrl+C` after it is ready.

Verify the effective topic configuration:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-configs \
  --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name retention-lab \
  --describe
```

### Step 2: Produce records into the first segment

```bash
printf 'old-1\nold-2\nold-3\n' | \
  kubectl -n confluent exec -i kafka-0 -c kafka -- \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic retention-lab
```

Confirm that they are readable:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic retention-lab \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.offset=true \
  --property print.value=true
```

### Step 3: Force a later append so Kafka can roll the old segment

Wait longer than `segment.ms`:

```bash
sleep 6
```

Then append one marker:

```bash
printf 'new-segment-marker\n' | \
  kubectl -n confluent exec -i kafka-0 -c kafka -- \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic retention-lab
```

The append gives Kafka an opportunity to close the older segment and start a new one.

### Step 4: Wait for asynchronous retention cleanup

`retention.ms=15000` makes a closed segment eligible; it does not promise deletion exactly 15 seconds later. The broker's retention-check interval is commonly several minutes. Allow up to approximately six minutes in this local lab.

Poll the partition's earliest readable offset. The first three records used offsets 0 through 2, so an earliest offset of at least 3 proves that the old segment is no longer readable:

```bash
RETENTION_COMPLETE=false

for attempt in {1..36}; do
  START_OFFSET=$(kubectl -n confluent exec kafka-0 -c kafka -- \
    kafka-get-offsets \
    --bootstrap-server kafka:9092 \
    --topic retention-lab \
    --time -2 | awk -F: '$1 == "retention-lab" && $2 == 0 {print $3}')

  echo "earliest offset=$START_OFFSET"

  if [ -n "$START_OFFSET" ] && [ "$START_OFFSET" -ge 3 ]; then
    RETENTION_COMPLETE=true
    break
  fi

  sleep 10
done

if [ "$RETENTION_COMPLETE" = true ]; then
  echo 'PASS: offsets 0-2 expired.'
else
  echo 'STOP: retention did not advance the earliest offset within six minutes.' >&2
fi
```

Inspect the physical segment directory and then consume again:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  ls -lh /mnt/data/data0/logs/retention-lab-0/

kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic retention-lab \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.offset=true \
  --property print.value=true
```

Expected:

- `old-1`, `old-2`, and `old-3` eventually disappear.
- The topic still exists.
- The Pod, PVC, and PV remain.
- `new-segment-marker` remains in the new active segment. It is not eligible until a later append, after `segment.ms`, rolls that segment too.

Do not confuse `delete.retention.ms` with normal record retention. `delete.retention.ms` applies to tombstone markers for compacted topics; `retention.ms` controls time-based deletion for this `cleanup.policy=delete` experiment.

### Step 5: Clean up

```bash
kubectl -n confluent delete kafkatopic retention-lab
```

---

## Scenario 6: Delete Kafka's PVC

> **STOP: storage-destructive experiment.** This deliberately removes the only broker's only copy of its records. Complete this only on a disposable cluster. With replication factor `1`, no other broker can restore the data.

### What this tests

A PVC in active use is protected from immediate removal. This lab first observes that protection, blocks CFK reconciliation for the Kafka CR, scales the generated StatefulSet to zero, lets the claim deletion complete, and then creates a new empty claim.

This direct StatefulSet manipulation is a controlled failure-injection technique, not a normal CFK administration procedure.

Expected matrix row in this cluster:

```text
Delete data0-kafka-0 -> old PV has Delete policy -> backing storage is deleted -> records are lost
```

### Step 1: Confirm that nothing valuable is present

```bash
kubectl config current-context
kubectl -n confluent get kafka,pod,pvc
kubectl -n confluent get kafkatopic
```

Continue only if this is the disposable `docker-desktop` environment.

### Step 2: Create evidence not managed by a KafkaTopic CR

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic storage-loss-lab \
  --partitions 1 \
  --replication-factor 1

printf 'must-disappear-1\nmust-disappear-2\n' | \
  kubectl -n confluent exec -i kafka-0 -c kafka -- \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic storage-loss-lab
```

Consume the two records before continuing.

### Step 3: Capture the old storage identity and policy

```bash
OLD_PVC_UID=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.metadata.uid}')

OLD_PV=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.spec.volumeName}')

OLD_RECLAIM=$(kubectl get pv "$OLD_PV" \
  -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')

kubectl get pv "$OLD_PV" \
  -o custom-columns='PV:.metadata.name,RECLAIM:.spec.persistentVolumeReclaimPolicy,STATUS:.status.phase'

echo "OLD_PVC_UID=$OLD_PVC_UID"
echo "OLD_PV=$OLD_PV"
echo "OLD_RECLAIM=$OLD_RECLAIM"
```

For the observed local cluster, the reclaim policy is `Delete`. If your output is `Retain`, the PV becomes `Released` and its bytes remain for manual recovery; it will not automatically bind to a newly created claim.

### Step 4: Observe PVC protection while Kafka still uses the claim

First prevent CFK from immediately reconciling the manual StatefulSet scale used later in this experiment:

```bash
kubectl -n confluent annotate kafka kafka \
  platform.confluent.io/block-reconcile=true \
  --overwrite
```

Request PVC deletion without waiting:

```bash
kubectl -n confluent delete pvc data0-kafka-0 --wait=false

kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='deletedAt={.metadata.deletionTimestamp}{" finalizers="}{.metadata.finalizers}{"\n"}'
```

Expected while `kafka-0` still uses it:

- The PVC has a deletion timestamp.
- The `kubernetes.io/pvc-protection` finalizer keeps it in `Terminating`.
- Kafka can temporarily keep using the already mounted filesystem.

### Step 5: Stop Kafka and allow claim deletion to finish

If CFK admission protection is enabled, explicitly allow this one disposable Kafka Pod deletion:

```bash
kubectl -n confluent label kafka kafka \
  confluent-operator.webhooks.platform.confluent.io/allow-kafka-pod-deletion=true \
  --overwrite
```

```bash
kubectl -n confluent scale statefulset kafka --replicas=0

kubectl -n confluent wait \
  --for=delete \
  pod/kafka-0 \
  --timeout=5m

kubectl -n confluent wait \
  --for=delete \
  pvc/data0-kafka-0 \
  --timeout=2m
```

The StatefulSet controller no longer creates a replacement Pod because its temporary desired replica count is zero. Once no Pod uses the claim, PVC protection allows the pending deletion to complete.

Wait for the provisioner only when the recorded reclaim policy was `Delete`:

```bash
if [ "$OLD_RECLAIM" = Delete ]; then
  kubectl wait \
    --for=delete \
    "pv/$OLD_PV" \
    --timeout=5m
else
  kubectl get pv "$OLD_PV"
fi
```

With `Delete`, the dynamically provisioned PV and backing storage should eventually disappear. Deletion is asynchronous and can lag or fail if the provisioner has a problem. With `Retain`, the PV should remain in `Released` state.

If the PVC remains `Terminating`, find the Pod still referencing it:

```bash
kubectl get pods -A -o yaml | grep -B10 -A10 'claimName: data0-kafka-0'
kubectl -n confluent describe pvc data0-kafka-0
```

Do not remove the PVC protection finalizer manually.

### Step 6: Let the StatefulSet provision empty storage

```bash
kubectl -n confluent scale statefulset kafka --replicas=1

kubectl -n confluent annotate kafka kafka \
  platform.confluent.io/block-reconcile-

kubectl -n confluent wait \
  --for=create \
  pod/kafka-0 \
  --timeout=5m

kubectl -n confluent wait \
  --for=condition=Ready \
  pod/kafka-0 \
  --timeout=10m

kubectl -n confluent label kafka kafka \
  confluent-operator.webhooks.platform.confluent.io/allow-kafka-pod-deletion-
```

If an earlier command fails, make sure reconciliation is unblocked:

```bash
kubectl -n confluent annotate kafka kafka \
  platform.confluent.io/block-reconcile-

kubectl -n confluent label kafka kafka \
  confluent-operator.webhooks.platform.confluent.io/allow-kafka-pod-deletion-
```

The readiness wait can time out. A fresh empty broker volume plus surviving KRaft metadata is not a valid single-replica data-recovery path. Depending on the recorded metadata and component versions, Kafka can start with empty or unavailable partitions, or remain unhealthy. All are useful observations for this destructive experiment; none restores the deleted records.

### Step 7: Prove that this is new storage

```bash
kubectl -n confluent get pvc data0-kafka-0 \
  -o custom-columns='PVC:.metadata.name,UID:.metadata.uid,PV:.spec.volumeName,STATUS:.status.phase'
```

Expected:

- PVC name is still `data0-kafka-0`.
- PVC UID differs from `OLD_PVC_UID`.
- PV name differs from `OLD_PV`.
- The old records are no longer readable.

The KRaft controller may still know the `storage-loss-lab` topic metadata, but the only replica's log bytes were on the deleted broker volume. Topic metadata is not a backup of topic records.

If Kafka does not recover cleanly, use the recovery section below rather than modifying volume files manually.

### Attempt to resume reconciliation after Scenario 6

Ensure reconciliation is unblocked, then reapply the source manifest from the repository root:

```bash
kubectl -n confluent annotate kafka kafka \
  platform.confluent.io/block-reconcile-

kubectl apply -f july-13/cp-singlenode.yaml

kubectl -n confluent wait \
  --for=condition=Ready \
  pod/kafka-0 \
  --timeout=10m
```

This is an attempt to resume reconciliation, not a full rebuild: the Kafka CR, generated StatefulSet, replacement PVC, KRaftController, and controller metadata still exist. Reapplying desired state cannot clear a broker-log/controller-metadata mismatch or reconstruct records whose only storage copy was deleted.

Before proceeding to Scenario 7, require a healthy Kafka cluster. If the readiness wait fails, perform the complete disposable-cluster reset and rebuild in Scenario 8. That recreates both broker and KRaft-controller storage. With `Delete` reclaim plus replication factor `1`, there is no Kubernetes or Kafka mechanism that can reconstruct the old records; only an external volume/data backup could do that.

---

## Scenario 7: Delete the Kafka custom resource with a `Delete` PV

> **STOP: deployment-destructive experiment.** This asks CFK to remove the Kafka deployment. In the current cluster, the Kafka PV reclaim policy is `Delete`, so the broker's backing storage is expected to be deleted.

### What this tests

This differs from deleting only the Pod:

- Pod deletion leaves the Kafka CR, StatefulSet, PVC, and PV in place.
- Kafka CR deletion tells CFK that the Kafka deployment itself is no longer desired.
- With PV reclaim policy `Delete`, removal of its claim causes the backing PV storage to be deleted.

### Step 1: Start from a rebuilt healthy cluster

```bash
kubectl -n confluent get kafka kafka
kubectl -n confluent get pod kafka-0
kubectl -n confluent get pvc data0-kafka-0
```

Do not start this scenario immediately after Scenario 6 unless those resources have recovered.

### Step 2: Remove CFK application resources that depend on Kafka

Confluent's documented deployment-deletion order removes application resources before the Kafka component. On this disposable namespace, list them first:

```bash
kubectl -n confluent get kafkatopic,kafkarestclass
```

Delete only the disposable application resources shown in this learning namespace:

```bash
kubectl -n confluent delete kafkatopic --all
kubectl -n confluent delete kafkarestclass --all
```

This avoids leaving KafkaTopic finalizers waiting on a Kafka endpoint that has already disappeared.

### Step 3: Create direct Kafka evidence

This test topic is deliberately created with the Kafka CLI, so there is no KafkaTopic custom resource to delete it during the previous step:

```bash
kubectl -n confluent exec kafka-0 -c kafka -- \
  kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic kafka-cr-delete-lab \
  --partitions 1 \
  --replication-factor 1

printf 'cr-delete-data-1\ncr-delete-data-2\n' | \
  kubectl -n confluent exec -i kafka-0 -c kafka -- \
  kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic kafka-cr-delete-lab
```

Consume the two records before continuing.

### Step 4: Capture the old PVC and PV

```bash
OLD_PVC_UID=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.metadata.uid}')

OLD_PV=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.spec.volumeName}')

OLD_RECLAIM=$(kubectl get pv "$OLD_PV" \
  -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')

kubectl get pv "$OLD_PV" \
  -o custom-columns='PV:.metadata.name,RECLAIM:.spec.persistentVolumeReclaimPolicy,STATUS:.status.phase'

echo "OLD_PVC_UID=$OLD_PVC_UID"
echo "OLD_PV=$OLD_PV"
echo "OLD_RECLAIM=$OLD_RECLAIM"
```

Continue only if you intend to test the displayed reclaim policy.

### Step 5: Permit the destructive deletion if CFK protection is enabled

CFK's deletion-protection webhook may reject deleting a component whose PV uses `Delete`. Allow it only for this disposable experiment:

```bash
kubectl -n confluent label kafka kafka \
  confluent-operator.webhooks.platform.confluent.io/allow-pv-deletion=true \
  --overwrite
```

### Step 6: Delete the desired Kafka deployment

In another terminal, watch the resources:

```bash
kubectl -n confluent get kafka,pod,pvc -w
```

Then request deletion:

```bash
kubectl -n confluent delete kafka kafka
```

Do not use `--force`.

The Kafka CR can disappear before every asynchronous Pod, claim, or provisioner cleanup operation has completed. Wait for each expected deletion:

```bash
kubectl -n confluent wait \
  --for=delete \
  pod/kafka-0 \
  --timeout=5m

kubectl -n confluent wait \
  --for=delete \
  pvc/data0-kafka-0 \
  --timeout=5m

if [ "$OLD_RECLAIM" = Delete ]; then
  kubectl wait \
    --for=delete \
    "pv/$OLD_PV" \
    --timeout=5m
else
  kubectl get pv "$OLD_PV"
fi
```

With this cluster's `Delete` policy, the Kafka CR, Pod, old PVC, and old PV should eventually be absent. A timeout indicates incomplete CFK or storage-provisioner cleanup and must be investigated before continuing.

If the PV policy had been `Retain`, the claim could be removed while the old PV changes to `Released`. A newly created claim does not automatically reuse a `Released` PV.

### Step 7: Observe recreation; do not treat it as data recovery

From the repository root:

```bash
kubectl apply -f july-13/cp-singlenode.yaml

kubectl -n confluent wait \
  --for=create \
  pod/kafka-0 \
  --timeout=5m

kubectl -n confluent get pvc data0-kafka-0 \
  -o custom-columns='PVC:.metadata.name,UID:.metadata.uid,PV:.spec.volumeName,STATUS:.status.phase'
```

Now observe whether Kafka can become ready:

```bash
if kubectl -n confluent wait \
  --for=condition=Ready \
  pod/kafka-0 \
  --timeout=10m; then
  echo 'Kafka became Ready; checking the test topic.'

  if kubectl -n confluent exec kafka-0 -c kafka -- \
    kafka-topics \
    --bootstrap-server kafka:9092 \
    --describe \
    --topic kafka-cr-delete-lab; then
    kubectl -n confluent exec kafka-0 -c kafka -- \
      kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic kafka-cr-delete-lab \
      --from-beginning \
      --timeout-ms 10000 \
      --property print.offset=true \
      --property print.value=true
  else
    echo 'The old test topic is not available.'
  fi
else
  echo 'Kafka did not become Ready; surviving KRaft metadata and the fresh broker disk did not form a healthy recovery.' >&2
fi
```

Possible observations:

- A new PVC UID and PV name are provisioned.
- The source manifest recreates the declared Kafka resources.
- `kafka-cr-delete-lab` is missing, empty, unavailable, or its partition is offline.
- Kafka can remain unhealthy because its controller metadata survived while its only broker log copy did not.
- Reapplying YAML restores desired configuration, not deleted application data.

If the consumer runs, verify that `cr-delete-data-1` and `cr-delete-data-2` are absent. If Kafka never becomes Ready, no record-level consumer assertion is possible; the failed recovery itself is the result.

The KRaft controller was not deleted in this experiment. It may retain topic metadata even though the broker's only record copy is gone. Reapplying only the Kafka CR is therefore an observation step, not a complete KRaft disaster-recovery procedure. Use Scenario 8 to produce a known-clean disposable cluster before further labs.

---

## Scenario 8: Reset the Docker Desktop Kubernetes cluster

> **STOP: cluster-destructive experiment.** Reset deletes all Kubernetes resources in this Docker Desktop cluster, not only resources in `confluent`. Namespace isolation cannot protect anything from a cluster reset.

### What this tests

Expected matrix row:

```text
Reset Docker Desktop Kubernetes -> old Kubernetes resources, PVCs, and PVs are removed -> replacement cluster cannot access the old Kafka storage -> treat records as lost
```

There is no harmless dry run. Run this last.

### Step 1: Verify and inventory the cluster

Fail closed on the target context. Do not continue unless this prints `PASS`:

```bash
CURRENT_CONTEXT=$(kubectl config current-context)

if [ "$CURRENT_CONTEXT" = docker-desktop ]; then
  echo 'PASS: disposable docker-desktop context selected.'
else
  echo "STOP: refusing reset preparation for context $CURRENT_CONTEXT" >&2
fi
```

Record the cluster, CFK application resources, storage identity, and installed Helm release:

```bash
kubectl get namespaces
kubectl get all -A
kubectl get kafka,kraftcontroller,kafkatopic,kafkarestclass -A
kubectl get pvc -A
kubectl get pv
kubectl api-resources --api-group=platform.confluent.io

OLD_PVC_UID=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.metadata.uid}')

OLD_PV=$(kubectl -n confluent get pvc data0-kafka-0 \
  -o jsonpath='{.spec.volumeName}')

OLD_PV_UID=$(kubectl get pv "$OLD_PV" \
  -o jsonpath='{.metadata.uid}')

echo "OLD_PVC_UID=$OLD_PVC_UID"
echo "OLD_PV=$OLD_PV"
echo "OLD_PV_UID=$OLD_PV_UID"

helm list -n confluent
helm get values confluent-operator -n confluent -o yaml
```

Keep this external terminal open so the old identity variables remain available after reset. Save the original Helm chart version and any user-supplied values outside Kubernetes before continuing.

The read-only inspection performed while this guide was written found:

```text
Chart:        confluent-for-kubernetes-0.1514.40
App version:  3.2.2
User values:  null
```

If the release has changed, use the newly observed version and values during rebuild instead of these recorded values.

There is also a version-hygiene issue to resolve before relying on a clean rebuild: the current `july-13/cp-singlenode.yaml` pins `confluent-init-container:2.11.1`, while the installed CFK application version is `3.2.2`. Confluent requires the init-container version to match the operator version. This persistence guide does not silently alter that deployment manifest.

Manifests and Helm values should be saved in source control. `kubectl get all` is not a complete backup, and Git-tracked YAML does not contain Kafka record data.

### Step 2: Create a reset canary

```bash
kubectl create namespace cluster-reset-lab \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n cluster-reset-lab create configmap reset-marker \
  --from-literal=token="$(date -u +%Y%m%dT%H%M%SZ)" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n cluster-reset-lab get configmap reset-marker \
  -o custom-columns='UID:.metadata.uid,TOKEN:.data.token'

RESET_CM_UID_BEFORE=$(kubectl -n cluster-reset-lab \
  get configmap reset-marker -o jsonpath='{.metadata.uid}')

RESET_CM_TOKEN_BEFORE=$(kubectl -n cluster-reset-lab \
  get configmap reset-marker -o jsonpath='{.data.token}')

echo "RESET_CM_UID_BEFORE=$RESET_CM_UID_BEFORE"
echo "RESET_CM_TOKEN_BEFORE=$RESET_CM_TOKEN_BEFORE"
```

### Step 3: Reset from Docker Desktop

Use the Docker Desktop UI:

```text
Docker Desktop -> Troubleshoot -> Reset Kubernetes cluster
```

Depending on the Desktop version, the reset control can also be under Kubernetes settings. Read the confirmation carefully: it must say that Kubernetes resources will be deleted.

Do not use `Clean / Purge data` or `Reset to factory defaults`; those operations have a broader blast radius than this lab.

### Step 4: Wait for the replacement cluster

After Docker Desktop reports Kubernetes running:

```bash
kubectl config use-context docker-desktop

API_READY=false

for attempt in {1..60}; do
  if kubectl --context docker-desktop get --raw='/readyz' >/dev/null 2>&1; then
    API_READY=true
    break
  fi

  echo 'Waiting for the new Kubernetes API...'
  sleep 5
done

if [ "$API_READY" = true ]; then
  kubectl --context docker-desktop wait \
    --for=condition=Ready \
    nodes \
    --all \
    --timeout=5m
else
  echo 'STOP: the replacement Kubernetes API did not become ready within five minutes.' >&2
fi
```

### Step 5: Prove the old cluster state is gone

```bash
if kubectl --context docker-desktop get namespace cluster-reset-lab >/dev/null 2>&1; then
  echo 'FAIL: old reset canary namespace still exists.' >&2
else
  echo 'PASS: old reset canary namespace is absent.'
fi

if kubectl --context docker-desktop get namespace confluent >/dev/null 2>&1; then
  echo 'FAIL: old confluent namespace still exists.' >&2
else
  echo 'PASS: old confluent namespace is absent.'
fi

if kubectl --context docker-desktop get crd \
  kafkas.platform.confluent.io >/dev/null 2>&1; then
  echo 'FAIL: old CFK Kafka CRD still exists.' >&2
else
  echo 'PASS: old CFK Kafka CRD is absent.'
fi

if helm status confluent-operator \
  --kube-context docker-desktop \
  -n confluent >/dev/null 2>&1; then
  echo 'FAIL: old CFK Helm release still exists.' >&2
else
  echo 'PASS: old CFK Helm release is absent.'
fi

if kubectl --context docker-desktop get pv "$OLD_PV" >/dev/null 2>&1; then
  echo 'FAIL: the old Kafka PV object still exists.' >&2
else
  echo 'PASS: the old Kafka PV object is absent.'
fi

kubectl --context docker-desktop get pvc -A
kubectl --context docker-desktop get pv
```

Expected:

- `cluster-reset-lab` is not found.
- `confluent` is not found.
- The old CFK Helm release, CRDs, custom resources, Pods, PVC objects, and PV objects are absent.
- The replacement cluster has no usable path to the old Kafka storage, so the old records are not recoverable through Kubernetes.
- The `docker-desktop` kubeconfig context can still exist or be recreated; context presence does not prove the old cluster survived.

Docker documents deletion of Kubernetes stacks and resources. It does not promise forensic physical erasure of every byte in Docker Desktop's backing virtual disk, so this lab proves loss of the Kubernetes objects and usable storage path—not secure media sanitization.

### Step 6: Rebuild the learning environment

Reinstall CFK using the exact chart version and Helm values recorded before reset, then reapply the source manifest. For the release observed while writing this guide, the pinned outline is:

```bash
helm repo add confluentinc https://packages.confluent.io/helm --force-update
helm repo update

helm upgrade --install confluent-operator \
  confluentinc/confluent-for-kubernetes \
  --version 0.1514.40 \
  --namespace confluent \
  --create-namespace

kubectl -n confluent rollout status \
  deployment/confluent-operator \
  --timeout=5m

kubectl wait \
  --for=condition=Established \
  crd/kafkas.platform.confluent.io \
  --timeout=5m

kubectl apply -f july-13/cp-singlenode.yaml

kubectl -n confluent wait \
  --for=condition=platform.confluent.io/cluster-ready \
  kraftcontroller/kraftcontroller \
  --timeout=15m

kubectl -n confluent wait \
  --for=condition=platform.confluent.io/cluster-ready \
  kafka/kafka \
  --timeout=15m
```

If Step 1 recorded a different chart version or non-null user values, replace `0.1514.40` and add the original values file or `--set` options. Do not silently install the latest defaults.

Reinstalling CFK and reapplying manifests creates a new deployment. `persistence-lab` is a new empty topic and must receive new test records; YAML cannot restore records from the old cluster storage.

## Troubleshooting fingerprints

| Symptom | Likely layer | First checks |
|---|---|---|
| `RESTARTS` increases but Pod UID stays the same | Container-process termination/restart | `kubectl logs kafka-0 -c kafka --previous` |
| Pod UID changes but PVC/PV stay the same | Pod replacement | StatefulSet events and Pod ownership |
| PVC remains `Terminating` | PVC protection; a Pod still references it | `kubectl describe pvc`; find referencing Pods |
| PV becomes `Released` | Reclaim policy `Retain` | Preserve it; do not expect automatic rebinding |
| PV disappears after PVC deletion | Reclaim policy `Delete` | Provisioner events; storage is not recoverable through that PV |
| KafkaTopic remains `DELETING` | CFK finalizer cannot reach Kafka | KafkaTopic YAML and operator logs |
| Pod is `Pending` after storage recreation | Claim provisioning, binding, or node affinity | Pod/PVC events and StorageClass |
| Kafka is `Running 0/1` | Process started but readiness has not passed | Kafka logs and readiness events |
| API returns `connection refused` during Desktop restart | API server is temporarily offline | Wait for `/readyz`; do not reset the cluster |

Useful commands:

```bash
kubectl -n confluent describe pod kafka-0
kubectl -n confluent describe pvc data0-kafka-0
kubectl -n confluent get events --sort-by=.metadata.creationTimestamp
kubectl -n confluent logs kafka-0 -c kafka --tail=200
kubectl -n confluent logs kafka-0 -c config-init-container
kubectl -n confluent logs deployment/confluent-operator --since=10m
```

## Final outcome checklist

| Matrix operation | Identity evidence | Data evidence |
|---|---|---|
| Normal Pod deletion | New Pod UID; same PVC UID/PV | Records remain |
| Kafka process termination | Same Pod UID; restart count increases; same PVC/PV | Records remain |
| Docker Desktop restart | Kubernetes canary and PVC/PV remain | Records remain |
| KafkaTopic deletion | Broker and PVC/PV remain | Topic and its records disappear |
| Retention expiry | Topic and PVC/PV remain | Only expired closed segments disappear |
| PVC deletion with `Delete` PV | New PVC UID/PV after recovery | Old records disappear |
| Kafka CR deletion with `Delete` PV | Kafka, old PVC, and old PV disappear | Reapply creates configuration, not old records |
| Kubernetes reset | Old canary, namespace, CRDs, PVCs, and PVs disappear | Old cluster records disappear |

## References

- [Kubernetes Persistent Volumes, PVC protection, and reclaim policies](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
- [Kubernetes StatefulSets and PVC retention](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
- [Change a PersistentVolume reclaim policy](https://kubernetes.io/docs/tasks/administer-cluster/change-pv-reclaim-policy/)
- [Confluent for Kubernetes storage configuration](https://docs.confluent.io/operator/current/co-storage.html)
- [Confluent for Kubernetes topic management](https://docs.confluent.io/operator/current/co-manage-topics.html)
- [Confluent for Kubernetes deployment deletion order](https://docs.confluent.io/operator/current/co-delete-deployment.html)
- [Confluent for Kubernetes deletion-protection webhooks](https://docs.confluent.io/operator/current/co-deploy-cfk.html)
- [Confluent for Kubernetes reconciliation controls and troubleshooting](https://docs.confluent.io/operator/current/co-troubleshooting.html)
- [Confluent for Kubernetes planning and image-version requirements](https://docs.confluent.io/operator/current/co-plan.html)
- [Confluent Platform topic configuration reference](https://docs.confluent.io/platform/7.9/installation/configuration/topic-configs.html)
- [Docker Desktop Kubernetes](https://docs.docker.com/desktop/use-desktop/kubernetes/)
- [Docker Desktop troubleshooting and reset behavior](https://docs.docker.com/desktop/troubleshoot-and-support/troubleshoot/)
- [Docker Desktop CLI](https://docs.docker.com/desktop/features/desktop-cli/)
- [Docker Desktop release notes](https://docs.docker.com/desktop/release-notes/)
