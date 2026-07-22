# CFK SASL/PLAIN authentication validation runbook

This runbook validates the SASL/PLAIN authentication paths in the `prod` namespace. It covers successful authentication, deliberate credential rejection,
client/server Secret mismatches, and Kubernetes/CFK Secret-update propagation.

The tests use a disposable Kafka client Pod. This opens a new SASL connection for every test and avoids treating an already-established component connection as
proof that the current credentials are valid.

## Security scope

The current manifests use:

```text
SASL mechanism:  PLAIN
Wire protocol:   SASL_PLAINTEXT
TLS encryption:  Disabled
Authorization:   Not configured by these tests
```

SASL/PLAIN provides username/password authentication. `SASL_PLAINTEXT` does not encrypt those credentials in transit. Use this setup only in an isolated lab or
pre-production network. A production deployment should normally use TLS, making the protocol `SASL_SSL`.

Authentication and authorization are separate:

- A successful test proves that the listener accepted the identity.
- It does not prove that the identity has the required ACL or RBAC permissions.

## Related configuration

- [Security materialization](../02-security/README.md)
- [Kafka](../03-components/kafka.yaml)
- [KRaft controller](../03-components/kraftcontroller.yaml)
- [Connect](../03-components/connect.yaml)
- [Schema Registry](../03-components/schemaregistry.yaml)

## Authentication paths under test

| Scenario | Client credential | Server user directory | Endpoint |
|---|---|---|---|
| Connect to Kafka | `connect-kafka-sasl-client/plain.txt` | `kafka-sasl-server/plain-users.json` | `kafka.prod.svc.cluster.local:9071` |
| Schema Registry to Kafka | `sr-kafka-sasl-client/plain.txt` | `kafka-sasl-server/plain-users.json` | `kafka.prod.svc.cluster.local:9071` |
| Kafka broker to Kafka broker | `kafka-sasl-server/plain-interbroker.txt` | `kafka-sasl-server/plain-users.json` | `kafka-0.kafka.prod.svc.cluster.local:9072` |
| Kafka broker to KRaft | `kafka-kraft-sasl-client/plain-interbroker.txt` | `kraft-sasl-server/plain-users.json` | `kraftcontroller.prod.svc.cluster.local:9074` |
| KRaft controller to KRaft controller | `kraft-sasl-server/plain-interbroker.txt` | `kraft-sasl-server/plain-users.json` | `kraftcontroller.prod.svc.cluster.local:9074` |

For every row, the username in the client file must exist in the corresponding
server `plain-users.json`, and both sides must contain the same password.

## Test result rules

Use these rules to avoid false positives:

1. Run the positive test first. It proves that DNS, networking, the endpoint, and the real credential are working.
2. Run the negative test against the same endpoint. It changes only the password.
3. A negative test passes only when the output explicitly identifies a SASL authentication failure.
4. A timeout, DNS failure, unavailable broker, or connection refusal is **inconclusive**. It does not prove that Kafka rejected the credential.
5. `Running` or `Ready` alone is not proof of a fresh SASL authentication.

The negative helpers change only the password held by the disposable client. From the server's perspective, this is the same SASL mismatch as a wrong value
in `plain-users.json`, but it avoids damaging a live broker or controller Secret. Deliberately corrupt shared inter-broker or controller credentials only
in a disposable cluster.

## 1. Preflight checks

Confirm that the CFK resources and Pods are healthy:

```bash
kubectl -n prod get kafka,kraftcontroller,connect,schemaregistry
kubectl -n prod get pods -o wide
```

For this three-replica deployment, the expected component Pods are:

```text
kafka-0, kafka-1, kafka-2
kraftcontroller-0, kraftcontroller-1, kraftcontroller-2
connect-0
schemaregistry-0
```

List Secret key names without decoding their values:

```bash
for secret in \
  kafka-sasl-server \
  kraft-sasl-server \
  kafka-kraft-sasl-client \
  connect-kafka-sasl-client \
  sr-kafka-sasl-client
do
  printf '\n%s\n' "$secret"
  kubectl -n prod get secret "$secret" \
    -o go-template='{{range $key,$value := .data}}{{printf "%s\n" $key}}{{end}}'
done
```

Expected shapes:

| Secret | Required keys |
|---|---|
| `kafka-sasl-server` | `plain-users.json`, `plain-interbroker.txt` |
| `kraft-sasl-server` | `plain-users.json`, `plain-interbroker.txt` |
| `kafka-kraft-sasl-client` | `plain-users.json`, `plain-interbroker.txt` |
| `connect-kafka-sasl-client` | `plain.txt` |
| `sr-kafka-sasl-client` | `plain.txt` |

Do not use `kubectl get secret -o yaml` in screenshots, tickets, or committed
documentation. Secret `data` is base64-encoded, not encrypted.

## 2. Define the disposable client helper

Run the following function in the terminal that will execute the tests. It
creates one temporary Pod and mounts only the Secret required by the current
scenario.

```bash
create_auth_test_pod() {
  local secret_name="$1"

  kubectl -n prod delete pod sasl-auth-check \
    --ignore-not-found \
    --wait=true

  kubectl -n prod apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: sasl-auth-check
  labels:
    app: sasl-auth-check
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  terminationGracePeriodSeconds: 0
  containers:
    - name: client
      image: docker.io/confluentinc/cp-server:7.9.8
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: credentials
          mountPath: /mnt/credentials
          readOnly: true
  volumes:
    - name: credentials
      secret:
        secretName: ${secret_name}
EOF

  kubectl -n prod wait \
    --for=condition=Ready \
    pod/sasl-auth-check \
    --timeout=5m
}
```

The test Pod does not mount a Kubernetes ServiceAccount token and is deleted
before a different credential Secret is mounted.

## 3. Define the Kafka-listener test helper

This function supports both positive and negative tests against Kafka's internal
or replication listener. Temporary client properties are mode `0600` and are
deleted when the command exits.

```bash
test_kafka_listener() {
  local credential_key="$1"
  local bootstrap_server="$2"
  local expectation="$3"

  kubectl -n prod exec sasl-auth-check -c client -- \
    env \
      CREDENTIAL_FILE="/mnt/credentials/${credential_key}" \
      BOOTSTRAP_SERVER="${bootstrap_server}" \
      EXPECTATION="${expectation}" \
    sh -ec '
      umask 077

      USERNAME=$(sed -n "s/^username=//p" "$CREDENTIAL_FILE")
      PASSWORD=$(sed -n "s/^password=//p" "$CREDENTIAL_FILE")

      test -n "$USERNAME"
      test -n "$PASSWORD"

      if [ "$EXPECTATION" = "rejected" ]; then
        PASSWORD="intentionally-invalid-password"
      fi

      ESCAPED_USERNAME=$(printf "%s" "$USERNAME" |
        sed "s/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g")
      ESCAPED_PASSWORD=$(printf "%s" "$PASSWORD" |
        sed "s/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g")

      CLIENT_CONFIG=$(mktemp)
      trap "rm -f $CLIENT_CONFIG" EXIT HUP INT TERM

      printf "%s\n" \
        "security.protocol=SASL_PLAINTEXT" \
        "sasl.mechanism=PLAIN" \
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ESCAPED_USERNAME\" password=\"$ESCAPED_PASSWORD\";" \
        > "$CLIENT_CONFIG"

      set +e
      OUTPUT=$(kafka-broker-api-versions \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" 2>&1)
      RESULT=$?
      set -e

      if [ "$EXPECTATION" = "accepted" ]; then
        if [ "$RESULT" -eq 0 ]; then
          echo "PASS: Kafka accepted the credential"
        else
          echo "FAIL: the valid credential was not accepted"
          printf "%s\n" "$OUTPUT" | tail -n 25
          exit 1
        fi
      else
        if [ "$RESULT" -eq 0 ]; then
          echo "FAIL: Kafka accepted an invalid password"
          exit 1
        elif printf "%s\n" "$OUTPUT" | grep -Eiq \
          "saslauthenticationexception|authentication failed|failed authentication|invalid username|invalid credentials|invalid password"; then
          echo "PASS: Kafka explicitly rejected the invalid password"
        else
          echo "INCONCLUSIVE: the command failed, but not with an authentication error"
          printf "%s\n" "$OUTPUT" | tail -n 25
          exit 2
        fi
      fi
    '
}
```

Valid values for the third argument are `accepted` and `rejected`.

## 4. Connect to Kafka tests

The current Connect CR references `connect-kafka-sasl-client` and Kafka's
internal listener on port `9071`.

### Positive test

```bash
create_auth_test_pod connect-kafka-sasl-client

test_kafka_listener \
  plain.txt \
  kafka.prod.svc.cluster.local:9071 \
  accepted
```

Expected result:

```text
PASS: Kafka accepted the credential
```

Confirm the real Connect worker is also healthy:

```bash
kubectl -n prod exec connect-0 -c connect -- \
  curl -fsS http://127.0.0.1:8083/
```

The response should contain Connect version information and a non-empty
`kafka_cluster_id`.

### Negative test

```bash
test_kafka_listener \
  plain.txt \
  kafka.prod.svc.cluster.local:9071 \
  rejected
```

Expected result:

```text
PASS: Kafka explicitly rejected the invalid password
```

## 5. Schema Registry to Kafka tests

The current Schema Registry CR references `sr-kafka-sasl-client` and Kafka's
internal listener on port `9071`.

### Positive test

```bash
create_auth_test_pod sr-kafka-sasl-client

test_kafka_listener \
  plain.txt \
  kafka.prod.svc.cluster.local:9071 \
  accepted
```

Confirm the real Schema Registry process is healthy:

```bash
kubectl -n prod exec schemaregistry-0 -c schemaregistry -- \
  curl -fsS http://127.0.0.1:8081/subjects
```

An empty registry returns `[]`, which is healthy.

### Negative test

```bash
test_kafka_listener \
  plain.txt \
  kafka.prod.svc.cluster.local:9071 \
  rejected
```

## 6. Kafka broker-to-broker authentication tests

Kafka brokers present the credential from
`kafka-sasl-server/plain-interbroker.txt` to the replication listener. The
server-side accepted-user map is in the same Secret under `plain-users.json`.

### Positive test

```bash
create_auth_test_pod kafka-sasl-server

test_kafka_listener \
  plain-interbroker.txt \
  kafka-0.kafka.prod.svc.cluster.local:9072 \
  accepted
```

### Negative test

```bash
test_kafka_listener \
  plain-interbroker.txt \
  kafka-0.kafka.prod.svc.cluster.local:9072 \
  rejected
```

This validates credential acceptance on the replication listener. Also check
the actual cluster state because a disposable client is not a broker replica:

```bash
kubectl -n prod get kafka kafka
kubectl -n prod get pods -l app=kafka
```

All three brokers should be ready before treating broker-to-broker communication
as healthy.

## 7. Define the KRaft-controller test helper

The controller listener requires `kafka-metadata-quorum`, not
`kafka-broker-api-versions`.

```bash
test_controller_listener() {
  local credential_key="$1"
  local controller_endpoint="$2"
  local expectation="$3"

  kubectl -n prod exec sasl-auth-check -c client -- \
    env \
      CREDENTIAL_FILE="/mnt/credentials/${credential_key}" \
      CONTROLLER_ENDPOINT="${controller_endpoint}" \
      EXPECTATION="${expectation}" \
    sh -ec '
      umask 077

      USERNAME=$(sed -n "s/^username=//p" "$CREDENTIAL_FILE")
      PASSWORD=$(sed -n "s/^password=//p" "$CREDENTIAL_FILE")

      test -n "$USERNAME"
      test -n "$PASSWORD"

      if [ "$EXPECTATION" = "rejected" ]; then
        PASSWORD="intentionally-invalid-password"
      fi

      ESCAPED_USERNAME=$(printf "%s" "$USERNAME" |
        sed "s/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g")
      ESCAPED_PASSWORD=$(printf "%s" "$PASSWORD" |
        sed "s/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g")

      CLIENT_CONFIG=$(mktemp)
      trap "rm -f $CLIENT_CONFIG" EXIT HUP INT TERM

      printf "%s\n" \
        "security.protocol=SASL_PLAINTEXT" \
        "sasl.mechanism=PLAIN" \
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ESCAPED_USERNAME\" password=\"$ESCAPED_PASSWORD\";" \
        > "$CLIENT_CONFIG"

      set +e
      OUTPUT=$(kafka-metadata-quorum \
        --command-config "$CLIENT_CONFIG" \
        --bootstrap-controller "$CONTROLLER_ENDPOINT" \
        describe --status 2>&1)
      RESULT=$?
      set -e

      if [ "$EXPECTATION" = "accepted" ]; then
        if [ "$RESULT" -eq 0 ]; then
          echo "PASS: KRaft accepted the credential"
          printf "%s\n" "$OUTPUT"
        else
          echo "FAIL: the valid credential was not accepted"
          printf "%s\n" "$OUTPUT" | tail -n 25
          exit 1
        fi
      else
        if [ "$RESULT" -eq 0 ]; then
          echo "FAIL: KRaft accepted an invalid password"
          exit 1
        elif printf "%s\n" "$OUTPUT" | grep -Eiq \
          "saslauthenticationexception|authentication failed|failed authentication|invalid username|invalid credentials|invalid password"; then
          echo "PASS: KRaft explicitly rejected the invalid password"
        else
          echo "INCONCLUSIVE: the command failed, but not with an authentication error"
          printf "%s\n" "$OUTPUT" | tail -n 25
          exit 2
        fi
      fi
    '
}
```

## 8. Kafka broker-to-KRaft authentication tests

The Kafka dependency uses
`kafka-kraft-sasl-client/plain-interbroker.txt` when brokers connect to the
controller listener.

### Positive test

```bash
create_auth_test_pod kafka-kraft-sasl-client

test_controller_listener \
  plain-interbroker.txt \
  kraftcontroller.prod.svc.cluster.local:9074 \
  accepted
```

The successful output includes quorum information such as the cluster ID,
leader ID, current voters, and high watermark.

### Negative test

```bash
test_controller_listener \
  plain-interbroker.txt \
  kraftcontroller.prod.svc.cluster.local:9074 \
  rejected
```

## 9. KRaft controller-to-controller authentication tests

KRaft controllers use `kraft-sasl-server/plain-interbroker.txt` for quorum
communication, while `plain-users.json` provides the accepted-user map.

### Positive test

```bash
create_auth_test_pod kraft-sasl-server

test_controller_listener \
  plain-interbroker.txt \
  kraftcontroller.prod.svc.cluster.local:9074 \
  accepted
```

### Negative test

```bash
test_controller_listener \
  plain-interbroker.txt \
  kraftcontroller.prod.svc.cluster.local:9074 \
  rejected
```

Also verify the real quorum Pods:

```bash
kubectl -n prod get kraftcontroller kraftcontroller
kubectl -n prod get pods -l app=kraftcontroller
```

## 10. Detect a client/server Secret mismatch

Kubernetes and CFK validate Secret references and expected key names, but they do
not compare a client's password with the corresponding entry in the server's
`plain-users.json`. A syntactically valid mismatch is discovered during a SASL
handshake.

There are two different failure classes:

| Failure class | Example | Where it is normally detected |
|---|---|---|
| Structural | Required key missing, wrong Secret reference, invalid file shape | CFK reconciliation status or operator logs |
| Semantic | Correct key names, but client and server passwords differ | Runtime SASL handshake and component logs |

Inspect CFK conditions for a structural error:

```bash
kubectl -n prod get kafka kafka \
  -o jsonpath='{range .status.conditions[*]}{.type}{"="}{.status}{": "}{.message}{"\n"}{end}'
```

Use two checks:

1. Compare the desired Secret objects without printing their values.
2. Open a fresh SASL connection using the positive test.

The following optional macOS check compares the Connect client password with the
Kafka server entry. It requires `jq` and keeps decoded values only in a temporary
subshell:

```bash
(
  set +x

  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required for this optional static comparison"
    exit 2
  fi

  CLIENT_DATA=$(
    kubectl -n prod get secret connect-kafka-sasl-client \
      -o jsonpath='{.data.plain\.txt}' | base64 -D
  )

  CLIENT_USER=$(printf "%s\n" "$CLIENT_DATA" |
    sed -n 's/^username=//p')
  CLIENT_PASSWORD=$(printf "%s\n" "$CLIENT_DATA" |
    sed -n 's/^password=//p')

  SERVER_PASSWORD=$(
    kubectl -n prod get secret kafka-sasl-server \
      -o jsonpath='{.data.plain-users\.json}' |
      base64 -D |
      jq -r --arg user "$CLIENT_USER" '.[$user] // empty'
  )

  if [ -z "$CLIENT_USER" ] || [ -z "$CLIENT_PASSWORD" ]; then
    echo "FAIL: Connect plain.txt is missing a username or password"
  elif [ -z "$SERVER_PASSWORD" ]; then
    echo "FAIL: the Connect username is absent from Kafka plain-users.json"
  elif [ "$CLIENT_PASSWORD" = "$SERVER_PASSWORD" ]; then
    echo "PASS: the client and server Secret objects match"
  else
    echo "FAIL: the client and server passwords do not match"
  fi
)
```

On GNU/Linux, replace `base64 -D` with `base64 -d`.

The static comparison proves only what is stored in the Kubernetes API. Repeat
the positive Connect test after Kafka has loaded the server Secret to prove the
runtime state.

## 11. Observe Secret updates and CFK reconciliation

Before and after an in-place server Secret update, inspect its identity and
resource version:

```bash
kubectl -n prod get secret kafka-sasl-server \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESOURCE_VERSION:.metadata.resourceVersion'
```

For an in-place update:

- The Secret UID remains the same.
- The Secret `resourceVersion` changes.

Watch the API object in a separate terminal:

```bash
kubectl -n prod get secret kafka-sasl-server -w \
  -o custom-columns='NAME:.metadata.name,RESOURCE_VERSION:.metadata.resourceVersion'
```

CFK 3.2 and later tracks referenced Kafka/KRaft Secrets. Confirm that the Kafka
CR tracks the new version:

```bash
kubectl -n prod get kafka kafka \
  -o jsonpath='{.metadata.annotations.platform\.confluent\.io/tracked-secrets-versions}{"\n"}'
```

Observe any CFK-controlled broker roll:

```bash
kubectl -n prod get pods -l app=kafka \
  -L controller-revision-hash -w
```

Important behavior:

- Kubernetes knows that the Secret bytes changed; it does not understand whether
  two passwords match.
- CFK knows that a referenced Secret's `resourceVersion` changed; it does not
  authenticate on the component's behalf.
- Kafka's internal-listener credentials used by Confluent components are outside
  the server-side external-user hot-reload scope. Allow the CFK-controlled roll
  to complete before evaluating the new server credential.
- If a client `plain.txt` changes, restart the dependent client component so its
  JVM loads the new credential.
- When updating a listener Secret, preserve both `plain-users.json` and
  `plain-interbroker.txt`. Accidentally replacing the Secret with only one key
  can break internal authentication.

For Connect client-credential changes:

```bash
kubectl -n prod rollout restart statefulset/connect
kubectl -n prod rollout status statefulset/connect --timeout=10m
```

For Schema Registry client-credential changes:

```bash
kubectl -n prod rollout restart statefulset/schemaregistry
kubectl -n prod rollout status statefulset/schemaregistry --timeout=10m
```

Do not directly run `kubectl rollout restart statefulset/kafka`. Kafka restarts
must use CFK's controlled rolling behavior and health checks.

For a low-risk credential rotation, add a new username to the server directory
while the old username remains valid, wait for server reconciliation, switch and
restart the client, verify a fresh connection, and only then remove the old
username. A JSON user map cannot retain two passwords for one username.

## 12. Logs and troubleshooting

Connect authentication evidence:

```bash
kubectl -n prod logs connect-0 -c connect --since=30m |
  grep -Ei 'sasl|authentication|invalid username|failed'
```

Kafka listener evidence across all brokers:

```bash
kubectl -n prod logs -l app=kafka -c kafka --since=30m --prefix |
  grep -Ei 'sasl|authentication failed|failed authentication'
```

KRaft listener evidence across all controllers:

```bash
kubectl -n prod logs -l app=kraftcontroller -c kraftcontroller \
  --since=30m --prefix |
  grep -Ei 'sasl|authentication failed|failed authentication'
```

If `grep` prints nothing, no matching lines were found in that time window. It
does not itself prove success; use the active positive test.

| Observation | Interpretation | Next check |
|---|---|---|
| Positive test returns `SaslAuthenticationException` | Username is missing, password differs, or the wrong Secret is referenced | Compare CR `secretRef`, Secret keys, and client/server entries |
| Negative test reports an explicit authentication failure | Expected result; the listener enforces SASL/PLAIN | Record the test as passed |
| Negative test succeeds | Invalid credentials were accepted or the wrong listener was tested | Verify the endpoint and rendered listener authentication |
| Negative test is inconclusive | The request did not reach a usable authentication exchange | Check DNS, Service endpoints, Pods, and listener port |
| Pod is `Running`, but a fresh test fails | The health probe is not proof of current Kafka authentication, or an older connection was cached | Inspect component logs and repeat after reconciliation |
| Secret `resourceVersion` changed, but behavior is old | Secret projection, CFK reconciliation, or rolling restart is incomplete | Check tracked Secret annotation and Pod revisions |
| `TopicAuthorizationException` or another authorization error | Authentication likely succeeded, but permissions were denied | Inspect ACL/RBAC configuration |
| `/mnt/secrets` is absent from a component container | CFK rendered the credential into generated component configuration | Use the disposable Secret-mounted client in this runbook |

## 13. Cleanup

Delete the disposable client when testing is complete:

```bash
kubectl -n prod delete pod sasl-auth-check --ignore-not-found
```

Do not commit decoded Secrets, temporary JAAS properties, command output
containing credentials, or files under `production/02-security/runtime/`.

## References

- [Configure Kafka and KRaft authentication with CFK](https://docs.confluent.io/operator/current/co-authenticate-kafka.html)
- [Manage authentication and credential updates with CFK](https://docs.confluent.io/operator/current/co-manage-authentication.html)
- [CFK Secret tracking and safe rolling restarts](https://docs.confluent.io/operator/current/co-manage-certificates.html#secret-updates-and-safe-rolling-restarts)
- [Restart Confluent Platform components with CFK](https://docs.confluent.io/operator/current/co-roll-cluster.html)
- [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
