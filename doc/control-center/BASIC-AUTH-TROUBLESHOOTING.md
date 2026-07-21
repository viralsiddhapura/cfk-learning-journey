# Troubleshooting CFK Control Center Basic authentication

This guide documents a real troubleshooting session in which HTTP Basic authentication was added to a CFK-managed Control Center 7.9.1 deployment, but the browser did not request a username and password.

The incident had two separate causes:

1. The Kubernetes Secret did not contain the key name CFK expected.
2. After fixing the Secret, the CFK operator generated a Jetty login module that was incompatible with the Confluent Platform 7.9.1 Control Center image.

The final result was a healthy Control Center deployment with working `Administrators` and `Restricted` users.

## Problem statement

Control Center was configured with server-side HTTP Basic authentication:

```yaml
spec:
  authentication:
    type: basic
    basic:
      roles:
        - Administrators
        - Restricted
      restrictedRoles:
        - Restricted
      secretRef: controlcentersecret
```

However, opening Control Center at `http://localhost:9021` did not prompt for credentials.

The expected behavior was:

- An unauthenticated request to a protected Control Center API should return `401 Unauthorized`.
- The response should include `WWW-Authenticate: basic realm="c3"`.
- A user in `Administrators` should receive full read/write Control Center access.
- A user in `Restricted` should receive limited, mostly read-only access.

## Lab environment

| Item | Value |
|---|---|
| Kubernetes namespace | `confluent` |
| Control Center CR | `controlcenter` |
| Control Center Pod | `controlcenter-0` |
| Control Center image | `confluentinc/cp-enterprise-control-center:7.9.1` |
| CFK operator image observed during troubleshooting | `confluentinc/confluent-operator:0.1514.40` |
| Control Center port | `9021` |
| Authentication method | HTTP Basic |
| Administrative role | `Administrators` |
| Restricted role | `Restricted` |

Control Center 7.9.1 is Control Center Legacy. Its Basic-auth implementation supports administrator and restricted groups, but it is not Confluent RBAC.

## Initial configuration

The relevant manifest is [control-centre.yaml](../../july-13/control-centre.yaml), and the source credential file is [controlcentersecret.txt](../../july-13/controlcentersecret.txt).

The credential file followed the correct Control Center format:

```text
c3admin: <admin-password>,Administrators
c3restricted: <restricted-password>,Restricted
```

For Control Center, the values after the comma are roles. They are not part of the password:

| User | Password | Role |
|---|---|---|
| `c3admin` | `<admin-password>` | `Administrators` |
| `c3restricted` | `<restricted-password>` | `Restricted` |

Role names are case-sensitive and must match `roles` and `restrictedRoles` in the Control Center CR.

### The initial Secret mistake

The Secret was originally created from the file without overriding the Secret data key:

```bash
kubectl create secret generic controlcentersecret \
  --namespace confluent \
  --from-file=controlcentersecret.txt
```

That creates this Secret data key:

```text
controlcentersecret.txt
```

CFK does not infer the expected key from the filename or from `secretRef`. For server-side Basic authentication, it requires the exact key:

```text
basic.txt
```

The Secret object name and its data key are different concepts:

```text
secretRef: controlcentersecret     -> Kubernetes Secret object name
required data key: basic.txt       -> File mounted and consumed by Control Center
```

## Troubleshooting walkthrough

### 1. Inspect the desired state and reconciliation status

Confirm that the live Control Center CR contains the expected authentication configuration:

```bash
kubectl -n confluent get controlcenter controlcenter \
  -o jsonpath='{.spec.authentication}{"\n"}'
```

Then inspect the complete resource, including conditions and events:

```bash
kubectl -n confluent describe controlcenter controlcenter
```

The important event was:

```text
required key [basic.txt] missing in secretRef [controlcentersecret] for auth type [basic]
```

The CR also showed:

```text
metadata.generation:       4
status.observedGeneration: 3
```

This meant the API server had accepted generation 4 as the desired state, but CFK had only successfully reconciled generation 3. The existing Control Center Pod could remain healthy and `Running` while the newest authentication change was failing.

Useful command:

```bash
kubectl -n confluent get controlcenter controlcenter \
  -o jsonpath='generation={.metadata.generation}{" observed="}{.status.observedGeneration}{" phase="}{.status.phase}{" ready="}{.status.readyReplicas}{"\n"}'
```

### 2. Inspect the actual Secret keys

List only the keys, without decoding or exposing credential values:

```bash
kubectl -n confluent get secret controlcentersecret \
  -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
```

The original output contained `controlcentersecret.txt` instead of `basic.txt`.

### 3. Correct the Secret in place

The source file can keep its descriptive local filename. Use `--from-file=<required-key>=<source-file>` to create the key CFK expects:

```bash
kubectl create secret generic controlcentersecret \
  --namespace confluent \
  --from-file=basic.txt=cfk-learning-journey/july-13/controlcentersecret.txt \
  --dry-run=client -o yaml | \
kubectl apply -f -
```

This updates the existing Secret without first deleting it. Deleting the Secret creates an avoidable interval in which the referenced credential object does not exist.

Verify the required key:

```bash
kubectl -n confluent get secret controlcentersecret \
  -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'
```

Expected to include:

```text
basic.txt
```

If an older, unused key is also present, CFK ignores it. The required condition is that `basic.txt` exists and contains valid credentials.

### 4. Observe CFK reconciliation

After the Secret was corrected, CFK detected the change, reconciled generation 4, generated new configuration, and replaced `controlcenter-0` automatically.

Observe the resources:

```bash
kubectl -n confluent get controlcenter,pod -w
```

Wait for the StatefulSet roll:

```bash
kubectl -n confluent rollout status statefulset/controlcenter \
  --timeout=10m
```

Check the Pod UID and readiness:

```bash
kubectl -n confluent get pod controlcenter-0 \
  -o jsonpath='uid={.metadata.uid}{" created="}{.metadata.creationTimestamp}{" ready="}{.status.containerStatuses[0].ready}{" restarts="}{.status.containerStatuses[0].restartCount}{"\n"}'
```

No manual `kubectl rollout restart` was needed in this incident because CFK initiated the roll.

### 5. Confirm that CFK rendered Basic authentication

Inspect the generated Control Center settings:

```bash
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'grep -E "confluent\.controlcenter\.(rest\.authentication|auth\.restricted)" \
  /mnt/config/shared/controlcenter.properties'
```

Expected settings:

```properties
confluent.controlcenter.auth.restricted.roles=Restricted
confluent.controlcenter.rest.authentication.method=BASIC
confluent.controlcenter.rest.authentication.realm=c3
confluent.controlcenter.rest.authentication.roles=Administrators,Restricted
confluent.controlcenter.rest.authentication.skip.paths=/2.0/status/app_info
```

Also confirm that the Secret is mounted:

```bash
kubectl -n confluent get pod controlcenter-0 \
  -o jsonpath='{range .spec.volumes[*]}{.name}{" secret="}{.secret.secretName}{" configMap="}{.configMap.name}{"\n"}{end}'
```

### 6. Do not use the health endpoint as the authentication test

An unauthenticated request to this endpoint returned `200 OK`:

```text
/2.0/status/app_info
```

That was expected because CFK generated:

```properties
confluent.controlcenter.rest.authentication.skip.paths=/2.0/status/app_info
```

Kubernetes probes need this endpoint without user credentials. A successful response from it does not prove that Basic authentication is disabled.

The root path `/` can also serve public static UI content. Use a protected API path and inspect the `WWW-Authenticate` header instead:

```bash
curl -sS -D - -o /dev/null \
  http://127.0.0.1:9021/2.0/clusters
```

Expected unauthenticated response:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: basic realm="c3"
```

### 7. Diagnose the second failure: correct users still returned 401

After fixing the Secret, the authentication filter was active, but both valid users still received `401 Unauthorized`.

Inspect the generated JAAS login module:

```bash
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'grep PropertyFileLoginModule /mnt/config/shared/jaas-config.file'
```

The first generated value was:

```text
org.eclipse.jetty.security.jaas.spi.PropertyFileLoginModule
```

The CFK operator was generating the newer Jetty class, while the Confluent Platform 7.9.1 Control Center image required the Jetty 9-compatible class:

```text
org.eclipse.jetty.jaas.spi.PropertyFileLoginModule
```

### 8. Apply the Control Center 7.9 compatibility annotation

Apply Confluent's compatibility annotation to the live CR:

```bash
kubectl -n confluent annotate controlcenter controlcenter \
  platform.confluent.io/use-old-jetty9=true \
  --overwrite
```

CFK regenerated the JAAS configuration and rolled Control Center again.

Wait for completion:

```bash
kubectl -n confluent rollout status statefulset/controlcenter \
  --timeout=10m
```

Verify the corrected class:

```bash
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'grep PropertyFileLoginModule /mnt/config/shared/jaas-config.file'
```

Expected:

```text
org.eclipse.jetty.jaas.spi.PropertyFileLoginModule
```

Persist the annotation in the manifest so a clean deployment does not lose it:

```yaml
metadata:
  annotations:
    platform.confluent.io/use-old-jetty9: "true"
```

### 9. Validate authentication safely

Start a local port-forward:

```bash
kubectl -n confluent port-forward service/controlcenter 9021:9021
```

Verify that an unauthenticated protected request is rejected:

```bash
curl -sS -o /dev/null \
  -w 'unauthenticated=%{http_code}\n' \
  http://127.0.0.1:9021/2.0/clusters
```

Expected:

```text
unauthenticated=401
```

Test each user without putting the password directly in shell history. Supplying only the username makes `curl` prompt for the password:

```bash
curl -sS -o /dev/null \
  -w 'admin=%{http_code}\n' \
  -u c3admin \
  http://127.0.0.1:9021/2.0/clusters

curl -sS -o /dev/null \
  -w 'restricted=%{http_code}\n' \
  -u c3restricted \
  http://127.0.0.1:9021/2.0/clusters
```

In this environment, accepted credentials passed the authentication filter and reached endpoint routing, while missing or incorrect credentials remained `401`. A downstream `404` from this probe path means authentication passed but that particular route did not resolve to a resource; it is not an authentication failure.

For the final functional check, open `http://127.0.0.1:9021` in separate incognito or browser-profile sessions:

- The administrative user should have full read/write Control Center access.
- The restricted user should have limited access, with management controls hidden or denied.

Browsers cache HTTP Basic credentials, so using the same normal browser session is not a reliable way to switch between users.

## Root-cause summary

| Failure | Evidence | Resolution |
|---|---|---|
| Wrong Secret data key | `KeyInSecretRefIssue`: required key `basic.txt` missing | Recreated/applied the Secret data using `--from-file=basic.txt=<source-file>` |
| Latest CR generation not applied | `metadata.generation=4`, `status.observedGeneration=3` | Fixed the blocking Secret error and let CFK reconcile |
| Health endpoint returned `200` without credentials | Generated `skip.paths=/2.0/status/app_info` | Tested a protected endpoint instead |
| Correct users still returned `401` | Generated newer Jetty login-module class | Applied `platform.confluent.io/use-old-jetty9=true` for Control Center 7.9.1 |
| Browser did not visibly switch roles | HTTP Basic credentials cached by the browser | Used separate incognito sessions or browser profiles |

## What this issue teaches

### 1. Secret names and Secret keys are not interchangeable

`secretRef` identifies a Kubernetes Secret object. The consuming component still expects exact data keys inside that object. A correctly named Secret with the wrong key is unusable.

### 2. `Running` does not prove the newest desired state was applied

CFK can preserve an older healthy Pod when a newer reconciliation fails. Always compare:

```text
metadata.generation
status.observedGeneration
status.conditions
events
```

### 3. Generated configuration is the runtime source of truth

The CR shows intent. The generated ConfigMaps, mounted Secrets, JAAS file, Pod template, and application logs show what the process actually received.

### 4. Authentication must be tested on a protected path

Health endpoints and static UI files are commonly excluded from authentication. Test a protected API, confirm `401` without credentials, and inspect `WWW-Authenticate`.

### 5. Authentication success and endpoint success are different

A response after the authentication filter can still be `403`, `404`, or another application-specific status. Compare correct, missing, and deliberately wrong credentials rather than assuming every valid login must return `200` from every URL.

### 6. Component and CFK versions affect generated security classes

Using a newer CFK operator with a Confluent Platform 7.x component can require compatibility annotations. Verify the generated JAAS class instead of assuming the YAML alone is sufficient.

### 7. Control Center Basic roles are not Kafka authorization

`Administrators` and `Restricted` control what a user can do through Control Center. They do not create Kafka ACLs or Confluent RBAC role bindings, and they do not secure Kafka broker listeners.

### 8. Protect Basic authentication with TLS

HTTP Basic credentials are Base64-encoded, not encrypted. This local lab uses HTTP for simplicity, but production deployments must use HTTPS/TLS, managed secrets, password rotation, and preferably a centralized identity and authorization model.

## Quick diagnostic checklist

Run these checks in order:

```bash
# 1. Desired authentication configuration
kubectl -n confluent get controlcenter controlcenter \
  -o jsonpath='{.spec.authentication}{"\n"}'

# 2. Reconciliation conditions and events
kubectl -n confluent describe controlcenter controlcenter

# 3. Secret data keys without decoding values
kubectl -n confluent get secret controlcentersecret \
  -o go-template='{{range $k,$v := .data}}{{$k}}{{"\n"}}{{end}}'

# 4. Generated Control Center authentication properties
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'grep -E "rest\.authentication|auth\.restricted" \
  /mnt/config/shared/controlcenter.properties'

# 5. Generated JAAS login module
kubectl -n confluent exec controlcenter-0 -c controlcenter -- \
  sh -c 'grep PropertyFileLoginModule /mnt/config/shared/jaas-config.file'

# 6. Protected endpoint challenge
curl -sS -D - -o /dev/null \
  http://127.0.0.1:9021/2.0/clusters
```

## References

- [Configure authentication for Confluent Platform components using CFK](https://docs.confluent.io/operator/current/co-authenticate-cp.html)
- [Configure HTTP Basic authentication for Control Center 7.9](https://docs.confluent.io/platform/7.9/control-center/security/authentication.html)
- [Upgrade CFK: Basic authentication compatibility for Confluent Platform 7.x](https://docs.confluent.io/operator/current/co-upgrade-cfk.html)
- [Restart Confluent Platform components using CFK](https://docs.confluent.io/operator/current/co-roll-cluster.html)
