# SASL/PLAIN secret materialization

Real credentials live below `runtime/`, which is ignored by Git.

## Kafka broker to KRaft controller

Although Kafka acts as the client of the KRaft controller, CFK's
`dependencies.kRaftController.controllerListener` field expects a
listener-style Secret. It must contain these keys:

- `plain-users.json`: the controller listener's accepted-user map.
- `plain-interbroker.txt`: the identity Kafka presents to the controller.

The existing Kafka-to-KRaft `plain.txt` already has the required
`username`/`password` format, so materialize it under the key name CFK expects:

```bash
kubectl -n prod create secret generic kafka-kraft-sasl-client \
  --from-file=plain-users.json=production/02-security/runtime/server/kraft/plain-users.json \
  --from-file=plain-interbroker.txt=production/02-security/runtime/clients/kafka-to-kraft/plain.txt \
  --dry-run=client -o yaml | kubectl apply -f -
```

Verify only the key names, without printing credential values:

```bash
kubectl -n prod get secret kafka-kraft-sasl-client \
  -o go-template='{{range $key,$value := .data}}{{printf "%s\\n" $key}}{{end}}'
```

The username and password in `plain-interbroker.txt` must match an entry in the
KRaft controller's `plain-users.json`. Authentication does not grant Kafka
permissions; authorization is configured separately.
