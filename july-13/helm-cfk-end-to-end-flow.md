# Helm and CFK: end-to-end flow

```mermaid
flowchart TB
    USER([Engineer])
    APISERVER["Kubernetes API Server"]

    subgraph INSTALL["1. Bootstrap CFK — install or upgrade"]
        HELM["Helm client"]
        CHART["CFK Helm chart"]
        CRDS["CRDs<br/>Kafka and KRaftController API types"]
        ACCESS["ServiceAccount and RBAC"]
        DEPLOY["Deployment: confluent-operator"]
        OP["Operator Pod<br/>confluent-operator-&lt;hash&gt;-&lt;suffix&gt;"]

        HELM -->|"renders templates with values"| CHART
        CHART -->|"submits Kubernetes objects"| APISERVER
        APISERVER --> CRDS
        APISERVER --> ACCESS
        APISERVER --> DEPLOY
        DEPLOY -->|"ReplicaSet creates"| OP
    end

    subgraph DECLARE["2. Declare the desired Confluent state"]
        APPLY["kubectl apply"]
        FILE["cp-singlenode.yaml<br/>KRaftController/kraftcontroller<br/>Kafka/kafka"]
        STORE[("etcd<br/>spec = desired state")]

        APPLY --> FILE
        FILE -->|"two custom-resource requests"| APISERVER
        CRDS -.->|"supply types and validation schemas"| APISERVER
        APISERVER -->|"validates and persists"| STORE
    end

    subgraph RECONCILE["3. CFK reconciliation — continuous"]
        GENERATED["Generated Kubernetes objects<br/>StatefulSets, Services, ConfigMaps and PVCs"]
        CONTROL["Built-in Kubernetes controllers<br/>StatefulSet controller, scheduler and storage provisioner"]
        KUBELET["Node kubelet<br/>pulls images and runs containers"]
        KRAFT["kraftcontroller-0<br/>10 Gi PVC"]
        KAFKA["kafka-0<br/>10 Gi PVC"]
        STATUS["Custom-resource status<br/>RUNNING, readyReplicas: 1"]

        APISERVER -->|"watch events and current state"| OP
        OP -->|"create or update"| APISERVER
        APISERVER --> GENERATED
        GENERATED --> CONTROL
        CONTROL --> KUBELET
        KUBELET --> KRAFT
        KRAFT -->|"Kafka clusterRef dependency is ready"| KAFKA
        KUBELET --> KAFKA
        OP -->|"reports observed state"| STATUS
        STATUS --> APISERVER
    end

    USER --> HELM
    USER --> APPLY

    classDef installer fill:#0f766e,color:#fff,stroke:#115e59,stroke-width:2px;
    classDef api fill:#2563eb,color:#fff,stroke:#1e40af,stroke-width:2px;
    classDef desired fill:#7c3aed,color:#fff,stroke:#5b21b6,stroke-width:2px;
    classDef operator fill:#ea580c,color:#fff,stroke:#9a3412,stroke-width:2px;
    classDef runtime fill:#15803d,color:#fff,stroke:#166534,stroke-width:2px;

    class HELM,CHART installer;
    class APISERVER,CRDS,ACCESS,DEPLOY api;
    class APPLY,FILE,STORE desired;
    class OP,STATUS operator;
    class GENERATED,CONTROL,KUBELET,KRAFT,KAFKA runtime;
```

## Reading the diagram

- **Helm bootstraps CFK:** it renders and submits CRDs, permissions, and the operator Deployment, then the Helm process exits.
- **The CRDs teach the API server new types:** they make `Kafka` and `KRaftController` valid Kubernetes resources.
- **Your file owns desired state:** `spec` in `cp-singlenode.yaml` says that one KRaft controller and one Kafka broker should exist.
- **The operator owns reconciliation:** it watches those custom resources and creates or updates the lower-level Kubernetes objects needed to realize the requested state.
- **Kubernetes owns execution:** built-in controllers, the scheduler, storage provisioner, and kubelet create PVCs, select a node, and run the Pods.
- **The operator reports observed state:** it writes readiness and lifecycle information under each custom resource's `status`.

The operator Pod's random-looking name is generated from the Deployment name, a ReplicaSet hash, and a unique Pod suffix. Its name may change; its reconciliation role does not.
