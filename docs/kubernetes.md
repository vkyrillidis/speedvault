# Run on Kubernetes

`deploy/kubernetes/` holds Kustomize manifests (`kubectl apply -k`, no
other tool):

- `base`: stuart (a one-replica StatefulSet, its state on a 1 GiB volume,
  `--auto --replicas 1`), the nodes (a StatefulSet of one-CPU, 1 GiB pods,
  six to start: three primaries, each with a replica), the seed Service, a
  PodDisruptionBudget.
- `components/autoscaling`: the HorizontalPodAutoscaler, to add to an
  overlay of your own; `overlays/autoscaling` is the base with it, and
  holds the Prometheus adapter's rules (`prometheus-adapter.yaml`, Helm
  values).
- `components/no-replicas`: the base without replicas (three nodes, all
  primaries: half the pods, but a pod lost loses its keys, and a rolling
  update moves every key; see "What happens when").
- `overlays/kind`: what `tools/k8s/kind_check.sh` tests in kind (the image
  built locally, small requests).

```sh
kubectl create namespace db
kubectl -n db apply -k deploy/kubernetes/base    # set the image in an overlay of your own
kubectl -n db scale statefulset speedvault --replicas 8   # a new primary and its replica
kubectl -n db scale statefulset speedvault --replicas 6   # the last two go, their slots moved first
```

**Autoscaling** needs Prometheus scraping the pods (the nodes carry
`prometheus.io/scrape` and `prometheus.io/port` annotations; the series
need `namespace` and `pod` labels, as the prometheus chart's
`kubernetes-pods` job gives them) and the Prometheus adapter with our
rules, then the HPA:

```sh
helm install prometheus prometheus-community/prometheus -n monitoring --create-namespace \
  --set server.global.scrape_interval=15s
helm install prometheus-adapter prometheus-community/prometheus-adapter -n monitoring \
  -f deploy/kubernetes/overlays/autoscaling/prometheus-adapter.yaml \
  --set prometheus.url=http://prometheus-server.monitoring.svc --set prometheus.port=80
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/db/pods/*/speedvault_busy_cores"
kubectl -n db apply -k deploy/kubernetes/overlays/autoscaling
kubectl -n db get hpa speedvault      # TARGETS: busy cores against 400m, memory ratio against 700m
```

The scrape interval matters: the busy rule is a rate over a minute, which
needs two scrapes in it, and the chart scrapes every minute by default
(the HPA then gets no busy metric). The HPA scales on two metrics per pod:
`speedvault_busy_cores` (the share of its core a node works, from its busy
time: not the container's CPU, which busy-polling keeps near 100% at any
load) and `speedvault_memory_used_ratio` (its memory against its
`maxmemory`: not the container's memory, as the allocator keeps pages
after keys go). [KEDA](https://keda.sh) reads Prometheus too, without the
adapter; a `ScaledObject` doing the same would be (not tested here):

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: speedvault
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: StatefulSet, name: speedvault}
  minReplicaCount: 6
  maxReplicaCount: 12
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:      # as in components/autoscaling/hpa.yaml
        scaleUp: {stabilizationWindowSeconds: 60, policies: [{type: Pods, value: 2, periodSeconds: 300}]}
        scaleDown: {selectPolicy: Disabled}
  triggers:          # AverageValue: the query's value divided by the pods
    - type: prometheus
      metricType: AverageValue
      metadata:
        serverAddress: http://prometheus-server.monitoring.svc
        query: sum(rate(speedvault_busy_seconds_total{namespace="db"}[1m]))
        threshold: "0.4"
    - type: prometheus
      metricType: AverageValue
      metadata:
        serverAddress: http://prometheus-server.monitoring.svc
        query: sum(speedvault_memory_used_ratio{namespace="db"})
        threshold: "0.7"
```

**Clients** in the Kubernetes cluster (any namespace) take
`speedvault-seed.<namespace>.svc:7000` as their seed: the nodes that are
ready (joined, not leaving). They are redirected to the nodes' pod
addresses, which is what each node announces (`--announce
$(POD_IP):7000`); it joins stuart with its pod's name (`--name
$(POD_NAME)`), by which stuart recognises a pod that comes back at a new
address. Addresses, not names, because a client keeps a connection per
node address and reconnects it when it drops: a restarted pod's name is
the old one's, and a client that caches DNS reconnects to the old pod's
address, where nothing answers. Lettuce does (netty's resolver keeps an
answer for its TTL, 30 s from CoreDNS by default): with the nodes
announcing names, a rolling update cost it 4–7 timeouts; with addresses,
none. Redis Cluster on Kubernetes announces pod addresses too. Clients
outside the Kubernetes cluster can't reach the pods: run them inside, or
give the nodes addresses they can reach (`--announce`). For Lettuce, see
also the refresh settings in [running a cluster](cluster.md).

**Replicas**: stuart (`--replicas 1`) makes the first pods to join
primaries and the next their replicas, and each pod joining later fills a
primary's missing replica before it becomes a primary itself: grow and
shrink the StatefulSet by two. A node's zone is its Kubernetes node
(`--zone $(NODE_NAME)`, from the Downward API): stuart gives a replica a
primary on another machine where one lacks a replica, and the pods are
spread over the machines (a `topologySpreadConstraint`, not required: on
fewer machines than pods they share). A replica is ready only once synced
with its primary, so a rolling update, or an eviction under the
PodDisruptionBudget, waits for each pod back to hold its copy before
stopping the next. Without replicas: `components/no-replicas`.

**What happens when** (see the dbnode's and stuart's sections):

- A pod starts: the node joins stuart, which gives it its share of the
  slots; the moves run while clients go on.
- A pod gets SIGTERM (a scale-down, a rolling update, `kubectl delete
  pod`, an eviction; the PodDisruptionBudget and `OrderedReady` let one go
  at a time): a primary hands its slots to its replica (its writes wait a
  few milliseconds; no key moves), a replica just goes; either redirects
  clients for 10 s more (`--leave-linger`) and exits. The pod back joins
  as the replica of the primary lacking one and copies its keys: a
  rolling update moves no slot. A primary without a replica that can take
  over (none, or one still copying after a minute) drains instead: its
  slots move to the others, then it exits. Without replicas
  (`components/no-replicas`) every SIGTERM drains, and a rolling update
  moves each pod's keys out and back in.
- A scale-down removes the highest pods, which joined last: usually a
  primary and its own replica, whose slots move to the others first. If
  the roles were shuffled (rolling updates, failovers), stuart makes a
  primary the nodes no longer need a replica (`--auto`), its slots moved
  away first.
- A pod killed (`--grace-period=0 --force`, the node out of memory)
  comes back under its name, empty, and stuart takes it for the old node:
  a primary's replica is promoted at once, losing only the writes the
  primary hadn't passed on (none that a `WAIT` saw acknowledged), and the
  pod becomes its replica. A
  primary with no replica up loses its keys: its slots go out again,
  empty.
- A machine stops: stuart fails its primaries over to their replicas
  after `--fail-after` (5 s; in kind every slot was served elsewhere 7 s
  after a worker was killed, nothing acknowledged lost). Its pods stay
  where they were (a StatefulSet never runs two pods of one name, and
  can't tell that they stopped) until the machine is back, or deleted
  (`kubectl delete node`) or marked out of service (the
  `node.kubernetes.io/out-of-service` taint, once the node is
  `NotReady`, about 50 s on): then they start elsewhere and join as
  replicas (in kind, ready 13 s after the taint). A node that won't come back stays `down` in
  `STUART NODES` until `STUART FORGET <id>` (stuart can't tell a dead
  node from one it can't reach).
- stuart's pod is rescheduled: clients go on; joins, leaves and moves wait
  for it, and it starts again from its state on the volume. Keep the
  volume: a stuart that lost it knows no nodes, and they don't join again.
- Everything deleted at once (`kubectl delete -k`, the namespace): every
  node is leaving, so stuart lets them all go at once; gone in seconds.

**Sizing:**

- **CPU**: one core per node, `requests` = `limits` (a node is one
  thread). The HPA adds pods when the nodes average 40% busy: with half
  of them replicas, which only apply their primary's writes, that is the
  primaries at 70–80%, the rest room for the moves, as a new node relieves
  the others only once its slots have arrived (70% with
  `components/no-replicas`).
- **Memory**: the pod's limit, with `--maxmemory 75%` of it for keys; the
  rest is the allocator's and the connections'. The HPA adds pods at 70%
  of `maxmemory` on average; stuart moves no slot to a node at 90%; past
  100% writes get `-OOM`. Small strings take about twice Redis 7.0.15's
  memory and 2.6–2.9× Redis 8's and Valkey 9's (see the open items;
  collections about theirs): 1 GiB holds about 4M keys of 10 bytes. A
  replica takes what its primary takes: with replicas the cluster holds
  half as much. A replica's sync needs no room at the primary (no fork:
  Redis's copy-on-write takes up to 1.9× under writes); the replica peaks
  10–20% above its final size while it syncs, inside the quarter kept
  outside `maxmemory`.
- **Grace period**: `terminationGracePeriodSeconds` (660 s) must cover
  `--drain-timeout` (600 s) and `--leave-linger` (10 s), so that a node
  exits by itself. A handover takes milliseconds, then the linger; the
  long grace period is for a primary that drains. A drain takes what moving the node's keys takes: in
  kind, two new nodes took their 2/5 of 550 MB in about 3 s, and a
  scale-down of two or three nodes took 25–40 s, lingers included.
- **The HPA** (`components/autoscaling/hpa.yaml`): 6 to 12 pods, up by at
  most two pods (a pair) per five minutes after a minute of load, never down by
  itself (`scaleDown.selectPolicy: Disabled`: scaling down moves every key
  of the nodes removed, and load that comes and goes would move them back
  and forth). Scale down with `kubectl scale`; the pods leave one after
  the other.

**Testing it**: `tools/k8s/kind_check.sh` runs the manifests in kind (a
control plane and three workers), a checker writing throughout with
redis-py, redis-cli and Lettuce, and with `WAIT`; every replica is then
compared with its primary key by key. Besides scaling, a rolling update
(no slot moved), pods deleted and force-deleted, it kills a worker
(`docker kill`): its primaries fail over, its pods start on the others
once it has the out-of-service taint. `--autoscale` installs Prometheus
and the adapter and lets the HPA grow the cluster by pairs;
`--no-replicas` runs it all without replicas (see [performance](performance.md) and
`tools/k8s/`).

