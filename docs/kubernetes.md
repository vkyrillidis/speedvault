# Run on Kubernetes

`deploy/kubernetes/` holds Kustomize manifests (`kubectl apply -k`, no
other tool):

- `base`: stuart (a one-replica StatefulSet, its state on a 1 GiB volume,
  `--auto`), the nodes (a StatefulSet of one-CPU, 1 GiB pods, three to
  start), the seed Service, a PodDisruptionBudget.
- `components/autoscaling`: the HorizontalPodAutoscaler, to add to an
  overlay of your own; `overlays/autoscaling` is the base with it, and
  holds the Prometheus adapter's rules (`prometheus-adapter.yaml`, Helm
  values).
- `overlays/kind`: what `tools/k8s/kind_check.sh` tests in kind (the image
  built locally, small requests).

```sh
kubectl create namespace db
kubectl -n db apply -k deploy/kubernetes/base    # set the image in an overlay of your own
kubectl -n db scale statefulset speedvault --replicas 5   # the two new nodes get their share
kubectl -n db scale statefulset speedvault --replicas 3   # the last two drain, then exit
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
kubectl -n db get hpa speedvault      # TARGETS: busy cores and memory ratio, each against 700m
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
  minReplicaCount: 3
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
        threshold: "0.7"
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

**What happens when** (see the dbnode's and stuart's sections):

- A pod starts: the node joins stuart, which gives it its share of the
  slots; the moves run while clients go on.
- A pod gets SIGTERM (a scale-down, a rolling update, `kubectl delete
  pod`, an eviction; the PodDisruptionBudget and `OrderedReady` let one go
  at a time): the node leaves, its slots move to the others, then it
  redirects clients for 10 s more (`--leave-linger`) and exits. Without
  persistence that's the only way a pod stops without losing keys, so it
  happens on every SIGTERM: a rolling update moves each pod's keys out and
  back in.
- A pod killed without draining (`--grace-period=0 --force`, its machine
  gone) comes back under its name, empty: stuart takes it for the old
  node, and the old node's slots go out again, empty (their keys are lost;
  replicas are phase 8). A pod that won't come back stays `down` in
  `STUART NODES`, its slots unserved, until `STUART FORGET <id>` (stuart
  can't tell a dead node from one it can't reach).
- stuart's pod is rescheduled: clients go on; joins, leaves and moves wait
  for it, and it starts again from its state on the volume. Keep the
  volume: a stuart that lost it knows no nodes, and they don't join again.
- Everything deleted at once (`kubectl delete -k`, the namespace): every
  node is leaving, so stuart lets them all go at once; gone in seconds.

**Sizing:**

- **CPU**: one core per node, `requests` = `limits` (a node is one
  thread). The HPA adds pods when the nodes average 70% busy: the 30% left
  is room for the moves, as a new node relieves the others only once its
  slots have arrived.
- **Memory**: the pod's limit, with `--maxmemory 75%` of it for keys; the
  rest is the allocator's and the connections'. The HPA adds pods at 70%
  of `maxmemory` on average; stuart moves no slot to a node at 90%; past
  100% writes get `-OOM`. Small keys take about twice Redis's memory (see
  the open items): 1 GiB holds about 4M keys of 10 bytes.
- **Grace period**: `terminationGracePeriodSeconds` (660 s) must cover
  `--drain-timeout` (600 s) and `--leave-linger` (10 s), so that a node
  exits by itself. A drain takes what moving the node's keys takes: in
  kind, two new nodes took their 2/5 of 550 MB in about 3 s, and a
  scale-down of two or three nodes took 25–40 s, lingers included.
- **The HPA** (`components/autoscaling/hpa.yaml`): 3 to 12 pods, up by at
  most two pods per five minutes after a minute of load, never down by
  itself (`scaleDown.selectPolicy: Disabled`: scaling down moves every key
  of the nodes removed, and load that comes and goes would move them back
  and forth). Scale down with `kubectl scale`; the pods leave one after
  the other.

**Testing it**: `tools/k8s/kind_check.sh` runs the manifests in kind, a
checker writing throughout with redis-py, redis-cli and Lettuce;
`--autoscale` installs Prometheus and the adapter and lets the HPA grow
the cluster (see "Performance" and `tools/k8s/`).
