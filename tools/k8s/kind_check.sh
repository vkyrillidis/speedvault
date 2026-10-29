#!/usr/bin/env bash
# The Kubernetes manifests end to end, in kind (Kubernetes in Docker):
# makes a kind cluster of a control plane and three workers, builds the
# image and the checker's (checker/Dockerfile) and loads them into it,
# applies deploy/kubernetes/overlays/kind in a namespace of its own, and
# runs the scenarios below. In each, a checker pod (kcheck.py), in another
# namespace, writes through the seed Service while the pods change (redis-py,
# redis-cli and Lettuce, LETTUCE below; redis-py with WAIT too), then reads
# every node: no acknowledged write lost, none applied twice, no key at
# two nodes, every replica the same as its primary, and no write failed
# unless a pod was killed (then only writes its replica hadn't got may be
# lost).
#
# The manifests' six pods are three primaries with a replica each (stuart
# --replicas 1):
#
#   form        six nodes form a cluster by themselves (stuart --auto):
#               three pairs, each replica on another kind node than its
#               primary
#   scale-up    kubectl scale 6 -> 8: a new primary with its share, and
#               its replica
#   scale-down  kubectl scale 8 -> 6: the last two go, one after the other,
#               and exit by themselves
#   rollout     kubectl rollout restart: each primary hands its slots to
#               its replica, each pod back is a replica: no slot moves
#   delete      kubectl delete pod of a primary: it hands over (no slot
#               moves), and comes back as a replica
#   force       kubectl delete pod --grace-period=0 --force of a primary:
#               its replica is promoted, it comes back as its replica;
#               only writes the replica hadn't got may be lost
#   worker-stop a kind worker killed (docker kill: its machine gone, no
#               SIGTERM for its pods) that holds no stuart nor checker:
#               its primaries fail over to their replicas;
#               once Kubernetes marks it NotReady it gets the out-of-service
#               taint, and its pods start on the other workers, as
#               replicas; then it starts again
#   stuart      stuart's pod deleted: it comes back with its state (the
#               table, the nodes) from its volume, writes go on meanwhile
#   delete-all  kubectl delete -k of the overlay (stuart, the nodes and
#               their Services at once): every pod gone within a minute,
#               then applied again
#   teardown    the namespace deleted (every pod gets SIGTERM at once): its
#               pods gone within a minute, not at the end of the grace
#               period (the namespace itself goes a minute or two later)
#
# Before them, the autoscaling overlay (the HPA) is checked with a
# server-side dry run.
#
# --no-replicas runs them without replicas (tools/k8s/no-replicas: the
# kind overlay with components/no-replicas): three nodes, scale-up and
# scale-down 3 -> 5 -> 3, each SIGTERM draining the node, force losing
# the pod's keys, worker-stop its pods' keys.
#
# --autoscale runs the HPA for real instead: it installs Prometheus and the
# Prometheus adapter (Helm charts, tools/k8s/monitoring/prometheus.yaml and
# the adapter's rules in deploy/kubernetes/overlays/autoscaling), applies
# tools/k8s/autoscale (the kind overlay with the HPA, quicker). Its scenarios (by default these, then
# rollout and delete):
#
#   hpa-memory  6 nodes filled to 90% of their maxmemory: the HPA adds a
#               pair (ceil(6 x 0.9 / 0.7) = 8), and no more once their
#               slots have come (4 primaries at 67.5%)
#   hpa-cpu     memtier_benchmark keeping 3 primaries about 55% busy, their
#               replicas far less, past the test HPA's 20% average (and its
#               10% tolerance): the HPA adds a pair (to its maximum here,
#               8); the client's throughput and p99, second by second,
#               alongside
#   hpa-down    kubectl scale 8 -> 6 once the load is gone: the HPA leaves
#               it there (it never scales down, and the metrics are low)
#
# With --no-replicas (tools/k8s/autoscale-no-replicas): hpa-memory 3 -> 5
# at 95.5%, hpa-cpu 5 -> 6 (each node 55% busy, the HPA at 40%), hpa-down
# 6 -> 3.
#
# --auth runs them with a password (tools/k8s/auth: the kind overlay with
# components/auth, from a Secret made here): the checker logs in with it,
# without Lettuce (LettuceLoad doesn't log in).
#
# For each, when the HPA asked for more pods, when they were ready, and
# when their slots had arrived, from the start of the load.
#
#   tools/k8s/kind_check.sh [--keep] [--reuse] [--autoscale] [--no-replicas] [--auth] [--out DIR] [SCENARIO...]
#
# --keep leaves the kind cluster up at the end; --reuse uses one that is
# already there (and rebuilds the images). --out DIR keeps memtier's JSON
# and the scaling timelines there. Needs docker; kind (v0.29.0) and
# kubectl from PATH, or KIND and KUBECTL; with --autoscale, helm (v3.18) or
# HELM, and kind's nodes must reach the charts' images. Runs from Git
# Bash on Windows too (Docker Desktop). KIND_API_HOST: the host the kind
# API server is reached at when docker runs elsewhere (CI's
# Docker-in-Docker service, "docker"), instead of 127.0.0.1.
set -euo pipefail
# Git Bash: leave /app/... alone, and give Windows tools C:/... paths.
export MSYS_NO_PATHCONV=1
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && { pwd -W 2>/dev/null || pwd; })"
ROOT="$(cd "$HERE/../.." && { pwd -W 2>/dev/null || pwd; })"
KIND="${KIND:-kind}"
KUBECTL="${KUBECTL:-kubectl}"
CLUSTER=speedvault-check
NODE_IMAGE=kindest/node:v1.33.1
NS=speedvault-check
# The checker runs in a namespace of its own, as clients in other
# namespaces would: the names the nodes announce must resolve there.
CNS=speedvault-client
HELM="${HELM:-helm}"
CHARTS=https://prometheus-community.github.io/helm-charts
PROMETHEUS_CHART=29.37.0
ADAPTER_CHART=5.3.0
# memtier_benchmark in hpa-cpu: threads, clients per thread, the
# --rate-limiting of each of their connections, for so long. In cluster
# mode a client has a connection per primary, each at that rate: the load
# per primary stays the same as nodes come (the HPA goes to its maximum;
# the memory scenario is the one that shows it stopping by itself). The
# defaults make about 560k requests a second on five primaries, about half
# what memtier's 8 threads can do on a 16-core Docker Desktop, so that its
# latencies are the nodes' and not its own queue's; that keeps each
# primary about 55% busy (without busy-polling a request costs a node
# about 5 us of busy time). Elsewhere, tune LOAD_RATE so that hpa-cpu's
# "HPA wants" line shows the nodes past 220m (440m with --no-replicas).
LOAD_THREADS="${LOAD_THREADS:-8}"
LOAD_CLIENTS="${LOAD_CLIENTS:-8}"
LOAD_RATE="${LOAD_RATE:-2000}"
LOAD_TIME="${LOAD_TIME:-240}"
# The checker's Lettuce clients (LettuceLoad's modes, comma separated;
# empty: none).
LETTUCE="${LETTUCE-default}"
KEEP=0
REUSE=0
AUTOSCALE=0
REPLICAS=1
AUTH=0
OUT=""
SCENARIOS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --keep) KEEP=1; shift;;
    --reuse) REUSE=1; shift;;
    --autoscale) AUTOSCALE=1; shift;;
    --no-replicas) REPLICAS=0; shift;;
    --auth) AUTH=1; shift;;
    --out) OUT="$2"; mkdir -p "$OUT"; shift 2;;
    -h|--help) sed -n '2,99p' "$0" | sed '/^set -euo/,$d'; exit 0;;
    -*) echo "unknown argument: $1" >&2; exit 2;;
    *) SCENARIOS+=("$1"); shift;;
  esac
done
# The checker's commands, with the password to log in with (--auth).
AUTH_ENV=()
if [ "$AUTH" = 1 ]; then
  [ "$AUTOSCALE" = 0 ] && [ "$REPLICAS" = 1 ] || { echo "--auth goes without --autoscale and --no-replicas" >&2; exit 2; }
  PASSWORD="kind-check-$RANDOM$RANDOM"
  AUTH_ENV=(env SPEEDVAULT_PASSWORD="$PASSWORD" REDISCLI_AUTH="$PASSWORD")
  LETTUCE=""
fi
if [ ${#SCENARIOS[@]} -eq 0 ]; then
  if [ "$AUTOSCALE" = 1 ]; then
    SCENARIOS=(hpa-memory hpa-cpu hpa-down rollout delete)
  else
    SCENARIOS=(form scale-up scale-down rollout delete force worker-stop stuart delete-all teardown)
  fi
fi
# The pod counts: BASE the manifests', UP after scale-up; the autoscaling
# scenarios' (MEM_* for hpa-memory, CPU_* for hpa-cpu, MAX the test HPA's
# maximum, hpa-down's start).
if [ "$REPLICAS" = 1 ]; then
  BASE=6 UP=8 FILL=0.9 MEM_TO=8 CPU_FROM=6 CPU_TO=8 MAX=8
else
  BASE=3 UP=5 FILL=0.955 MEM_TO=5 CPU_FROM=5 CPU_TO=6 MAX=6
fi

k() { "$KUBECTL" --context "kind-$CLUSTER" -n "$NS" "$@"; }
kc() { "$KUBECTL" --context "kind-$CLUSTER" -n "$CNS" "$@"; }
say() { echo "$(date +%H:%M:%S) $*"; }
FAILED=()
dump() {
  k get pods -o wide >&2 || true
  k logs stuart-0 --tail 40 >&2 || true
  for p in $(k get pods -l app.kubernetes.io/name=speedvault -o name 2>/dev/null); do
    echo "--- $p" >&2; k logs "$p" --tail 15 >&2 || true
  done
}
cleanup() {
  if [ "$KEEP" = 1 ]; then
    say "kind cluster $CLUSTER kept: $KUBECTL --context kind-$CLUSTER -n $NS get pods"
  else
    "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ "$REUSE" = 1 ] && "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  say "reusing kind cluster $CLUSTER"
else
  "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  say "creating kind cluster $CLUSTER ($NODE_IMAGE)"
  if [ -n "${KIND_API_HOST:-}" ]; then
    # Listening on every address of the docker host, with that name in
    # the API server's certificate.
    "$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --wait 120s --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes: [{role: control-plane}, {role: worker}, {role: worker}, {role: worker}]
networking:
  apiServerAddress: "0.0.0.0"
kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      certSANs: ["$KIND_API_HOST"]
EOF
  else
    # Three workers: a replica on another than its primary, and one to stop.
    "$KIND" create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --wait 120s --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes: [{role: control-plane}, {role: worker}, {role: worker}, {role: worker}]
EOF
  fi
fi
if [ -n "${KIND_API_HOST:-}" ]; then
  port=$(docker port "$CLUSTER-control-plane" 6443/tcp | head -1 | sed 's/.*://')
  "$KUBECTL" config set-cluster "kind-$CLUSTER" --server "https://$KIND_API_HOST:$port" >/dev/null
fi
say "building the images"
docker build -q -t speedvault:dev "$ROOT" >/dev/null
docker build -q -t speedvault-checker:dev -f "$HERE/checker/Dockerfile" "$ROOT/tools" >/dev/null
"$KIND" load docker-image --name "$CLUSTER" speedvault:dev speedvault-checker:dev >/dev/null

# Fresh namespaces: no pods, no stuart state. Emptied rather than deleted
# when they're there: with the adapter's aggregated API (--autoscale) a
# namespace deleted can stay Terminating for many minutes after its last
# object went, the namespace controller backing off after a stale
# discovery of custom.metrics.k8s.io.
for ns in "$NS" "$CNS"; do
  if "$KUBECTL" --context "kind-$CLUSTER" get namespace "$ns" >/dev/null 2>&1; then
    "$KUBECTL" --context "kind-$CLUSTER" -n "$ns" delete statefulset,hpa,pod --all --wait=true --timeout 5m >/dev/null
    "$KUBECTL" --context "kind-$CLUSTER" -n "$ns" delete service,pdb,pvc --all --wait=true --timeout 2m >/dev/null
  else
    "$KUBECTL" --context "kind-$CLUSTER" create namespace "$ns" >/dev/null
  fi
done
if [ "$AUTOSCALE" = 1 ]; then
  say "installing Prometheus ($PROMETHEUS_CHART) and the adapter ($ADAPTER_CHART)"
  h() { "$HELM" --kube-context "kind-$CLUSTER" -n monitoring "$@"; }
  h upgrade --install prometheus prometheus --repo "$CHARTS" --version "$PROMETHEUS_CHART"     --create-namespace -f "$HERE/monitoring/prometheus.yaml" --wait --timeout 5m >/dev/null
  h upgrade --install prometheus-adapter prometheus-adapter --repo "$CHARTS" --version "$ADAPTER_CHART"     -f "$ROOT/deploy/kubernetes/overlays/autoscaling/prometheus-adapter.yaml"     --set prometheus.url=http://prometheus-server.monitoring.svc --set prometheus.port=80     --wait --timeout 5m >/dev/null
  OVERLAY="$HERE/autoscale"
  [ "$REPLICAS" = 1 ] || OVERLAY="$HERE/autoscale-no-replicas"
  k apply -k "$OVERLAY" >/dev/null
else
  OVERLAY="$ROOT/deploy/kubernetes/overlays/kind"
  [ "$REPLICAS" = 1 ] || OVERLAY="$HERE/no-replicas"
  if [ "$AUTH" = 1 ]; then
    OVERLAY="$HERE/auth"
    k create secret generic speedvault-auth --from-literal=password="$PASSWORD" --dry-run=client -o yaml | k apply -f - >/dev/null
  fi
  k apply -k "$OVERLAY" >/dev/null
  k apply -k "$ROOT/deploy/kubernetes/overlays/autoscaling" --dry-run=server >/dev/null
  say "the autoscaling overlay passes a server-side dry run"
fi

nodes_ready() {  # nodes_ready N: the StatefulSet has N pods, all ready
  local want="$1"
  for _ in $(seq 300); do
    local got
    got=$(k get statefulset speedvault -o jsonpath='{.status.replicas} {.status.readyReplicas} {.status.updatedReplicas} {.status.currentRevision} {.status.updateRevision}')
    set -- $got
    if [ "${1:-}" = "$want" ] && [ "${2:-}" = "$want" ] && [ "${3:-}" = "$want" ] && [ "${4:-}" = "${5:-}" ]; then return 0; fi
    sleep 2
  done
  echo "the StatefulSet didn't get to $want ready pods: $got" >&2
  return 1
}

start_checker() {  # start_checker N: a checker pod writing, N nodes even first
  kc delete pod checker --ignore-not-found --wait=true >/dev/null
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: checker
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
  containers:
    - name: checker
      image: speedvault-checker:dev
      imagePullPolicy: Never
      args: ["run", "--nodes", "$1"$([ -n "$LETTUCE" ] && echo ", \"--lettuce\", \"$LETTUCE\"")]
      env:
        - name: SEED
          value: speedvault-seed.$NS.svc
        - name: STUART
          value: stuart.$NS.svc
        - name: REPLICAS
          value: "$REPLICAS"
        - name: SPEEDVAULT_PASSWORD
          value: "${PASSWORD:-}"
EOF
  for _ in $(seq 400); do
    if kc logs checker 2>/dev/null | grep -qx writing; then return 0; fi
    case "$(kc get pod checker -o jsonpath='{.status.phase}')" in Failed|Succeeded) break;; esac
    sleep 1
  done
  kc logs checker >&2 || true
  echo "the checker didn't start writing" >&2
  return 1
}

stop_checker() {  # stop_checker N: the checker's verdict once N nodes are even
  kc exec checker -- "${AUTH_ENV[@]}" python3 /app/k8s/kcheck.py stop "$1"
  local phase=""
  for _ in $(seq 400); do
    phase=$(kc get pod checker -o jsonpath='{.status.phase}')
    case "$phase" in Failed|Succeeded) break;; esac
    sleep 2
  done
  kc logs checker | sed -n '/^writing$/,$p' | sed 's/^/    /'
  [ "$phase" = Succeeded ]
}

check() {  # check ARGS...: kcheck.py ARGS in the running checker, its output indented
  kc exec checker -- "${AUTH_ENV[@]}" env REPLICAS="$REPLICAS" python3 /app/k8s/kcheck.py "$@" | sed 's/^/    /'
  return "${PIPESTATUS[0]}"
}

counts() {  # stuart's counters: "slots_moved=N handovers=N demotions=N"
  kc exec checker -- "${AUTH_ENV[@]}" python3 /app/k8s/kcheck.py counts
}

count() {  # count NAME COUNTS: NAME's value in COUNTS
  echo "$2" | tr ' ' '\n' | sed -n "s/^$1=//p"
}

gone() {  # gone FROM TO: the pods speedvault-FROM .. speedvault-(TO-1), as names
  local i; for i in $(seq "$1" $(($2 - 1))); do echo "pod/speedvault-$i"; done
}

ready_node() {  # ready_node NAME: the kind node's Ready condition (True, False, Unknown)
  "$KUBECTL" --context "kind-$CLUSTER" get node "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
}

STOPPED=""
restore_worker() {  # the worker worker-stop stopped: started again, untainted
  [ -n "$STOPPED" ] || return 0
  docker start "$STOPPED" >/dev/null || true
  for _ in $(seq 90); do [ "$(ready_node "$STOPPED")" = True ] && break; sleep 2; done
  "$KUBECTL" --context "kind-$CLUSTER" taint node "$STOPPED" node.kubernetes.io/out-of-service- >/dev/null 2>&1 || true
  say "  $STOPPED started again: Ready $(ready_node "$STOPPED")"
  STOPPED=""
}

stuart_nodes() {  # once stuart answers through its Service (it may have just restarted)
  for _ in $(seq 30); do
    kc exec checker -- "${AUTH_ENV[@]}" redis-cli -h "stuart.$NS.svc" -p 7600 STUART NODES 2>/dev/null | grep . && return 0
    sleep 1
  done
  echo "stuart doesn't answer, or knows no nodes" >&2
  return 1
}

# The autoscaling scenarios.

hpa_metrics() {  # the HPA's view: "speedvault_busy_cores=812m speedvault_memory_used_ratio=402m"
  k get hpa speedvault -o jsonpath='{range .status.currentMetrics[*]}{.pods.metric.name}={.pods.current.averageValue} {end}'
}

milli() {  # milli 812m -> 812, milli 1 -> 1000
  case "$1" in *m) echo "${1%m}";; *k) echo $((${1%k} * 1000000));; *) echo $(($1 * 1000));; esac
}

wait_metrics() {  # wait_metrics MAX: the adapter serves both metrics, each average below MAX (milli)
  local m=""
  for _ in $(seq 120); do
    m=$(hpa_metrics)
    local ok=1 n=0
    for kv in $m; do
      # "<unknown>" or nothing until the adapter has a value
      [[ "${kv#*=}" =~ ^[0-9]+[mk]?$ ]] || continue
      n=$((n + 1))
      [ "$(milli "${kv#*=}")" -lt "$1" ] || ok=0
    done
    [ "$n" = 2 ] && [ "$ok" = 1 ] && return 0
    sleep 3
  done
  echo "the HPA's metrics didn't get below ${1}m: $m" >&2
  k describe hpa speedvault >&2 || true
  return 1
}

TIMELINE=/dev/null
watch_scale() {  # watch_scale N T0: the HPA asks for N, they're ready; what happens when, from T0
  local want="$1" t0="$2" last="" asked=""
  for _ in $(seq 600); do
    local now got
    now=$(($(date +%s) - t0))
    got=$(k get hpa speedvault -o jsonpath='{.status.desiredReplicas}')/$(k get statefulset speedvault -o jsonpath='{.status.replicas}/{.status.readyReplicas}')
    if [ "$got" != "$last" ]; then
      # desired/pods/ready
      say "  +${now}s HPA wants ${got%%/*}, pods ${got#*/} ($(hpa_metrics))" | tee -a "$TIMELINE"
      last="$got"
    fi
    if [ -z "$asked" ] && [ "${got%%/*}" = "$want" ]; then asked=$now; fi
    if [ "$got" = "$want/$want/$want" ]; then
      say "  +${now}s $want pods ready (the HPA asked at +${asked}s)" | tee -a "$TIMELINE"
      return 0
    fi
    sleep 1
  done
  echo "the HPA didn't get to $want ready pods: $got" >&2
  k describe hpa speedvault >&2 || true
  return 1
}

even() {  # even N T0: N nodes hold even shares, nothing moves
  kc exec checker -- "${AUTH_ENV[@]}" python3 /app/k8s/kcheck.py even "$1" >/dev/null
  say "  +$(($(date +%s) - $2))s $1 nodes hold even shares of the slots" | tee -a "$TIMELINE"
}

hold() {  # hold N SECONDS: the StatefulSet stays at N meanwhile (the HPA adds none)
  # Its replicas, not the HPA's desiredReplicas: after a scale-down by hand
  # that still has the old count until the HPA's next sync.
  for _ in $(seq "$2"); do
    local r; r=$(k get statefulset speedvault -o jsonpath='{.spec.replicas}')
    [ "$r" -le "$1" ] || { echo "the HPA scaled to $r pods ($(hpa_metrics))" >&2; return 1; }
    sleep 1
  done
  say "  the HPA added no pod in $2 s ($(hpa_metrics))" | tee -a "$TIMELINE"
}

ensure_nodes() {  # ensure_nodes N: N ready pods, scaled by hand if needed
  local got; got=$(k get statefulset speedvault -o jsonpath='{.spec.replicas}')
  [ "$got" = "$1" ] || k scale statefulset speedvault --replicas "$1" >/dev/null
  nodes_ready "$1"
}

start_load() {  # memtier_benchmark through the seed Service, LOAD_TIME seconds; JSON on its stdout
  kc delete pod load --ignore-not-found --wait=true >/dev/null
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: load
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
  containers:
    - name: load
      image: speedvault-checker:dev
      imagePullPolicy: Never
      command: ["sh", "-c"]
      args:
        - >-
          memtier_benchmark --cluster-mode -s speedvault-seed.$NS.svc -p 7000
          -t $LOAD_THREADS -c $LOAD_CLIENTS --rate-limiting $LOAD_RATE --test-time $LOAD_TIME
          --ratio 1:4 -d 32 --key-maximum 100000 --key-pattern R:R --distinct-client-seed
          --hide-histogram --print-percentiles 50,99,99.9 --json-out-file /tmp/load.json
          >/tmp/load.out 2>/tmp/load.err; rc=\$?;
          tail -c 2000 /tmp/load.err >&2; grep -E 'Totals|Gets|Sets' /tmp/load.out >&2;
          cat /tmp/load.json; exit \$rc
EOF
  kc wait --for=condition=Ready pod/load --timeout 60s >/dev/null
}

load_result() {  # waits for memtier to finish; its seconds summarized, against T0
  local phase=""
  for _ in $(seq $((LOAD_TIME + 120))); do
    phase=$(kc get pod load -o jsonpath='{.status.phase}')
    case "$phase" in Failed|Succeeded) break;; esac
    sleep 1
  done
  kc logs load > "${OUT:-/tmp}/load.json.tmp" 2>/dev/null || true
  # The JSON starts at its first "{"; memtier's own lines come before it.
  sed -n '/^{/,$p' "${OUT:-/tmp}/load.json.tmp" > "${OUT:-/tmp}/load-$1.json"
  sed '/^{/,$d' "${OUT:-/tmp}/load.json.tmp" | sed 's/^/    /'
  rm -f "${OUT:-/tmp}/load.json.tmp"
  kc exec -i checker -- "${AUTH_ENV[@]}" python3 /app/k8s/kcheck.py load < "${OUT:-/tmp}/load-$1.json" | sed 's/^/    /' | tee -a "$TIMELINE"
  [ "$phase" = Succeeded ] || { echo "memtier_benchmark failed ($phase)" >&2; return 1; }
}

scenario() {
  local name="$1" t0
  [ -z "$OUT" ] || TIMELINE="$OUT/timeline-$name.txt"
  t0=$(date +%s)
  say "== $name"
  case "$name" in
    form)
      nodes_ready $BASE
      start_checker $BASE
      stuart_nodes | sed 's/^/    /'
      # Each replica on another kind node than its primary.
      [ "$REPLICAS" = 0 ] || check pairs
      stop_checker $BASE;;
    scale-up)
      start_checker $BASE
      k scale statefulset speedvault --replicas $UP >/dev/null
      nodes_ready $UP
      say "$UP pods ready after $(($(date +%s) - t0)) s"
      stop_checker $UP;;
    scale-down)
      k scale statefulset speedvault --replicas $UP >/dev/null  # if scale-up didn't run
      nodes_ready $UP
      start_checker $UP
      local t1; t1=$(date +%s)
      k scale statefulset speedvault --replicas $BASE >/dev/null
      nodes_ready $BASE
      k wait --for=delete $(gone $BASE $UP) --timeout 60s >/dev/null 2>&1 || true
      local took=$(($(date +%s) - t1))
      say "scaled down in $took s"
      # Well within the grace period: the nodes drained and exited by
      # themselves, not killed at its end.
      [ "$took" -lt 300 ] || { echo "the scale-down took $took s" >&2; return 1; }
      stop_checker $BASE;;
    rollout)
      nodes_ready $BASE
      start_checker $BASE
      local before; before=$(counts)
      k rollout restart statefulset speedvault >/dev/null
      k rollout status statefulset speedvault --timeout 15m >/dev/null
      nodes_ready $BASE
      say "rolled out in $(($(date +%s) - t0)) s"
      local after; after=$(counts)
      stop_checker $BASE
      if [ "$REPLICAS" = 1 ]; then
        # Every primary handed over: no slot moved.
        local moved handed
        moved=$(($(count slots_moved "$after") - $(count slots_moved "$before")))
        handed=$(($(count handovers "$after") - $(count handovers "$before")))
        say "  $moved slots moved, $handed handovers"
        [ "$moved" = 0 ] && [ "$handed" -ge 3 ] || { echo "a rolling update moved $moved slots, $handed handovers" >&2; return 1; }
      fi;;
    delete)
      nodes_ready $BASE
      start_checker $BASE
      local pod=speedvault-1 before
      [ "$REPLICAS" = 0 ] || pod=$(kc exec checker -- "${AUTH_ENV[@]}" python3 /app/k8s/kcheck.py primary)
      before=$(counts)
      local uid; uid=$(k get pod "$pod" -o jsonpath='{.metadata.uid}')
      say "  deleting $pod"
      k delete pod "$pod" >/dev/null
      wait_new_pod "$pod" "$uid"
      nodes_ready $BASE
      local after; after=$(counts)
      stop_checker $BASE
      if [ "$REPLICAS" = 1 ]; then
        # The primary handed over: no slot moved.
        local moved handed
        moved=$(($(count slots_moved "$after") - $(count slots_moved "$before")))
        handed=$(($(count handovers "$after") - $(count handovers "$before")))
        say "  $moved slots moved, $handed handovers"
        [ "$moved" = 0 ] && [ "$handed" = 1 ] || { echo "deleting a primary moved $moved slots, $handed handovers" >&2; return 1; }
      fi;;
    force)
      nodes_ready $BASE
      start_checker $BASE
      local pod=speedvault-1
      [ "$REPLICAS" = 0 ] || pod=$(kc exec checker -- "${AUTH_ENV[@]}" python3 /app/k8s/kcheck.py primary)
      check lossy "$pod"
      local uid; uid=$(k get pod "$pod" -o jsonpath='{.metadata.uid}')
      k delete pod "$pod" --grace-period=0 --force >/dev/null 2>&1
      wait_new_pod "$pod" "$uid"
      nodes_ready $BASE
      stop_checker $BASE;;
    worker-stop)
      nodes_ready $BASE
      start_checker $BASE
      # A worker with nodes but neither stuart nor the checker (stopped,
      # stuart couldn't fail over; the checker would stop checking): the
      # one with the most primaries.
      local spared worker="" best=-1 w
      spared="$(k get pod stuart-0 -o jsonpath='{.spec.nodeName}') $(kc get pod checker -o jsonpath='{.spec.nodeName}')"
      for w in $(k get pods -l app.kubernetes.io/name=speedvault -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u); do
        case " $spared " in *" $w "*) continue;; esac
        local n; n=$(stuart_nodes | grep " zone=$w" | grep -vc "replica-of" || true)
        [ "$n" -gt "$best" ] && { best=$n; worker=$w; }
      done
      [ -n "$worker" ] || { echo "no worker without stuart and the checker" >&2; return 1; }
      check lossy-zone "$worker"
      [ "$REPLICAS" = 0 ] || [ "$best" -gt 0 ] || echo "    (no primary on $worker: nothing to fail over)"
      local t1; t1=$(date +%s)
      # Killed: a machine that stops (a graceful `docker stop` would
      # SIGTERM the pods, and their primaries hand over).
      docker kill "$worker" >/dev/null
      STOPPED=$worker
      say "  $worker killed"
      if [ "$REPLICAS" = 1 ]; then
        check failover "$worker"
      fi
      for _ in $(seq 90); do [ "$(ready_node "$worker")" != True ] && break; sleep 2; done
      say "  +$(($(date +%s) - t1))s $worker $(ready_node "$worker" | sed 's/Unknown/not ready (Unknown)/;s/False/not ready/')"
      # Its pods stay (Terminating once evicted) until the node is known
      # to be down: then they're deleted, and start elsewhere.
      "$KUBECTL" --context "kind-$CLUSTER" taint node "$worker" node.kubernetes.io/out-of-service=nodeshutdown:NoExecute >/dev/null
      nodes_ready $BASE
      say "  +$(($(date +%s) - t1))s $BASE pods ready again, on $(k get pods -l app.kubernetes.io/name=speedvault -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | tr '\n' ' ')"
      [ "$REPLICAS" = 0 ] || check pairs || true
      stop_checker $BASE
      restore_worker;;
    stuart)
      nodes_ready $BASE
      start_checker $BASE
      k delete pod stuart-0 >/dev/null
      k wait --for=condition=Ready pod/stuart-0 --timeout 120s >/dev/null
      stuart_nodes | sed 's/^/    /'
      stop_checker $BASE;;
    delete-all)
      nodes_ready $BASE
      kc delete pod checker --ignore-not-found --wait=true >/dev/null
      k delete -k "$OVERLAY" --wait=true --timeout 90s >/dev/null
      k wait --for=delete pod --all --timeout 90s >/dev/null 2>&1 || true
      local took=$(($(date +%s) - t0))
      say "deleted in $took s"
      if [ -n "$(k get pods -o name)" ] || [ "$took" -ge 60 ]; then
        echo "deleting took $took s" >&2
        k get pods >&2
        return 1
      fi
      k delete pvc --all --wait=true >/dev/null
      k apply -k "$OVERLAY" >/dev/null
      nodes_ready $BASE;;
    teardown)
      nodes_ready $BASE
      kc delete pod checker --ignore-not-found --wait=true >/dev/null
      local t1; t1=$(date +%s)
      "$KUBECTL" --context "kind-$CLUSTER" delete namespace "$NS" --wait=false >/dev/null
      # The pods: gone within a minute, not at the end of the grace period.
      for _ in $(seq 60); do [ -z "$(k get pods -o name 2>/dev/null)" ] && break; sleep 1; done
      local took=$(($(date +%s) - t1))
      say "every pod gone in $took s"
      [ "$took" -lt 60 ] || { echo "the pods took $took s to go" >&2; k get pods >&2; return 1; }
      # The namespace: Kubernetes 1.33 deletes a namespace's pods first and
      # the rest once they're gone, a minute or two later.
      "$KUBECTL" --context "kind-$CLUSTER" wait --for=delete namespace "$NS" --timeout 5m >/dev/null
      say "the namespace deleted in $(($(date +%s) - t1)) s";;
    hpa-memory)
      ensure_nodes $BASE
      wait_metrics 300
      start_checker $BASE
      local t1; t1=$(date +%s)
      check fill $FILL
      say "  +$(($(date +%s) - t1))s filled" | tee -a "$TIMELINE"
      watch_scale $MEM_TO "$t1"
      even $MEM_TO "$t1"
      hold $MEM_TO 60
      stop_checker $MEM_TO;;
    hpa-cpu)
      # The FLUSHALL of the checker's start empties the nodes first (else
      # hpa-memory's keys, on fewer nodes, would make the HPA add some).
      local cur; cur=$(k get statefulset speedvault -o jsonpath='{.spec.replicas}')
      nodes_ready "$cur"
      start_checker "$cur"
      ensure_nodes $CPU_FROM
      even $CPU_FROM "$t0"
      wait_metrics 300
      start_load
      local t1; t1=$(date +%s)
      say "  load: memtier_benchmark, $((LOAD_THREADS * LOAD_CLIENTS)) clients at $LOAD_RATE requests a second to each primary, for $LOAD_TIME s"
      watch_scale $CPU_TO "$t1"
      even $CPU_TO "$t1"
      load_result cpu
      stop_checker $CPU_TO;;
    hpa-down)
      # Scaling down moves keys: only by hand. (A test would wait the HPA's
      # 5 minute scale-down window to see it happen.)
      [ "$(k get hpa speedvault -o jsonpath='{.spec.behavior.scaleDown.selectPolicy}')" = Disabled ] \
        || { echo "the HPA may scale down by itself" >&2; return 1; }
      ensure_nodes $MAX
      wait_metrics 300
      start_checker $MAX
      local t1; t1=$(date +%s)
      k scale statefulset speedvault --replicas $BASE >/dev/null
      nodes_ready $BASE
      k wait --for=delete $(gone $BASE $MAX) --timeout 120s >/dev/null 2>&1 || true
      say "  scaled down in $(($(date +%s) - t1)) s" | tee -a "$TIMELINE"
      hold $BASE 45
      stop_checker $BASE;;
    *) echo "unknown scenario $name" >&2; return 2;;
  esac
}

wait_new_pod() {  # wait_new_pod NAME OLD-UID: NAME is back, a new pod
  for _ in $(seq 120); do
    local uid
    uid=$(k get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    [ -n "$uid" ] && [ "$uid" != "$2" ] && return 0
    sleep 1
  done
  echo "$1 didn't come back" >&2
  return 1
}

for s in "${SCENARIOS[@]}"; do
  start=$(date +%s)
  # In a subshell outside any condition, so that set -e stops it at the
  # first failing step.
  set +e
  (set -e; scenario "$s")
  rc=$?
  set -e
  if [ "$rc" = 0 ]; then
    say "ok   $s ($(($(date +%s) - start)) s)"
  else
    say "FAIL $s"
    dump
    restore_worker
    FAILED+=("$s")
  fi
done
kc delete pod checker --ignore-not-found --wait=false >/dev/null 2>&1 || true
if [ ${#FAILED[@]} -gt 0 ]; then
  say "FAILED: ${FAILED[*]}"
  exit 1
fi
say "OK"
