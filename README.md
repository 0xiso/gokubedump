# gokubedump

A **dramatically faster** Kubernetes manifest dumper, written in Go.

`gokubedump` walks every list-able resource in your cluster — including CRDs —
and writes each object as a clean YAML file, organized by namespace, with
volatile/cluster-specific fields stripped so the output is suitable for
re-applying or diffing across clusters.

## Why it's fast

Most existing manifest-dump tools are shell scripts that loop `kubectl get`
serially per resource per namespace. They're correct, but slow — a real
cluster can take **tens of minutes**. `gokubedump` finishes the same work in
**seconds to a minute** because of how it's built:

| Optimization | What other tools do | What `gokubedump` does |
| --- | --- | --- |
| Concurrency | shell loop, 1 at a time, or ~10 workers | **32 parallel workers** by default (tunable via `-p`) |
| Discovery | `kubectl api-resources` per call | **single** `ServerPreferredResources()` discovery |
| Listing | spawn `kubectl` subprocess per resource | **dynamic client** — one process, persistent HTTP/2 connections |
| Throttling | hits client-side QPS limit (5/10) | QPS=1000, Burst=2000 — pinned by the API server, not the client |
| Events | dumped (huge, useless for re-apply) | **skipped** |
| Archive | external `tar`/`gzip` pass | **streamed** in-process to `.tar.gz` |

The combination matters: lifting QPS without parallelism doesn't help, and
adding workers without lifting QPS just queues them up against the default
client throttle. Doing both, plus avoiding `kubectl` subprocess fork-exec on
every resource, is what gets you the order-of-magnitude difference.

## Build

```sh
make build           # local binary
make build-linux     # static linux/amd64 binary
```

## Usage

```sh
./gokubedump [flags]
```

| Flag | Default | Description |
| --- | --- | --- |
| `-kubeconfig` | `~/.kube/config` | path to kubeconfig |
| `-out` | `dump` | output directory (must not exist or be empty) |
| `-archive-to` | _(unset)_ | if set, tar.gz the dump into this directory and remove the source |
| `-parent-only` | `false` | skip resources that have `ownerReferences` (i.e. dump only top-level objects) |
| `-p` | `32` | parallel workers |

Example:

```sh
./gokubedump -out mycluster -archive-to backups -parent-only
```

`gokubedump` refuses to write into a non-empty `-out` directory, so a dump never
contains leftovers from an earlier run. With `-archive-to` the directory is
removed after archiving, so repeated runs work without cleanup.

If anything could not be dumped (an API group failed discovery, a list was
forbidden, a file could not be written, ...), everything that did succeed is
still written and archived, but the process exits with status `1` and logs the
number of errors, so scripts and cron jobs can detect an incomplete dump.

## Output

```
dump/
├── _cluster/                     # cluster-scoped resources
│   ├── nodes_node-1.yaml
│   ├── customresourcedefinitions_foo.example.com.yaml
│   └── ...
├── kube-system/
│   ├── deployments_coredns.yaml
│   ├── services_kube-dns.yaml
│   └── ...
└── default/
    └── ...
```

Files are named `<resource>_<name>.yaml`. When the same resource name exists in
more than one API group (for example core `nodes` and `metrics.k8s.io` `nodes`),
the non-core ones are written as `<resource>.<group>_<name>.yaml` so they do not
overwrite each other.

Each file is the original object minus:

- `metadata`: `creationTimestamp`, `deletionGracePeriodSeconds`,
  `deletionTimestamp`, `finalizers`, `generation`, `managedFields`,
  `ownerReferences`, `resourceVersion`, `selfLink`, `uid`
- `status`
- `metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]`
- Service: `clusterIP`, `clusterIPs` (kept when `None`, so headless Services
  stay headless), `ipFamilies`, `ipFamilyPolicy`, `healthCheckNodePort`,
  per-port `nodePort`
- ServiceAccount: `secrets`
- PersistentVolumeClaim: `spec.volumeName`
- PersistentVolume: `spec.claimRef`
- Node: `spec.podCIDR`, `spec.podCIDRs`, `spec.providerID`

The result re-applies cleanly into another cluster.

## License

MIT
