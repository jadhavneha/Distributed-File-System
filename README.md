# HyDFS: A Distributed File System with Integrated Stream Processing

HyDFS is a fault-tolerant, distributed file system implemented in Go, inspired by the design principles of systems such as HDFS and Dynamo. It supports consistent hashing–based file placement, replication, failure detection, automatic re-replication, and an integrated stream-processing framework called **RainStorm** that executes multi-stage dataflow jobs directly on top of the file system.

This project was built as a full end-to-end distributed systems implementation, emphasizing correctness, robustness under failures, and practical deployability across multiple machines.

---

## High-Level Architecture

HyDFS is composed of three tightly integrated layers:

1. **Membership & Failure Detection Layer**
2. **Distributed File System (HyDFS Core)**
3. **RainStorm Stream Processing Engine**

Each node runs the same binary and can dynamically join or leave the system.

---

## 1. Membership & Failure Detection

### Node Identity
Each node is uniquely identified by:
<IP_ADDRESS>@<TIMESTAMP>

This ensures uniqueness even across node restarts.

### Leader / Introduer
- A fixed **leader node** (configured via `LEADER_SERVER_HOST`) acts as the introducer.
- New nodes join by sending a `JOIN` request to the leader.
- The leader replies with the full membership list.

### Failure Detection
- Nodes periodically send **PING** messages to peers.
- Failures are detected using TCP timeouts.
- Failure notifications are **piggybacked** onto heartbeat messages to reduce control traffic.
- When a node is suspected failed:
  - It is removed from the membership list
  - Re-replication is triggered immediately

### Piggybacking
Control messages such as `HELLO`, `FAIL`, and `LEAVE` are piggybacked onto heartbeat ACKs with a bounded TTL to ensure eventual dissemination.

---

## 2. Consistent Hashing & Ring Management

HyDFS uses **consistent hashing** to place both nodes and files on a fixed-size ring.

- Ring size: `1024` points
- Hash function: FNV-1a
- Each node occupies a single ring position
- Files are mapped to the first node clockwise from their hash position

### Replica Placement
- Replication factor: **3**
- One **primary replica**
- Two successor replicas on the ring

This design ensures:
- Minimal data movement on node joins/failures
- Deterministic replica selection
- Even load distribution

---

## 3. HyDFS File System

### File Model
- Files are split into **append-only blocks**
- Each block is immutable once written
- Metadata is maintained in memory and persisted on disk

### Supported Operations
| Operation | Description |
|---------|------------|
| `create` | Create a new distributed file |
| `append` | Append a local file as a new block |
| `multiappend` | Concurrent append from multiple nodes |
| `get` | Retrieve and reconstruct a file |
| `merge` | Consolidate and propagate latest version |
| `delete` | Remove file replicas |
| `ls` | Show file replica locations |

### Primary-Based Writes
- All writes are routed to the **primary replica**
- The primary assigns block IDs
- Blocks are synchronously replicated to successors

### Temporary Files
During reads from remote replicas, data is stored in **temporary namespaces** to avoid corrupting existing state until all blocks are received.

---

## 4. Failure Handling & Re-Replication

When a node fails:
1. The node is removed from the membership list
2. Ring state is rebuilt
3. Files for which the failed node was a replica are identified
4. Missing replicas are reconstructed automatically
5. Bandwidth and duration metrics are recorded

This process is fully automatic and requires no operator intervention.

---

## 5. RainStorm: Integrated Stream Processing

RainStorm is a distributed stream-processing engine built directly on top of HyDFS.

### Key Features
- Multi-stage dataflow execution
- Exactly-once processing semantics (optional)
- Stateful operators
- Failure recovery with log replay
- Dynamic autoscaling
- Built-in metrics reporting

### Execution Model
- Jobs consist of **stages**
- Each stage has multiple **tasks**
- Tasks are distributed across the ring
- Tuples flow stage-by-stage between tasks

### Supported Operators
- `identity`
- `grep`
- `replace`
- `tokenize`
- `csv_first3`
- `filter_csv`
- `aggregateByKeys`
- `geo_grid`

Operators can be chained arbitrarily to form pipelines.

---

## 6. Exactly-Once Semantics

When enabled:
- Each tuple is assigned a globally unique ID
- Tasks maintain:
  - Processed set
  - Pending acknowledgments
- Logs are written to HyDFS
- On failure, tasks replay logs and resume from a consistent state

This ensures correctness even under node crashes and message reordering.

---

## 7. Autoscaling

RainStorm supports **dynamic task scaling**:
- The leader monitors per-stage throughput
- If throughput exceeds `HW`, tasks are added
- If throughput drops below `LW`, tasks are removed
- Task assignments are updated consistently across the cluster

Autoscaling is disabled when exactly-once semantics are enabled (to preserve determinism).

---

## 8. Command-Line Interface

Each node provides an interactive shell.

### Membership & Debugging
```bash
list_mem
list_ring
list_self
piggybacks
leave
```
### File System Commands
```bash
create <local> <hydfs>
append <local> <hydfs>
multiappend <hydfs> <vm> <local> ...
get <hydfs> <local>
merge <hydfs>
ls <hydfs>
liststore
```

### Rainstrom Commands
```bash
rainstorm <Nstages> <Ntasks> <op1> ... <opN> <src> <dest> <exactly_once> <autoscale> <rate> <LW> <HW>
list_tasks
kill_task <stage> <index> [jobID]
word-count <tasks> <src> <dest>
```


