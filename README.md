# fair-job-queue

A durable, Postgres-backed background job queue with weighted fair queuing
across tenants, so one tenant's job volume can't starve everybody else's.

## Problem

A shared job queue ordered by naive FIFO or a static priority column has an
obvious failure mode: whichever tenant enqueues the most jobs (or the
"highest priority" ones) monopolizes the workers, and everyone else waits
behind them indefinitely. That's fine until you have multiple tenants with
wildly different volumes sharing infrastructure -- a bulk-import tenant
enqueuing 40,000 jobs shouldn't mean a low-volume tenant's single job waits
an hour.

This queue schedules by **weighted fair queuing**: every job gets a virtual
finish time derived from its cost and its tenant's weight, and the queue
always serves the globally-earliest virtual finish time. A tenant's *volume*
doesn't buy it more service than its *weight* entitles it to.

## Architecture

```
enqueue(tenant, cost) --> [ Postgres: jobs, tenants, queue_state ] <-- claim()/ack()/fail()
                                        ^
                    FOR UPDATE SKIP LOCKED, ORDER BY virtual_finish
                                        |
                         +--------------+--------------+
                         |                              |
                    NestJS WorkerService          standalone worker
                    (poll loop, DI)               (run-worker.ts, used
                                                    by crash-recovery test)
```

The queue logic lives in `src/queue/queue-core.ts` (`QueueCore`) as a
plain TypeScript class with zero Nest dependencies -- it only needs a `pg`
`Pool`. `src/queue/queue.service.ts` is a thin `@Injectable()` wrapper that
gives it a Nest DI lifecycle (pool creation/teardown) and exposes it via
`QueueController` (`POST /queue/enqueue`, `GET /queue/stats`, etc). The
worker side follows the same split: `WorkerRunner` (framework-agnostic poll
loop) vs `WorkerService` (Nest wrapper that registers a handler and starts
it). This means the scheduling and durability logic is portable to any
Node context (a CLI, a cron job, a non-Nest service) -- Nest is purely the
delivery mechanism the spec asked for, not load-bearing for correctness.

### Scheduling: Self-Clocked Fair Queuing (SCFQ)

The spec calls for "a virtual finish time per job from job cost and tenant
weight; the queue always dequeues the globally-earliest virtual finish
time" -- i.e. a WFQ family algorithm, not round-robin-per-tenant. I
implemented **Self-Clocked Fair Queuing** (Golestani, *"A Self-Clocked Fair
Queueing Scheme for Broadband Applications"*, INFOCOM 1994), adapted from a
single-server packet scheduler to a multi-worker Postgres job queue:

```
virtual_start(job)  = max(tenant.last_finish, queue_state.virtual_time)
virtual_finish(job) = virtual_start(job) + cost / tenant.weight
```

- `tenant.last_finish` is the finish tag of that tenant's previously
  enqueued job (0 if it has none in flight) -- this is what makes a
  higher-weight tenant's jobs accrue finish time more slowly, and what lets
  a tenant that's been quiet "catch up" to roughly where the queue
  currently is instead of keeping ancient, arbitrarily-low finish tags.
- `queue_state.virtual_time` is the *system* virtual time. The original SCFQ
  defines it as "the finish tag of the packet **currently in service**" on
  a single server -- self-clocked, because the server's own progress
  defines the clock instead of needing a hypothetical GPS (Generalized
  Processor Sharing) simulation like classic WFQ requires. With multiple
  concurrent Postgres workers there's no single "packet in service," so
  this implementation advances `queue_state.virtual_time` to
  `GREATEST(virtual_time, claimed_job.virtual_finish)` every time *any*
  worker claims a job -- the max finish tag among jobs currently being
  serviced. This is the direct multi-server generalization of "self-clock
  off what's in service."
- Dequeue is always `ORDER BY virtual_finish ASC, id ASC FOR UPDATE SKIP
  LOCKED LIMIT 1` -- the `id ASC` tiebreak makes ordering deterministic
  when two jobs land on the exact same virtual finish time.

I picked SCFQ over classic WFQ/PGPS because WFQ requires simulating a
hypothetical fluid-flow GPS server to compute system virtual time, which is
extra bookkeeping this queue doesn't need; SCFQ gets the same fairness
bound (bounded lag from ideal GPS service) from state you already have
(the finish tag of what's being served). I picked it over Start-time Fair
Queuing (SFQ, Goyal et al.) mainly for the same reason the spec favors it
explicitly ("a virtual finish time... dequeues the globally-earliest
virtual finish time") -- SFQ schedules by *start* tag, which fits a
different DoD than the one given here.

**Rejected: round-robin-per-tenant.** Explicitly excluded by the spec, and
for good reason -- round robin ignores job *cost*, so a tenant submitting
expensive jobs gets the same service slots as one submitting cheap ones,
which isn't actually fair in any cost-weighted sense.

**Rejected: a second stateful system (Redis sorted set, SQS, etc) for
scheduling order.** Postgres already has to be the durability layer (jobs
must survive a crash), and `FOR UPDATE SKIP LOCKED` gives safe concurrent
dequeue without a second moving part to keep consistent with the source of
truth. The tradeoff is throughput ceiling -- see below.

**Known scaling tradeoff: `queue_state` and each tenant's cursor row are
locked (`FOR UPDATE`) during every enqueue,** which serializes virtual-time
assignment across all enqueuers. This is the one intentionally serialized
path in the system. It's necessary because SCFQ's fairness guarantee
depends on a total order over virtual-time assignment -- two concurrent
enqueues for the same tenant must not race on `last_finish`. For the
throughput this queue targets (a background job system, not a
packet-per-microsecond router) this lock is held for microseconds per
enqueue and isn't the bottleneck; a sharded/per-tenant-partitioned
`queue_state` would remove the *cross-tenant* contention if that ever
became a problem, at the cost of losing a single global fairness clock.

### Visibility timeout (crash recovery)

`claim()` sets `visible_at = now() + visibility_timeout` and `status =
'in_progress'`. The dequeue query's `WHERE` clause is:

```sql
status = 'pending' OR (status = 'in_progress' AND visible_at <= now())
```

so a job whose worker crashed before acking becomes claimable again once
its timeout elapses -- no separate heartbeat/lease-renewal process, no
second system tracking liveness. `ack()` and `fail()` both check
`locked_by = $workerId`, so a worker that got its job reclaimed out from
under it (e.g. it was merely slow, not actually dead, and comes back to
life) cannot silently complete a job someone else is now processing.

**Rejected: fire-and-forget claim (no timeout, no reclaim).** This is
explicitly what the spec asks to avoid -- a crashed worker would silently
lose the job forever with no automatic recovery.

### Dead-letter / poison jobs

Each `claim()` increments `attempts`. `fail()` appends a
`{attempt, error, failedAt}` record to `failure_history` (JSONB) and moves
the job to `dead_letter` once `attempts >= max_attempts`; otherwise it goes
back to `pending` immediately, **at its original `virtual_finish`** -- a
retry doesn't jump the fairness queue ahead of jobs that are actually
making progress, but it also doesn't lose its place the way appending it
as a brand-new job at the back would. Dead-lettered jobs are excluded from
the dequeue `WHERE` clause entirely and never resurface.

## Setup (from a clean clone)

Requires local Postgres binaries (`initdb`, `pg_ctl`, `psql`) on `PATH` --
this project stands up its own throwaway cluster, it does not use your
system Postgres.

```bash
npm install

# Initializes and starts a dedicated local Postgres cluster in .pgdata/,
# listening on a unix socket only (no TCP, so it can't collide with any
# other Postgres on port 5432), and creates the fair_job_queue database.
npm run db:start
# prints: DATABASE_URL=postgres://postgres@localhost/fair_job_queue?host=/abs/path/.pgdata/socket

export DATABASE_URL=postgres://postgres@localhost/fair_job_queue?host=$(pwd)/.pgdata/socket
npm run migrate

npm test
```

To stop the cluster: `npm run db:stop`. All cluster state lives under
`.pgdata/` (gitignored) -- delete that directory for a fully clean slate.

## Usage

```ts
import { createPool } from './src/queue/db';
import { QueueCore } from './src/queue/queue-core';

const core = new QueueCore(createPool(), { visibilityTimeoutSeconds: 30 });

await core.setTenantWeight('acme', 2);      // acme gets 2x the fair share of 'globex'
await core.setTenantWeight('globex', 1);

await core.enqueue({ tenantId: 'acme', jobType: 'send-email', cost: 1 });
await core.enqueue({ tenantId: 'globex', jobType: 'render-pdf', cost: 5, maxAttempts: 3 });

const job = await core.claim('worker-1');
if (job) {
  try {
    // ... do the work ...
    await core.ack(job.id, 'worker-1');
  } catch (err) {
    await core.fail(job.id, 'worker-1', String(err)); // retries, or dead-letters after maxAttempts
  }
}
```

Or via the NestJS HTTP API (`npm run start`, default port 3100):

```bash
$ curl -s -X POST localhost:3100/queue/enqueue -H 'content-type: application/json' \
    -d '{"tenantId":"acme","jobType":"send-email","cost":2}'
{"id":"2"}

$ curl -s localhost:3100/queue/stats
{"pending":1,"in_progress":0,"completed":1,"dead_letter":0}
```
(real output captured from a local run against a freshly migrated database)

## Testing

```bash
npm test                  # full suite (fairness benchmark, crash recovery, dead-letter, sanity)
npm run test:fairness     # just the fairness benchmark
npm run test:crash        # just the crash-recovery test (spawns and SIGKILLs a real worker process)
npm run test:deadletter   # just the dead-letter test
```

All tests run against the real local Postgres cluster started above --
`FOR UPDATE SKIP LOCKED` is genuinely exercised, not mocked.

### Fairness benchmark

`test/fairness.spec.ts` simulates a "big" tenant enqueuing 400 jobs and a
"small" tenant enqueuing 4 (100x volume skew), both weight 1, costs varying
1-5, arrival order randomly interleaved. It independently recomputes the
SCFQ target order in plain JS from the same enqueue sequence (a from-scratch
second implementation of the formula above, not a copy of the production
code path) and asserts the real Postgres-backed dequeue order matches it
exactly, then reports the numeric comparison against naive baselines.
Real output from a local run:

```
[fairness benchmark] total jobs=404 (big=400, small=4, 100x volume skew), equal tenant weight 1:1
  small-tenant avg dequeue position -- WFQ (ours): 5.3 | naive FIFO: 188.8 | naive strict-priority-by-volume: 402.5  (out of 404 jobs)
  small-tenant avg claim latency (WFQ, wall clock): 3.75ms
  WFQ improvement over naive FIFO: 36.0x earlier service
  WFQ improvement over naive priority-by-volume: 76.7x earlier service

[weighted fairness] first 40 dequeues: gold(weight 4) cost served=63, standard(weight 1) cost served=16, ratio=3.94 (target ~4.0)
```

Despite being outnumbered 100:1, the small tenant's jobs land at position
~5 on average instead of ~189 (naive FIFO, since it happened to arrive
scattered through the stream) or ~403 (a naive strict tenant-priority
queue, which would starve it behind the entire big-tenant backlog). The
second run confirms weight actually controls proportional service: a
4x-weight tenant receives ~3.94x the cost-weighted service of a 1x-weight
tenant in the first 40 dequeues, tracking its 4:1 weight ratio rather than
their near-1:1 volume ratio.

### Crash recovery

`test/crash-recovery.spec.ts` enqueues one job, spawns a **real OS
process** (`src/worker/run-worker.ts` under `ts-node`, not an in-process
simulation) that claims it, waits for it to print `CLAIMED <id>`, then
sends it `SIGKILL` before it can ack. It asserts the job is invisible to
other workers until the visibility timeout elapses, then claimable by a
second worker, and that a stale ack from the dead worker's id (simulating a
zombie process resuming) is rejected by the `locked_by` check -- so
completion stays exactly-once even under that failure mode.

### Dead-letter

`test/dead-letter.spec.ts` fails a job repeatedly and asserts it flips to
`dead_letter` exactly at the configured `max_attempts` threshold, with
`failure_history` containing one intact record per attempt (error message
+ timestamp), and that dead-lettered jobs never resurface from `claim()`.

## Scope notes

- No retry backoff (failed jobs become immediately retryable, ordered by
  their original virtual finish time) -- a real deployment would likely add
  exponential backoff via `visible_at`, straightforward to layer on but
  outside this spec's core scheduling/durability focus.
- No admin UI; `GET /queue/stats` and `GET /queue/jobs/:id` are the only
  introspection endpoints.
- Tenant weights default to 1 and are set via `setTenantWeight` /
  `POST /queue/tenants/:id/weight`; there's no weight-change audit trail.
