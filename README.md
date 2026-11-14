OpenRate
========

OpenRate is an open source **rating and billing engine** for high volume
transaction processing, written in Java. It is used to build systems that read
usage records (call records, data sessions, events, meter readings), enrich and
validate them, apply pricing, and write the results out again.

OpenRate is a **framework, not an application**. On its own it does nothing
useful: you build an application on top of it by configuring a pipeline and
extending the abstract modules it provides. See
[Building an application](#building-an-application).

* **Version:** 1.5.2.6
* **Java:** 17 (LTS)
* **Build:** Maven
* **License:** Apache License 2.0

---

## Table of contents

- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [Architecture](#architecture)
- [The transaction manager](#the-transaction-manager)
- [Multi-pipeline synchronisation](#multi-pipeline-synchronisation)
- [Configuration](#configuration)
- [Building an application](#building-an-application)
- [Module reference](#module-reference)
- [The record model](#the-record-model)
- [Rating concepts](#rating-concepts)
- [Project layout](#project-layout)
- [Dependencies](#dependencies)
- [Development notes and known issues](#development-notes-and-known-issues)

---

## Quick start

### Build

```bash
mvn clean verify
```

This compiles, runs the full test suite (155 tests) and produces
`target/OpenRate-<version>.jar` plus a sources jar.

The tests are self contained: they run against an in-memory HSQLDB instance and
need no external database or configuration. They do bind **TCP port 8086** on
localhost for the ECI, so the suite will fail if that port is already in use.

### Run

OpenRate is started by pointing it at a configuration file, which **must be on
the classpath** (not a filesystem path):

```bash
java -cp $CLASSPATH OpenRate.OpenRate -p <properties-file.properties.xml>
```

The framework runs in one of two modes, determined by the adapters you
configure rather than by a command line switch:

* **Batch mode** — the pipeline starts, processes whatever input is available,
  and shuts down. Typical for file and database driven processing.
* **Daemon mode** — the pipeline starts and runs until stopped, waiting for new
  work to arrive. Typical for socket, JMS and real time pipelines.

### Exit codes

`OpenRate.main()` returns a non-zero status when startup fails, for example when
the arguments are malformed or the properties file cannot be found on the
classpath. Startup failures print the reason to stdout before exiting.

---

## Core concepts

| Concept | Meaning |
|---|---|
| **Framework** | The top level process. Owns resources and one or more pipelines. |
| **Pipeline** | An input adapter, a chain of processing modules, and one or more output adapters. |
| **Adapter** | Reads records from, or writes records to, the outside world. |
| **Processing module (plug in)** | A step in the pipeline that inspects or modifies a record. |
| **Record** | The unit of work travelling through a pipeline. |
| **Buffer** | The thread safe queue connecting one pipeline stage to the next. |
| **Resource** | A framework level, shared service: logging, data sources, caches, the transaction manager, the event interface. |
| **Cache** | Reference data loaded once and shared by processing modules, typically pricing or customer data. |
| **Transaction** | A batch of work (a file, a database selection) treated as a single commit/rollback unit. |
| **Stream** | The external unit a transaction wraps, for example one input file. |

---

## Architecture

### Framework and pipelines

A single OpenRate process (the *framework*) hosts one or more *pipelines*.
Pipelines within a framework share resources and memory, which is the main
reason to run several pipelines in one process rather than several processes.

```
                        +-----------------------------------+
                        |          OpenRate framework       |
                        |                                   |
   Shared resources ----+--> LogFactory                     |
   (one instance, used  |    DataSourceFactory              |
    by every pipeline)  |    CacheFactory                   |
                        |    TransactionManagerFactory      |
                        |    ECI (event control interface)  |
                        |                                   |
                        |  +-----------+   +-----------+    |
                        |  | Pipeline  |   | Pipeline  | .. |
                        |  +-----------+   +-----------+    |
                        +-----------------------------------+
```

### Pipeline anatomy

Each pipeline is a linear chain. Stages are connected by buffers, and each stage
runs in its own thread inside a dedicated thread group:

```
  external            +--------+          +--------+          +--------+            external
  source   --------> | Input   | -buffer-> | Proc 1 | -buffer-> | Proc N | -buffer-> | Output | --------> sink
                     | Adapter |          | module |          | module |          | Adapter|
                     +--------+          +--------+          +--------+            +--------+
```

Key properties:

* **Records move in batches.** Each module has a configurable `BatchSize`
  controlling how many records it takes from its input buffer per cycle. This
  amortises the cost of thread handoff; record-at-a-time processing would be far
  slower.
* **Stages run concurrently.** While module N processes batch 1, module N-1 can
  already be processing batch 2.
* **Output adapter chaining is supported**, so one record can be written to
  several outputs. If an output is not explicitly named, the record goes to all
  of them.
* The pipeline is assembled reflectively from configuration in
  `Pipeline.ConfigurePipeline()`. Modules are named in the config file by their
  fully qualified class name.

### Header and trailer records

Streams are delimited by control records rather than by adapter callbacks:

* A **HeaderRecord** is emitted when a new stream (for example a file) starts.
  This opens a transaction.
* A **TrailerRecord** is emitted when the stream ends. This begins transaction
  close down.

Every processing module sees these records and may act on them — this is how
modules know to flush per-stream state. Modules that do not care simply pass
them through.

---

## The transaction manager

OpenRate deliberately does **not** use record level (J2EE style) transactions,
which are far too costly for the volumes involved. Instead, each *stream* is
wrapped in a transaction, managed by `OpenRate.transaction.TransactionManager`.

Every module that participates registers as a *client* of the transaction. The
transaction advances only when all clients agree, and its overall status is the
*lowest* status reported by any client.

### Transaction states

Defined in `OpenRate.transaction.TMDefs`:

| Value | State | Meaning |
|---|---|---|
| 0 | `TM_NONE` | No transaction. |
| 1 | `TM_PROCESSING` | Records are flowing. |
| 2 | `TM_FLUSHED` | All records have passed through; clients may now commit. |
| 3 | `TM_FINISHED_ERR` | Client finished, but with errors. |
| 4 | `TM_FINISHED_OK` | Client finished cleanly. |
| 5 | `TM_CLOSING` | Transaction is closing down. |
| 6 | `TM_CLOSED` | Complete. |

If *any* client reports `TM_FINISHED_ERR`, or an abort is requested, the whole
transaction is marked errored and rolls back.

The `TransactionFlusher` runs as a separate thread per pipeline. Once every
client has flushed, the transaction is queued to the flusher, which drives it
through commit or rollback and then closes it.

---

## Multi-pipeline synchronisation

Some operations — notably reloading a shared cache — cannot safely run while
pipelines are processing. The framework coordinates this with a sync protocol
across all pipelines in the process:

1. A pipeline that needs a sync point sets its sync status to **1** (`SyncFlag`).
2. The framework sees the flag and sets **every** pipeline's status to **2**
   (`SyncRequest`).
3. Each pipeline must stop after finishing its current transaction, then report
   **3** (`SyncReached`).
4. Once *all* pipelines report `SyncReached`, the framework sets them to **4**
   (`SyncProcess`). Pending events are processed now.
5. Each pipeline reports **5** (`SyncDone`) when finished.
6. When all are `SyncDone`, the framework resets everyone to **0** and normal
   processing resumes.

This is why a cache reload in one pipeline briefly pauses the others.

---

## Configuration

A single XML file configures the entire framework. It must be on the classpath.
Note that despite the `.properties.xml` extension it is ordinary XML, not a Java
properties file.

### Overall shape

```xml
<config>
  <Application> ... </Application>

  <!-- which pipelines exist, and whether they start -->
  <PipelineList>
    <MyPipe><Active>True</Active></MyPipe>
  </PipelineList>

  <!-- one block per pipeline, named to match PipelineList -->
  <MyPipe>
    <InputAdapter>
      <MyInput>
        <ClassName>OpenRate.adapter.file.FlatFileInputAdapter</ClassName>
        <BatchSize>5000</BatchSize>
      </MyInput>
    </InputAdapter>

    <Process>
      <MyLookupStep>
        <ClassName>com.example.MyLookupPlugIn</ClassName>
        <DataCache>MyCache</DataCache>
        <BatchSize>5000</BatchSize>
      </MyLookupStep>
    </Process>

    <OutputAdapter>
      <MyOutput>
        <ClassName>OpenRate.adapter.file.FlatFileOutputAdapter</ClassName>
        <BatchSize>5000</BatchSize>
      </MyOutput>
    </OutputAdapter>
  </MyPipe>

  <!-- framework wide shared services -->
  <Resource>
    <LogFactory> ... </LogFactory>
    <ECI> ... </ECI>
    <TransactionManagerFactory> ... </TransactionManagerFactory>
    <DataSourceFactory> ... </DataSourceFactory>
    <CacheFactory> ... </CacheFactory>
  </Resource>
</config>
```

Processing modules execute **in the order they appear** inside `<Process>`.

### Resources

**LogFactory** — wires up logging. `Properties` names a Log4j2 configuration
file, also resolved from the classpath.

```xml
<LogFactory>
  <ClassName>OpenRate.logging.LogFactory</ClassName>
  <Properties>log4j2.xml</Properties>
  <DefaultCategory>MyApp</DefaultCategory>
</LogFactory>
```

**DataSourceFactory** — defines JDBC connection pools. `DataSourceBuilder`
selects the pool implementation; each child of `<DataSource>` is a named pool
referenced elsewhere by that name.

```xml
<DataSourceFactory>
  <ClassName>OpenRate.resource.DataSourceFactory</ClassName>
  <DataSourceBuilder>
    <ClassName>OpenRate.db.C3P0DataSource</ClassName>
  </DataSourceBuilder>
  <DataSource>
    <MyDB>
      <db_url>jdbc:hsqldb:mem://localhost/testdb</db_url>
      <driver>org.hsqldb.jdbc.JDBCDriver</driver>
      <username>SA</username>
      <password></password>
      <ValidationQuery>SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS</ValidationQuery>
      <TestConnectionPeriod>60</TestConnectionPeriod>
    </MyDB>
  </DataSource>
</DataSourceFactory>
```

Available pool builders: `OpenRate.db.C3P0DataSource`,
`OpenRate.db.HikariCPDataSource`.

**CacheFactory** — declares reference data caches. Each cache names its
implementation class, where its data comes from (`DB` or `File`), and the query
or file that loads it.

```xml
<CacheFactory>
  <ClassName>OpenRate.resource.CacheFactory</ClassName>
  <CacheableClass>
    <MyCache>
      <ClassName>OpenRate.cache.BestMatchCache</ClassName>
      <DataSourceType>DB</DataSourceType>
      <DataSource>MyDB</DataSource>
      <SelectStatement>select MAP_GROUP, INPUT_VAL, RESULT from MY_MAP</SelectStatement>
    </MyCache>
  </CacheableClass>
</CacheFactory>
```

Processing modules bind to a cache with `<DataCache>MyCache</DataCache>`.

**ECI (Event Control Interface)** — opens an admin socket for querying and
controlling a running framework: inspecting module state, triggering cache
reloads, and shutting down cleanly.

```xml
<ECI>
  <ClassName>OpenRate.configurationmanager.EventHandler</ClassName>
  <Port>8086</Port>
  <MaxConnection>2</MaxConnection>
</ECI>
```

Connect with any line oriented client (`telnet`, `nc`). Modules expose their own
commands by registering services through `ClientManager`.

> **Security note:** the ECI port is an unauthenticated control channel. Bind it
> to a trusted interface and firewall it.

---

## Building an application

1. **Depend on the OpenRate jar.**
2. **Define a record type.** Extend `OpenRate.record.AbstractRecord`, or use
   `FlatRecord` / `RatingRecord` directly. A record normally implements:
   * `mapOriginalData()` — raw input to structured fields (mandatory)
   * `unmapOriginalData()` — structured fields back to flat output (needed to
     write files)
   * `getDumpInfo()` — human readable dump for diagnostics (optional)
3. **Extend the processing modules you need.** Most classes in
   `OpenRate.process` are named `Abstract*` because they implement the mechanics
   and leave the business decision to you. A typical plug in extends
   `AbstractPlugIn` (or `AbstractTransactionalPlugIn`) and implements:
   * `procValidRecord(IRecord r)` — the normal path
   * `procErrorRecord(IRecord r)` — records already flagged with errors
   * `procHeader(...)` / `procTrailer(...)` — stream boundaries
4. **Pick adapters**, or extend one if your format is unusual.
5. **Write the configuration file** and put it on the classpath.
6. **Run it.**

Example applications that demonstrate complete pipelines:

* **OpenRateSimple** — the simplest non-trivial pipeline; start here
* **ORPixip** — revenue assurance across five data sources, with re-rating and
  comparison
* **ORTyfon** — fixed line and VoIP reseller rating with fraud detection
* **ORSeaSatCom** — GSM and satellite reseller retail rating
* **OpenRateWeblinksInterconnect** — interconnect rating

---

## Module reference

### Input adapters

| Package | Adapters |
|---|---|
| `adapter.file` | `FlatFileInputAdapter`, `FlatFileNTInputAdapter`, `BinaryFileInputAdapter`, `XMLFileInputAdapter`, `ASN1FileInputAdapter` |
| `adapter.jdbc` | `JDBCInputAdapter` |
| `adapter.jms` | `AbstractAMQInputAdapter` (ActiveMQ) |
| `adapter.socket` | `SocketInputAdapter` |
| `adapter.objectInterface` | `ObjectInputAdapter`, `BufferInputAdapter` |
| `adapter.listener` | `ListenerNTInputAdapter` |
| `adapter` | `NullInputAdapter` (testing) |

File input adapters select files using a glob of the form
`<prefix>*<suffix>`, where `*` must match at least one character.

### Output adapters

| Package | Adapters |
|---|---|
| `adapter.file` | `FlatFileOutputAdapter`, `FlatFileNTOutputAdapter`, `FlatFileMultiStreamOutputAdapter` |
| `adapter.jdbc` | `JDBCOutputAdapter`, `JDBCBatchOutputAdapter`, `MySQLDirectLoadOutputAdapter`, `PgSQLCopyOutputAdapter` |
| `adapter.jms` | `AbstractAMQOutputAdapter` |
| `adapter.socket` | `SocketOutputAdapter` |
| `adapter` | `NullOutputAdapter`, `NullNTOutputAdapter` |

`MySQLDirectLoadOutputAdapter` and `PgSQLCopyOutputAdapter` use native bulk load
paths and are dramatically faster than row-by-row inserts for large volumes.

### Real time adapters

`adapter.realTime` provides a request/response path that bypasses batch
buffering for latency sensitive work (for example online credit checks):
`AbstractRTAdapter`, `AbstractRTSocketAdapter`, `AbstractRTMethodAdapter`,
`RTSocketListener`, `RTMethodListener`, `RTProcessingProxy`.

### Tee adapters

`AbstractTeeAdapter`, `AbstractBufferTeeAdapter` and `AbstractSocketTeeAdapter`
duplicate a record stream to a secondary destination without disturbing the main
flow — useful for auditing and monitoring.

### Processing modules

**Lookup and matching**

`AbstractBestMatch`, `AbstractBestMatchFixedLine`, `AbstractRegexMatch`,
`AbstractIndexedLookupMatch`, `AbstractNumberRangeMatch`,
`AbstractMultipleValidityMatch`, `AbstractValidityFromLookup`,
`AbstractValiditySegmentMatch`, `AbstractTimeMatch`

**Rating**

`AbstractRateCalc`, `AbstractRUMRateCalc`, `AbstractRUMCPRateCalc`,
`AbstractRUMMap`, `AbstractRUMBestMatch`, `AbstractRUMBestMatchFixedLine`,
`AbstractRUMTimeMatch`, `AbstractRUMHolidayMatch`, `AbstractMinFeeCalc`,
`AbstractProRation`, `GatherRUMImpacts`

**Customer and balance**

`AbstractCustomerLookup`, `AbstractCustomerLookupAudited`,
`AbstractJBCustomerLookup`, `AbstractBalanceHandlerPlugIn`

**Control and utility**

`AbstractPlugIn`, `AbstractTransactionalPlugIn`, `AbstractStubPlugIn`,
`AbstractDuplicateCheck`, `AbstractAggregation`, `AbstractCallAssembly`,
`AbstractPersistentObjectProcess`, `AbstractTransientHash`,
`AbstractRecordInsertPlugIn`, `AbstractEmailNotification`, `ProcessStateLogger`,
`Dump`, `DumpNT`, `DumpRT`

### Caches

Reference data is loaded once into a cache and shared across modules. Caches
implement `ICacheable` and are loaded by `ICacheLoader`; those implementing
`ICacheAutoReloadable` can be reloaded at runtime via the ECI (which triggers
the sync protocol described above).

`BestMatchCache`, `BestMatchFixedLineCache`, `RegexMatchCache`,
`IndexedLookupCache`, `NumberRangeCache`, `MultipleValidityCache`,
`ValidityFromCache`, `ValiditySegmentCache`, `TimeModelCache`, `RateCache`,
`RUMRateCache`, `RUMCPRateCache`, `RUMMapCache`, `RUMHolidayMatchCache`,
`MinFeeCache`, `BalanceCache`, `CustomerCache`, `CustomerCacheAudited`,
`JBCustomerCache`, `AggregationCache`, `CallAssemblyCache`,
`DuplicateCheckCache`, `PersistentIndexedObject`, `TransientHash`

### Buffers

`ArrayListQueueBuffer` and `LinkedListQueueBuffer` implement `IBuffer` and
connect pipeline stages. `LinkedListQueueBuffer` is backed by
`ConcurrentLinkedDeque`.

---

## The record model

All records derive from `AbstractRecord` and implement `IRecord`.

| Record | Purpose |
|---|---|
| `FlatRecord` | A raw line of text, as read or written by a file adapter. |
| `RatingRecord` | The main structured record for rating, carrying charge packets and RUMs. |
| `DBRecord` | Database sourced record. |
| `HeaderRecord` / `TrailerRecord` | Stream boundary markers (see above). |
| `KeyValuePairRecord` | Generic key/value payload. |
| `QueueMessageRecord` | JMS message wrapper. |

Records accumulate errors rather than throwing: a module attaches a
`RecordError` (see `IError`, `ErrorType`, `AbstractError`) and the record
continues down the pipeline, where later modules and the output adapter can
route it to an error sink. This keeps one bad record from stopping a run.

---

## Rating concepts

**RUM** — *rateable Usage Metric*. A named, quantified unit on a record that
pricing is applied to: seconds of call duration, megabytes transferred, number
of events. A single record may carry several RUMs, each priced differently.

**Charge packet** (`ChargePacket`) — holds the rating information driving zoning
and rating. Each time the applicable rating changes, a new charge packet is
created, so one record may hold many.

**Price model** — a named set of tiers and beats. Rating types supported by the
RUM map include:

* `Flat` — simple `RUM × price`, no tiers or beats
* `Tiered` — price varies by consumption band
* and further models described in the `RUMMapCache` javadoc

**Beat** — the granularity of charging; for example 60 second beats round a
45 second call up to a full minute.

**Resource** — in a rating context, the thing a charge impacts: a currency, a
bundle allowance, a counter. `BalanceImpact` records the effect on a balance.

**RUM map** — maps price groups to the price models, RUMs and resources they
affect. Loaded by `RUMMapCache` from file or database.

---

## Project layout

```
src/main/java/OpenRate/
  OpenRate.java            framework entry point and lifecycle
  Pipeline.java            pipeline assembly and execution
  IPipeline.java           pipeline contract
  CommonConfig.java        shared configuration constants

  adapter/                 input and output adapters (file, jdbc, jms,
                           socket, realTime, objectInterface, listener)
  buffer/                  inter-stage queues
  cache/                   reference data caches
  configurationmanager/    ECI: event handling and admin sockets
  db/                      JDBC helpers and connection pool builders
  exception/               exception hierarchy
  lang/                    domain types (customer, product, balance helpers)
  logging/                 logging abstraction over Log4j2
  parser/                  parsers, including ASN.1
  process/                 processing modules
  record/                  record model
  resource/                framework level shared resources
  threads/                 thread pool
  transaction/             transaction manager and flusher
  utils/                   conversion and general utilities

src/test/
  java/OpenRate/           unit and integration tests
  java/TestUtils/          framework bootstrap helpers for tests
  resources/               test configurations (*.properties.xml)
```

---

## Dependencies

Runtime dependencies are deliberately minimal:

| Dependency | Purpose |
|---|---|
| `log4j-api` / `log4j-core` | Logging |
| `c3p0` | JDBC connection pool |
| `HikariCP` | JDBC connection pool (alternative) |
| `activemq-client` | JMS adapters |
| `angus-mail` | Email notification module |
| `mysql-connector-j` | MySQL driver |
| `postgresql` | PostgreSQL driver |
| `hsqldb` | HSQLDB driver, and the test database |
| `junit` | Tests only |

The JDBC drivers are provided for convenience; a deployment only needs the one
matching its database.

---

## Development notes and known issues

### Running the tests

The suite is timing sensitive in places because it exercises concurrent
transaction handling. It is also timezone and locale independent — verified
under `TZ=UTC` as well as CET.

The suite binds localhost port 8086 (the ECI), so it cannot run twice
concurrently on one machine.

Always run the suite with a timeout, so a concurrency regression fails the build
instead of hanging it:

```bash
mvn -B clean verify -Dsurefire.timeout=400
```

### c3p0 statement caching is disabled by default

`C3P0DataSource` sets both `maxStatements` and `maxStatementsPerConnection` to
`0`. This is deliberate. c3p0 documents these as alternatives and warns against
setting both; from c3p0 0.10 onward, setting both causes cached statements
belonging to connections already returned to the pool to be handed out, which
surfaces as the SQL error `connection exception: closed` once transactions
overlap.

Override per data source with `MaxStatements` / `MaxStatementsPerConnection`
only if you
have verified the behaviour with your driver. Most modern drivers cache server
side anyway.

### Test isolation

Some tests in `AbstractDuplicateCheckTest` open a transaction and close it at
the end of the test body. If an assertion fails in between, the transaction is
leaked, and any later test that waits for all transactions to close will hang
rather than fail. When a test in this class hangs, look for an *earlier* test
failure as the real cause.

### Java 21 and later

`ThreadGroup.destroy()` is called in `OpenRate.java`, `Pipeline.java` and
`CacheFactory.java`. It is deprecated for removal and throws
`UnsupportedOperationException` on Java 21+. This must be addressed before
moving beyond Java 17. The CI workflow runs Java 21 and 25 as advisory
(non-blocking) jobs to track this.

### Continuous integration

`.github/workflows/build.yml` gates on Java 17 and additionally runs advisory
builds on newer JDKs. Test reports are uploaded on failure.

---

## Further reading

The code is the primary documentation, and the javadoc is substantial —
particularly on `OpenRate`, `Pipeline`, `TransactionManager` and the
`Abstract*` process modules, which document their configuration properties
inline. Generate it with:

```bash
mvn javadoc:javadoc -Ddoclint=none
```

Output lands in `target/reports/apidocs/`. The `-Ddoclint=none` is required:
a number of javadoc comments embed unescaped XML and HTML examples, which
JDK 17's doclint rejects as malformed. Without the flag the build fails.

Each package also carries a `package.html` describing its role.

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

Copyright 2006-2026 Ian Sparkes.
