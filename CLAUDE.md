# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Debezium is an open source change data capture (CDC) platform that provides a low latency data streaming platform for monitoring databases. It captures row-level changes in databases and produces change events that can be consumed by applications. This is version 1.9.8.Final.

**Key Architecture:**
- Built on top of Kafka Connect framework for distributed, scalable deployment
- Also supports embedded mode where connectors run directly in application space
- Each connector monitors a single upstream database server
- Captures all changes and records them in Kafka topics (typically one topic per table)

## Build System

This is a Maven multi-module project with the following requirements:
- **JDK 11 or later** required for building (enforced by Maven)
- **Java 8** is the compilation target (source/target: 1.8)
- **Maven 3.6.3 or later** (or use `./mvnw` wrapper)
- **Docker** required for integration tests (uses Docker containers for database instances)

### Common Build Commands

**Full build with all tests:**
```bash
mvn clean verify
```

**Full build with assembly (used for releases):**
```bash
mvn clean install -Passembly
```

**Quick build (skip all tests, CheckStyle, formatters, etc.):**
```bash
mvn clean verify -Dquick
```

**Skip integration tests only (if Docker not available):**
```bash
mvn clean verify -DskipITs
```

**Skip all tests:**
```bash
mvn clean install -DskipTests -DskipITs
```

### Running Tests

**Run tests for a specific connector:**
```bash
cd debezium-connector-mysql
mvn clean verify
```

**Start Docker container for manual testing:**
```bash
cd debezium-connector-mysql
mvn docker:build docker:start
```

This starts a Docker container that stays running for IDE-based test execution. Integration tests expect database connection info as system properties (e.g., `-Ddatabase.hostname=localhost -Ddatabase.port=3306`).

**Stop Docker containers:**
```bash
mvn docker:stop
```

### Connector-Specific Testing

**PostgreSQL with wal2json decoder:**
```bash
mvn clean install -pl :debezium-connector-postgres -Pwal2json-decoder
```

**PostgreSQL with pgoutput decoder:**
```bash
mvn clean install -pl :debezium-connector-postgres -Ppgoutput-decoder,postgres-10
```

**Oracle with XStream:**
```bash
mvn clean install -pl debezium-connector-oracle -Poracle-xstream,oracle-tests -Dinstantclient.dir=<path>
```

**MongoDB with oplog:**
```bash
mvn clean install -pl debezium-connector-mongodb -Dcapture.mode=oplog -Dversion.mongo.server=3.6
```

## Module Structure

### Core Modules

- **debezium-api**: Public API and SPI interfaces (engine, common types)
- **debezium-core**: Core framework implementation shared by all connectors
  - `io.debezium.pipeline`: Pipeline framework for coordinating change event sources
  - `io.debezium.relational`: Base classes for relational database connectors
  - `io.debezium.schema`: Schema management and history tracking
  - `io.debezium.config`: Configuration management
  - `io.debezium.transforms`: Kafka Connect SMTs (Single Message Transforms)
- **debezium-embedded**: Library for embedding connectors directly in applications (no Kafka required)
- **debezium-ddl-parser**: ANTLR-based DDL parsers for tracking schema changes

### Connector Modules

Each connector follows a standard structure:
- **debezium-connector-mysql**: MySQL/MariaDB connector
- **debezium-connector-mongodb**: MongoDB connector
- **debezium-connector-oracle**: Oracle connector
- **debezium-connector-sqlserver**: SQL Server connector
- **debezium-connector-kingbase**: KingBase connector (PostgreSQL-based)
- **debezium-connector-dameng**: DaMeng connector (custom addition)

### Support Modules

- **debezium-server**: Standalone server (alternative to Kafka Connect)
- **debezium-testing**: Testing utilities and system tests
- **debezium-quarkus-outbox**: Quarkus extension for outbox pattern
- **support/checkstyle**: CheckStyle configuration
- **support/revapi**: API compatibility checking

## Architecture Patterns

### Connector Architecture

Every Kafka Connect connector in Debezium follows this structure:

1. **Connector class** (extends `SourceConnector`): Entry point, returns Task class and configuration
   - Example: `MySqlConnector`

2. **ConnectorTask class** (extends `SourceTask`): Does the actual work of reading changes
   - Example: `MySqlConnectorTask`

3. **ConnectorConfig class**: Configuration definition and validation
   - Example: `MySqlConnectorConfig`

4. **ChangeEventSourceFactory**: Factory for creating snapshot and streaming sources
   - Example: `MySqlChangeEventSourceFactory`

5. **SnapshotChangeEventSource**: Handles initial snapshot of database
   - Example: `MySqlSnapshotChangeEventSource`

6. **StreamingChangeEventSource**: Handles ongoing streaming of changes
   - Example: `MySqlStreamingChangeEventSource` (reads MySQL binlog)

### Pipeline Framework

The pipeline framework in `debezium-core` coordinates the lifecycle:

- **ChangeEventSourceCoordinator**: Orchestrates snapshot and streaming phases
- **ChangeEventSource** (SPI): Base interface for event sources
- **SnapshotChangeEventSource**: SPI for snapshot implementations
- **StreamingChangeEventSource**: SPI for streaming implementations
- **IncrementalSnapshotChangeEventSource**: SPI for incremental snapshots (read-only)

### Offset & History Management

- **OffsetContext**: Tracks connector position in the source (binlog position, LSN, etc.)
- **DatabaseHistory**: Tracks schema changes over time (required for relational DBs)
  - Stored separately from offsets (e.g., in Kafka topic or file)
  - Allows connector to reconstruct schema at any historical offset

### Embedded Engine Usage

Applications can embed connectors without Kafka using `debezium-embedded`:

```java
Configuration config = Configuration.create()
    .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
    .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    // ... connector-specific config
    .build();

EmbeddedEngine engine = EmbeddedEngine.create()
    .using(config)
    .notifying(record -> handleEvent(record))
    .build();

executor.execute(engine);
```

**Important**: Embedded engine provides at-least-once delivery (may see duplicates after crashes). For exactly-once, use full Kafka Connect deployment.

## Key Dependencies

- **Kafka**: 3.2.0 (Scala 2.13)
- **Jackson**: 2.13.2 (bumped for CVE-2020-36518)
- **Quarkus**: 2.7.2.Final
- **Apicurio**: 2.1.5.Final (schema registry integration)
- **ANTLR**: 4.8 (for DDL parsing)

**Database Drivers:**
- PostgreSQL: 42.3.5
- MySQL: 8.0.28, mysql-binlog-connector 0.27.2
- MongoDB: 4.3.3
- SQL Server: 9.4.1.jre8
- Oracle: 21.1.0.0

## Development Guidelines

### Adding a New Connector

1. Create a new module `debezium-connector-<database>`
2. Implement the required classes following the architecture pattern above
3. Extend base classes from `debezium-core`:
   - For relational DBs: extend classes in `io.debezium.relational`
   - Implement `ChangeEventSourceFactory` to provide snapshot/streaming sources
4. Add connector module to parent `pom.xml` `<modules>` section
5. Provide Docker configuration for integration tests
6. Add connector-specific README.md with configuration details

### Understanding Change Event Flow

1. **Snapshot Phase**: Connector reads current state of database (via `SnapshotChangeEventSource`)
   - Takes consistent snapshot of tables
   - Emits READ events for existing rows
   - Records schema at snapshot time

2. **Streaming Phase**: Connector monitors ongoing changes (via `StreamingChangeEventSource`)
   - MySQL: reads binlog
   - PostgreSQL: uses logical decoding (decoderbufs, wal2json, or pgoutput)
   - MongoDB: reads oplog or change streams
   - Emits CREATE, UPDATE, DELETE events

3. **Schema Tracking**: `DatabaseHistory` records all DDL changes
   - Critical for interpreting binlog/wal events at any offset
   - Must be maintained alongside offsets

### Common Development Tasks

**Run a single test:**
```bash
cd debezium-connector-mysql
mvn test -Dtest=MySqlConnectorIT#shouldProcessCreateTable
```

**Debug with Docker container:**
```bash
cd debezium-connector-mysql
mvn docker:start
# Docker container is now running, run tests from IDE with:
# -Ddatabase.hostname=localhost -Ddatabase.port=3306
```

**Format code:**
```bash
mvn formatter:format
```

**Check code style:**
```bash
mvn checkstyle:check
```

**Check API compatibility:**
```bash
mvn revapi:check
```

## Special Notes

- This fork appears to have custom connectors added: `debezium-connector-kingbase` and `debezium-connector-dameng`
- The main branch is `tis.v1.9.8.Final` (not the standard `main` or `master`)
- Custom Maven repository configured: `oss://maven.qlangtech.com/`
- PostgreSQL connector module is commented out in parent POM (line 167)
- Integration tests require Docker to be running and accessible
- Tests marked as "long running" are skipped by default unless `-Passembly`, `-Prelease`, or `-Pperformance` profiles are active

## Useful File Patterns

- Connector entry points: `**/MySqlConnector.java`, `**/MongoDbConnector.java`
- Task implementations: `**/*ConnectorTask.java`
- Configuration: `**/*ConnectorConfig.java`
- Snapshot logic: `**/*SnapshotChangeEventSource.java`
- Streaming logic: `**/*StreamingChangeEventSource.java`
- Integration tests: `**/src/test/java/**/*IT.java`
- Unit tests: `**/src/test/java/**/*Test.java` (excluding IT.java)