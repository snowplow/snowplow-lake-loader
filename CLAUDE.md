# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Snowplow Lake Loader is a Scala application that loads Snowplow enriched events into open table formats (Delta, Iceberg, Hudi) on cloud storage. It supports Azure (Event Hubs → ADLS Gen2), GCP (Pubsub → GCS), and AWS (Kinesis → S3) platforms.

## Common Commands

### Building and Testing
```bash
# Run tests
sbt test

# Check Scala formatting
sbt scalafmtCheckAll scalafmtSbtCheck

# Format Scala code
sbt scalafmtAll scalafmtSbt

# Compile all modules
sbt compile

# Run a specific test class
sbt "testOnly *ProcessingSpec"

# Run tests for specific module
sbt "project core" test
```

### Docker Operations
```bash
# Build Docker image for AWS
sbt "project aws" docker:publishLocal

# Build Docker image for Azure with Hudi
sbt "project azureHudi" docker:publishLocal

# Stage Docker build
sbt "project gcp" docker:stage
```

### Module-Specific Commands
```bash
# Work with specific cloud modules
sbt "project aws" <command>
sbt "project azure" <command>
sbt "project gcp" <command>

# Work with table format modules
sbt "project awsHudi" <command>
sbt "project gcpBiglake" <command>
```

## Architecture

### Multi-Module Structure
- **Core Module** (`modules/core/`): Shared processing logic, Spark operations, table writers
- **Cloud Modules** (`modules/{aws,azure,gcp}/`): Cloud-specific entry points and authentication
- **Packaging Modules** (`packaging/{hudi,biglake,delta-iceberg}/`): Alternative table format dependencies

### Key Components
- **LoaderApp**: Main application entry point with cloud-specific implementations
- **Processing**: Core data processing pipeline using Apache Spark
- **Table Writers**: Format-specific writers (Delta, Iceberg, Hudi) in `modules/core/src/main/scala/com.snowplowanalytics.snowplow.lakes/tables/`
- **LakeWriter**: Orchestrates the writing process with windowing and batching

### Configuration System
- Uses HOCON format with reference configurations
- Cloud-specific configs in `config/` directory
- Default settings in `modules/core/src/main/resources/reference.conf`

### Dependencies and Exclusions
- Built with Scala 2.13.13 and SBT 1.9.8
- Uses Apache Spark for data processing
- Manages complex dependency conflicts (especially around different table format libraries)
- Common exclusions defined in `Dependencies.scala` to prevent version conflicts

### Testing Infrastructure
- `TestSparkEnvironment` provides Spark context for tests
- Test schemas stored in `modules/core/src/test/resources/iglu-client-embedded/`
- Uses ScalaTest framework
- Iglu schemas pre-fetched during build for offline testing

## Development Notes

### Code Style
- Uses Scalafmt version 3.6.0 with 140 character line limit
- Specific formatting rules for alignment and imports in `.scalafmt.conf`
- Copyright headers automatically managed by sbt-header plugin

### Build System Specifics
- Multi-project SBT build with complex dependencies between packaging variants
- Docker images built for multiple architectures (linux/amd64, linux/arm64/v8)
- Uses BuildInfo plugin to inject version and build metadata
- Biglake connector requires external JAR download during build

### Table Format Support
- **Delta**: Default format with configurable table properties
- **Iceberg**: Supports schema evolution and metadata optimization
- **Hudi**: Complex configuration for partitioning, clustering, and timeline management
- Format selection affects dependencies and runtime behavior

### Monitoring and Operations
- Health probe endpoint on port 8000
- Configurable metrics (StatsD), webhooks, and Sentry integration
- Telemetry data collection follows Snowplow defaults