# Snowplow Lake Loader

[![Build Status][build-image]][build]
[![Release][release-image]][releases]
[![License][license-image]][license]

## Introduction

This project contains applications required to load Snowplow data into Open Table Formats.

Lake Loader 0.9.0 supports [Delta](https://docs.delta.io/latest/index.html) and [Iceberg](https://iceberg.apache.org/docs/latest/) as output formats.

Check out [the example config files](./config) for how to configure your lake loader.

#### Azure

The Azure lake loader reads the stream of enriched events from Event Hubs and writes them to Azure Data Lake Storage Gen2.  This enables you to query your data lake via Databricks or Microsoft Fabric.

Basic usage:
`
```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  -v /path/to/iglu.json:/var/iglu.json \
  snowplow/lake-loader-azure:0.9.0 \
  --config /var/config.hocon \
  --iglu-config /var/iglu.json
```

#### GCP

The GCP lake loader reads the stream of enriched events from Pubsub and writes them to GCS.  This enables you to query your events in Databricks.

```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  -v /path/to/iglu.json:/var/iglu.json \
  snowplow/lake-loader-gcp:0.9.0 \
  --config /var/config.hocon \
  --iglu-config /var/iglu.json
```

#### AWS

The AWS lake loader reads the stream of enriched events from Kinesis and writes them to S3.  This enables you to query your events in Databricks or AWS Athena.

```bash
docker run \
  -v /path/to/config.hocon:/var/config.hocon \
  -v /path/to/iglu.json:/var/iglu.json \
  snowplow/lake-loader-aws:0.9.0 \
  --config /var/config.hocon \
  --iglu-config /var/iglu.json
```

## Find out more

| Technical Docs             | Setup Guide          | Roadmap & Contributing |
|----------------------------|----------------------|------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image]   |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]     |



## Copyright and License

Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[setup]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/
<!-- TODO: update link when docs site has a lake loader page: -->
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/
[roadmap]: https://github.com/snowplow/snowplow/projects/7

[build-image]: https://github.com/snowplow-incubator/snowplow-lake-loader/workflows/CI/badge.svg
[build]: https://github.com/snowplow-incubator/snowplow-lake-loader/actions/workflows/ci.yml

[release-image]: https://img.shields.io/badge/release-0.9.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-lake-loader/releases

[license]: https://docs.snowplow.io/limited-use-license-1.1
[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat
