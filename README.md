# DynamoDB Sync - ddb-sync

## Table of Contents

- [Introduction](#introduction)

  - [Quickstart](#quickstart)

  - [Functionality](#functionality)

  - [Problems we are solving](#problems-we-are-solving)

  - [Similar tools](#similar-tools)

- [Usage](#usage)

  - [Installation](#installation)

  - [Configuration methods](#configuration-methods)

  - [Output](#output)

    - [Logging](#logging)

    - [Status Messaging](#status-messaging)

  - [Invocation](#invocation)

    - [Permission Prerequisites](#permission-prerequisites)

    - [Configuration By File](#configuration-by-file)

    - [Configuration By CLI](#configuration-by-cli)

- [Development and Testing tools](#development-and-testing-tools)

- [Contributing](#contributing)


## Introduction

ddb-sync is a tool used for syncing data from a set of source tables to a set of destination tables in DynamoDB.  It's configurable to perform the backfill operation, the stream consumption or both by CLI or config file options.

### Quickstart

`go install gerrit.instructure.com/ddb-sync` and `ddb-sync --help` for usage

### Functionality

ddb-sync has two phases of work: streaming and backfilling. Backfill is a scan of the source table and duplication of each record to the destination table. Stream consumes a pre-configured DynamoDB Stream and writes all record changes to the destination table.

If streaming and backfill are configured on a table, stream runs sequentially after backfill.

**Note**: On actively written tables, the streaming phase is required to get new records synced.

ddb-sync does not verify source and destination tables consistency and an outside methodology should be used to verify the table has been delivered as expected.

### Problems we are solving

- Table Migrations
  - Across regions
  - Across accounts
- Table synchronizations

### Similar tools
- dynamodb-table-sync
- copy-dynamodb-table
- dynamodb-replicator

## Usage

### Installation

ddb-sync is a golang binary.

Ensure you have a proper Go environment setup and then:

```command
go install gerrit.instructure.com/ddb-sync
```

#### Tuning

**Note:** Be sure your source tables have provisioned capacity for reads and writes before using the tool. To optimize performance you should adjust provisioned read and write capacity. Be cautious about table sharding constraints.

During the output of the tool we note consumed write capacity units. This can be used to help provision and scale your table with use of the tool. See the output section for more details.

### Configuration methods

Invoke the installed binary:

`ddb-sync <cli-options>`

### Output

ddb-sync logs to stdout and displays status messaging to stderr. Either can be redirected to a file for later viewing.

For instance, `ddb-sync --config-file <config_file.yml> 1>ddb-sync-log.out` would capture the log to a file called ddb-sync-log.out.

#### Logging

The log will display each operation on a table, relevant status to the operation and when a checkpoint has occurred. The format is below.

`timestamp [src-table] => [dest-table] Log statement`

ddb-sync logs (with a timestamp prefix) on starts, completions and shard transitions in the stream. An example is below.


`2018/06/19 11:05:27 [ddb-sync-source] ⇨ [ddb-sync-dest] Backfill complete. 14k items written.`

Additionally, occasionally the tool will add checkpoints to the log for historical convenience, see below for an example.

```console
2018/06/19 11:35:35 Checkpoint: ↓↓↓↓
[ddb-sync-source] ⇨ [ddb-sync-dest]: Streaming: ~1k items written over 1m1s
[ddb-sync-source-2] ⇨ [ddb-sync-dest-2]: Backfill in progress: 822 items written over 1m1s
```


#### Status Messaging

The bottom portion of the output given to the user is the status messaging. It describes each table, the operation being worked on, the write capacity unit usage rate for writing to the destination table and the amount of records written from each operation.

An example follows:

```
TABLE                    DETAILS              BACKFILL        STREAM                         RATES & BUFFER
⇨ ddb-sync-dest         47K items (29MiB)    -COMPLETE-      2019 written (~18h57m latent)  12 items/s ⇨ ◕ ⇨ 32 WCU/s
⇨ ddb-sync-dest-2       ~46K items (~26MiB)  -SKIPPED-       789 written (~46m35s latent)   9  items/s ⇨ ◕ ⇨ 17 WCU/s
⇨ ddb-sync-destination  ~46K items (~26MiB)  267164 written  -PENDING-                      49 items/s ⇨ ◕ ⇨ 50 WCU/s
```

#### Invocation

Invoke the compiled binary and provide options for a run.

`ddb-sync <cli-options>`

#### Permission Prerequisites

The IAM permissions required by the roles the application uses to read and put data to the destination tables is listed as follows for each phase.

| Table             | Backfill Permissions                                         | Stream Permissions                                           |
| ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Source Table      | dynamodb:BatchGetItem<br />dynamodb:DescribeTable<br />dynamodb:GetItem<br />dynamodb:GetRecords<br />dynamodb:Scan | dynamodb:DescribeTable<br />dynamodb:DescribeStream<br />dynamodb:ListStreams<br />dynamodb:GetShardIterator<br />dynamodb:GetRecords |
| Destination Table | dynamodb:BatchWriteItem                                      | dynamodb:PutItem<br />dynamodb:UpdateItem<br />dynamodb:DeleteItem |

#### Configuration By CLI

The CLI options are present below:

```
  --config-file string       Filename for configuration yaml

  --input-region string      The input region
  --input-role-arn string    ARN of the input role
  --input-table string       Name of the input table

  --output-region string     The output region
  --output-role-arn string   ARN of the output role
  --output-table string      Name of the output table

  --backfill                 Perform the backfill operation (default true)
  --stream                   Perform the streaming operation (default true)
```

Either a config file or combination of input and output options are required.

The input and output table must differ by either name, region, or have a role arn provided.

#### Configuration By File

ddb-sync configuration file format is YAML. It describes a plan object which has an array of maps of input and output tables, their table name, region and the role ARN to access it with the appropriate permissions listed above.

An example is below.

```yaml
---
plan:
  - input:
      table: ddb-sync-source
      region: us-west-2
      role_arn: arn:aws:iam::<account_num>:role/ddb-sync-READ_ONLY_SOURCE
    output:
      table: ddb-sync-dest
      region: us-east-2
      role_arn: arn:aws:iam::<account_num>:role/ddb-sync_WRITE_ONLY_DEST
    stream:
      disabled: true
    backfill:
      disabled: false
```


## Development and Testing tools
You can use the golang script in `contrib/dynamo_writer.go` to help configure some faked items.  You'll need a table that conforms to table partition key schema.


For instance, in inseng, you can test with the source table of `ddb-sync-source`.

You can run the item writer to write items:
```console
vgo run contrib/dynamo_writer.go <table_name>
```

## Contributing

We welcome pull requests for additional functionality to add to and extend ddb-sync. If interested, write up a commit and add the "Toolsmiths" group to the functionality and drop a line in #toolsmiths notifying us that you have a commit ready for review. We will get a review in and give feedback as appropriate prior to merging. Our build system includes linting and a test suite but be sure to include further tests as a part of your request.

------
