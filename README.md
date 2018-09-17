# DynamoDB Sync - ddb-sync

## Table of Contents
- [Introduction](#introduction)

  - [Quickstart](#quickstart)

  - [Functionality](#functionality)

  - [Problems we are solving](#problems-we-are-solving)

  - [Similar tools](#similar-tools)

- [Usage](#usage)

  - [Installation](#installation)

  - [Permission Prerequisites](#permission-prerequisites)

  - [Configuration](#configuration)

    - [Tuning](#tuning)

  - [Invocation](#invocation)

  - [Output](#output)

    - [Logging](#logging)

    - [Status Messaging](#status-messaging)

- [Development and Testing tools](#development-and-testing-tools)

- [Contributing](#contributing)


## Introduction
ddb-sync is a tool used for syncing data from a set of source tables to a set of destination
tables in DynamoDB.  It's configurable to perform the backfill operation, the stream consumption
or both by CLI or config file options.

### Quickstart
`go get gerrit.instructure.com/ddb-sync` and `ddb-sync --help` for usage

### Functionality
ddb-sync has two phases of work: streaming and backfilling. Backfill is a scan of the source
table and duplication of each record to the destination table. Stream consumes a pre-configured
DynamoDB Stream and writes all record changes to the destination table.

If streaming and backfill are configured on a table, stream runs sequentially after backfill.

**Note**: On actively written tables, the streaming phase is required to get new records synced.

ddb-sync does not verify source and destination tables consistency and an outside methodology
should be used to verify the table has been delivered as expected.

### Problems we are solving
- Table Migrations
  - Across regions
  - Across accounts
- Table synchronizations

### Similar tools
- [dynamodb-table-sync](https://www.npmjs.com/package/dynamodb-table-sync)
- [copy-dynamodb-table](https://www.npmjs.com/package/copy-dynamodb-table)
- [dynamodb-replicator](https://www.npmjs.com/package/dynamo-replicator)

## Usage

### Installation
ddb-sync is a golang binary.

Ensure you have a proper Go environment setup and then:

```command
go get gerrit.instructure.com/ddb-sync
```

### Permission Prerequisites
The IAM permissions required by the roles the application uses to read and put data to the
destination tables is listed as follows for each phase.

| Table             | Backfill Permissions                                         | Stream Permissions                                           |
| ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **Source Table**      | dynamodb:DescribeTable<br />dynamodb:Scan | dynamodb:DescribeStream<br />dynamodb:DescribeTable<br />dynamodb:GetRecords (on **<src_table_arn>/streams/\***)<br />dynamodb:GetShardIterator <br />|
| **Destination Table** | dynamodb:BatchWriteItem | dynamodb:DeleteItem<br />dynamodb:PutItem |

#### Example backfill policy statement
In order to backfill a table, a policy resembling the following must be present.
```
{
  "Action": [
    "dynamodb:DescribeTable",
    "dynamodb:Scan"
  ],
  "Resource": [
    "<input_table_arn>"
  ],
  "Effect": "Allow"
},
{
  "Action": [
    "dynamodb:BatchWriteItem"
  ],
  "Resource": [
    "<output_table_arn>"
  ],
  "Effect": "Allow"
}
```

#### Example stream policy statement
In order to stream a table, a policy resembling the following must be present.
```
{
  "Action": [
    "dynamodb:DescribeStream",
    "dynamodb:DescribeTable",
    "dynamodb:GetShardIterator"
  ],
  "Resource": [
    "<input_table_arn>"
  ],
  "Effect": "Allow"
},
{
  "Action": [
    "dynamodb:GetRecords"
  ],
  "Resource": [
    "<input_table_arn>/stream/*"
  ],
  "Effect": "Allow"
},
{
  "Action": [
    "dynamodb:DeleteItem",
    "dynamodb:PutItem"
  ],
  "Resource": [
    "<output_table_arn>"
  ],
  "Effect": "Allow"
}
```



### Configuration
ddb-sync is probably most useful when used with a config file. You can use a config file to
run many concurrent operations together. Alternatively, you can provide a single set operation
as flags to the CLI. Either a config file or a combination of input and output options are
required. In addition if using the flags the input and output table must differ by either name,
region, or have a role arn provided.

The ddb-sync configuration file format is YAML. It describes a plan object which has an array
of objects of input and output tables, their table name, region and the role ARN to access it
with the appropriate permissions listed above.

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
  - input:
      table: ddb-sync-source-2
      region: us-west-2
      role_arn: arn:aws:iam::<account_num>:role/ddb-sync-READ_ONLY_SOURCE
    output:
      table: ddb-sync-dest-2
      region: us-east-2
      role_arn: arn:aws:iam::<account_num>:role/ddb-sync_WRITE_ONLY_DEST
```

#### Tuning
**Note:** Be sure your source tables have provisioned capacity for reads and writes before using
the tool. To optimize performance you should adjust provisioned read and write capacity. Be
cautious about table sharding constraints.

During the output of the tool we note consumed write capacity units. This can be used to help
provision and scale your table with use of the tool. See the output section for more details.

It is highly recommended that you run this utility on an EC2 instance with the proper permissions
attached to the instance profile.  This will provide predictable network performance, consistent
compute resources, and keep your data off the coffee shop router.

### Invocation
Invoke the compiled binary and provide options for a run.

`ddb-sync <cli-options>`

The CLI options are present below:

```console
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

### Stopping
Backfill only operations will exit (0) upon completion of all steps.  However,
when streaming steps are enabled, the command will not ever exit.  When you've ascertained that
your streaming operations have completed, you can SIGINT for a clean shutdown.

### Output
ddb-sync logs to stdout and displays status messaging to stderr. Either can be redirected to
a file for later viewing.

For instance, `ddb-sync --config-file <config_file.yml> 1>ddb-sync-log.out` would capture the
log to a file called ddb-sync-log.out.

#### Logging
The log will display each operation on a table, relevant status to the operation. The format
is below.

```console
timestamp [src-table] => [dest-table]: Log statement
```

ddb-sync logs (with a timestamp prefix) on starting or completing actions as well as shard
transitions in the stream. An example is below.

```console
2018/06/19 11:05:27 [ddb-sync-source] ⇨ [ddb-sync-dest]: Backfill complete. 14301 items written over 5m9s
```

ddb-sync will add progress updates every 20 minutes to the log for historical convenience, e.g.:

```console
2018/06/19 11:12:26
================= Progress Update ==================
[ddb-sync-source] ⇨ [ddb-sync-dest]: Streaming: 400 items written over 20m1s
[ddb-sync-source-2] ⇨ [ddb-sync-dest-2]: Backfill in progress: 822 items written over 1m1s
====================================================
```

#### Status Messaging
The bottom portion of the output given to the user is the status messaging. It describes each
table, the operation being worked on, the write capacity unit usage rate for writing to the
destination table and the amount of records written from each operation.

An example follows:

```console
------------------------------------------------ Current Status ---------------------------------------------------
TABLE                    DETAILS              BACKFILL        STREAM                         RATES & BUFFER
⇨ ddb-sync-dest         47K items (29MiB)    -COMPLETE-      2019 written (~18h57m latent)  12 items/s ⇨ ◕ ⇨ 32 WCU/s
⇨ ddb-sync-dest-2       ~46K items (~26MiB)  -SKIPPED-       789 written (~46m35s latent)   9  items/s ⇨ ◕ ⇨ 17 WCU/s
⇨ ddb-sync-destination  ~46K items (~26MiB)  267164 written  -PENDING-                      49 items/s ⇨ ◕ ⇨ 50 WCU/s
```

## Development and Testing tools
You can use the golang script in `contrib/dynamo_writer.go` to
help configure some faked items.  You'll need a table that conforms to table partition key schema.


For instance, in inseng, you can test with the source table of `ddb-sync-source`.

You can run the item writer to write items:
```console
vgo run contrib/dynamo_writer.go <table_name>
```

## Contributing
Pull requests welcome for both bug fixes and features.

Our build system includes linting and a test suite so be sure to include further
tests as a part of your pull request.

------
