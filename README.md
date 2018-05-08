# DDB-SYNC
This makes moving DynamoDB content between tables as simple as we can get.

## Usage
`ddb-sync` is a golang binary. Pull down the repo from gerrit and install it by running
`vgo install`.

_**NOTE:** Until `vgo` is merged into an official Go release, it must be installed separately. The
standard `go` toolchain can be used to install `vgo`: `go get -u golang.org/x/vgo`._

### Role permission requirements
If you want to use role assumption to gain access, you'll need to provide the following permissions.

#### Source Table (and stream)
##### Backfill permissions
DescribeTable
Scan

##### Stream permissions
DescribeTable
DescribeStream
ListStreams

#### Destination Table
##### Backfill permissions
BatchWriteItem
DescribeTable

##### Stream permissions
PutItem
DeleteItem


### CLI options
A plan with a single input and output table can be configured via the CLI using the following
options. If multiple tables need to be specified, a config file can be used (See
[Configuration file](#Configurationfile) below).

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

### Configuration file
An alternative to enumerating fields at the CLI is to pass a configuration yaml containing a set of
plans for sync operations.

Example:

```
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
