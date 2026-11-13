# External Storage

> [!WARNING]
> External storage is experimental and may change in the future.

This sample offloads large payloads to S3-compatible object storage with the SDK's external storage
support and the `temporalio-aws-s3driver` crate. A zlib `PayloadCodec` compresses every payload
first, so what is stored inline in Temporal and in S3 is compressed.

**Scenario:** The workflow receives an order count and calls a `fetch_orders` activity that returns
that many orders. The list, about 400 KiB even after compression, is passed to a second
`summarize_orders` activity, and the workflow returns its small `BatchSummary`.

The codec compresses each payload, and the SDK then compares the compressed size against the default
256 KiB threshold. Payloads still above it are stored in S3 and replaced in history by a small
reference. The workflow's own input and result are a few bytes and stay inline.

A mock S3 server ([s3_mock.rs](s3_mock.rs)) is included, so the sample runs locally without an AWS
account or Docker. To use real S3 instead, see [Using real S3](#using-real-s3).

## Running this sample

Run the `cargo` commands from the `crates/aws-s3driver/` directory.

1. `temporal server start-dev` to start the Temporal server.
2. In another terminal, start the mock S3 server. It listens on `localhost:9100`, serves the
   `temporal-payloads` bucket, and keeps objects in memory until it stops.

   ```bash
   cargo run --example external-storage-s3-mock
   ```

3. In another terminal, start the worker:

   ```bash
   cargo run --example external-storage-worker
   ```

4. In another terminal, run the workflow:

   ```bash
   cargo run --example external-storage-starter
   ```

   Example output:

   ```text
   Started workflow, run_id: Some("<run-id>")
   Processed 200 orders weighing 5000.0 kg in total
   ```

   The mock S3 server logs each request the driver makes:

   ```text
   HEAD /temporal-payloads/v0/ns/default/wt/ProcessOrderBatchWorkflow/wi/external-storage-workflow-id/ri/<run-id>/d/sha256/<hash> status=404 duration=1.4ms
   PUT /temporal-payloads/v0/ns/default/wt/ProcessOrderBatchWorkflow/wi/external-storage-workflow-id/ri/<run-id>/d/sha256/<hash> status=200 duration=13.3ms
   GET /temporal-payloads/v0/ns/default/wt/ProcessOrderBatchWorkflow/wi/external-storage-workflow-id/ri/<run-id>/d/sha256/<hash> status=200 duration=6.4ms
   HEAD /temporal-payloads/v0/ns/default/wt/ProcessOrderBatchWorkflow/wi/external-storage-workflow-id/ri/<run-id>/d/sha256/<hash> status=200 duration=6.3ms
   GET /temporal-payloads/v0/ns/default/wt/ProcessOrderBatchWorkflow/wi/external-storage-workflow-id/ri/<run-id>/d/sha256/<hash> status=200 duration=6.2ms
   ```

   The worker stores the `fetch_orders` result (`HEAD` finds no object, `PUT` uploads it), and the
   workflow retrieves it. When the workflow passes the same list to `summarize_orders`, the driver
   finds the object already stored and skips the upload, and the activity retrieves it.

5. Run `temporal workflow show` to see how the payloads were stored:

   ```bash
   temporal workflow show --workflow-id external-storage-workflow-id
   ```

   | Payload                   | Type                    | Storage           |
   | ------------------------- | ----------------------- | ----------------- |
   | Workflow input            | `usize`                 | Inline            |
   | `fetch_orders` input      | `usize`                 | Inline            |
   | `fetch_orders` result     | `Vec<Order>` (~400 KiB) | **External (S3)** |
   | `summarize_orders` input  | `Vec<Order>` (~400 KiB) | **External (S3)** |
   | `summarize_orders` result | `BatchSummary`          | Inline            |
   | Workflow result           | `BatchSummary`          | Inline            |

   The two offloaded payloads are the order list passed from one activity to the next, which is
   what external storage is for: large data that flows between activities without bloating
   workflow history.

## How it works

[storage.rs](storage.rs) builds the `DataConverter` that both the worker and the starter set on their
`ClientOptions`. The worker uses its client's data converter, so it needs no other configuration.

```rust
let driver = S3StorageDriver::new(
    S3StorageDriverOptions::builder()
        .client(Arc::new(AwsSdkS3StorageDriverClient::new(s3_client)))
        .bucket(bucket)
        .build(),
);
let data_converter = DataConverter::new(
    PayloadConverter::default(),
    DefaultFailureConverter::default(),
    ZlibCodec,
)
.with_external_storage(ExternalStorage::new(Arc::new(driver))?);
```

When encoding a payload, the SDK:

1. Serializes the value to a `Payload`.
2. Runs the codec, which compresses the payload.
3. Compares the compressed size against the threshold (default 256 KiB, set with
   `ExternalStorage::with_payload_size_threshold`).
4. If it is at or above the threshold, stores the compressed payload in S3 and replaces it with a
   reference.

Decoding reverses these steps, retrieving from S3 and then decompressing.

Every process that reads or writes this workflow's payloads must use the same codec and external
storage configuration, so each can read what the others wrote.

### Using real S3

Set `EXTERNAL_STORAGE_S3_BUCKET` to your bucket's name in the worker's and the starter's terminals,
and skip the mock S3 server:

```bash
export EXTERNAL_STORAGE_S3_BUCKET=<your-bucket>
```

The S3 client then takes its region and credentials from the usual AWS configuration, such as the
`AWS_REGION` and `AWS_PROFILE` environment variables or `~/.aws`. Set the variable in both
terminals: a worker and a starter that disagree would write payloads where the other can't read
them. See the [`temporalio-aws-s3driver` README](../../README.md) for the IAM
permissions the driver needs.

### Why a codec

External storage works without a codec. Remove `ZlibCodec` and the order list is still offloaded, only
uncompressed. The sample uses one because many applications already run a codec, and the two
interact:

- The threshold applies to the encoded payload. Compression alone can keep a payload small enough to
  stay inline, so the threshold is a limit on the compressed size, not the size of your data.
- The driver stores the encoded payload. With an encryption codec in place of `ZlibCodec`, the
  objects in S3 are encrypted too.
- The reference that replaces an offloaded payload in history is not passed through the codec, so
  the bucket and key it names are visible to anyone who can read the history.
