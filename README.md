# Assets Library and Canister

## Fork Information

This repository is a fork of [ic-assets](https://github.com/NatLabs/ic-assets) created by [Tomijaga](https://github.com/tomijaga) under the [NatLabs](https://github.com/NatLabs) organization.

The primary difference between this fork and the original repository is that we've published to a different package name in the mops.toml file. All core functionality remains the same, and we maintain the original MIT license.

Our goal is to contribute improvements back to the original repository while maintaining our own package for specific use cases.

## Introduction

A Motoko library implementation of the [Assets Canister](https://github.com/dfinity/sdk/blob/master/docs/design/asset-canister-interface.md) with v2 certification. It allows you to serve files from a canister and access them via the `<canister-id>.icp0.io` domain instead of the `<canister-id>.raw.ic0.io` domain.

**Demo:** [A simple frontend for uploading and serving certified files](https://zus34-xqaaa-aaaap-anvkq-cai.icp0.io/homepage)

> The code for the demo is in the [example/main.mo](./example/main.mo) file.

## Getting Started

### Asset Canister Interface

This is the documentation for the assets canister implemented in Rust by the Dfinity team. It has been copied here for reference. It is recommended to read it to understand how the asset canister works and how to use it. The Motoko implementation is a direct port of the Rust implementation with some changes to make it more user-friendly.

- [Asset Canister Interface](asset-canister-interface.md)

### Initialization and Configuration

#### Importing the Library

- Install via mops: `mops add ic-assets`
- Add the following code snippet to your canister:

```motoko
    import Assets "mo:ic-assets";

    shared ({ caller = owner }) actor class () = this_canister {

        let canister_id  = Principal.fromActor(this_canister);

        stable let assets_sstore_1 = Assets.init_stable_store(canister_id, owner);
        assets_sstore_1 := HttpAssets.upgrade_stable_store(assets_sstore_1);
        let assets = Assets.Assets(assets_sstore_1, null);

        public query func http_request_streaming_callback(token : Assets.StreamingToken) : async Assets.StreamingCallbackResponse {
            switch (assetStore.http_request_streaming_callback(token)) {
                case (#err(e)) throw Error.reject(e);
                case (#ok(response)) response;
            };
        };

        assets.set_streaming_callback(http_request_streaming_callback); // required

        public query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
            assets.http_request(request);
        };
    }
```

- The `init_stable_store` function initializes the stable heap store for the assets library so it's persistent across canister upgrades. It takes the `canister_id` and `owner` as arguments and grants them **#Commit** permission access.
- The `http_request_streaming_callback` function is a callback function that the assets library uses to stream files larger than the `2MB` transfer limit to the client. You need to expose this function as a public function in your canister and pass it to the assets library.

## Differences from the Rust Implementation

### Batch Operations

> Check out the [batch updates section](./asset-canister-interface.md#batch-updates) of the asset canister interface.

We've simplified the process to create and commit a batch request in the asset canister by eliminating the need to call `commit_batch` multiple times. Instead, `commit_batch` is now an asynchronous method that handles multiple hidden async calls to certify and store the assets data if they exceed the instruction limit.

1. [create_batch()](#method-create_batch) once to create a batch ID.
2. [create_chunk()](#method-create_chunk) one or more times, can be called concurrently.
3. [commit_batch()](#method-commit_batch) once to commit the batch.

This change allows you to call `commit_batch` once, and the assets library will handle the rest. If you don't need confirmation that the batch was successful, you can execute `commit_batch` without waiting for the result. This is beneficial for larger files as they can take longer to hash and certify their contents before storing them in the canister. The execution time is identical to the original method.

### Compute Evidence

> Check out the [batch updates by proposal section](./asset-canister-interface.md#batch-updated-by-proposal) of the asset canister interface.

Similar to batch operations, we've simplified `compute_evidence()` so that you only need to call it once to get the evidence for the batch request.

1. [create_batch()](#method-create_batch) once.
2. [create_chunk()](#method-create_chunk) one or more times, which can occur concurrently.
3. [propose_commit_batch()](#method-propose_commit_batch) once.
4. [compute_evidence()](#method-compute_evidence) once.

## Usage Examples

### Storing Small Assets

For storing assets less than the `2MB` limit you can simple use the `store` function.

```motoko
    let args = Assets.StoreArgs {
        key = "/assets/hello.txt";
        content_type = "text/plain";
        content = "Hello, World!";
        sha256 = null;
        content_encoding = "identity";
        is_aliased = ?true;
    };

    await assets.store(args);
```

### Storing Large Assets via Batch Requests

#### **Creating an Asset**

Assets are stored by their content encoding, so a single asset can store multiple versions of the same file with different content encodings. The encodings could be one of `identity`, `gzip`, or `br`, where `identity` is for the raw or plain text file while `gzip` and `br` are for compressed files.

```motoko

let hello_world_file = Assets.StoreArgs {
    key = "/assets/hello.txt";
    content_type = "text/plain";
    content = "Hello, World!";
    sha256 = null;
    content_encoding = "identity";
    is_aliased = ?true;
};

let batch_id = await assets.create_batch();

let hello_world_chunks = Assets.split_into_chunks(hello_world_file.content);
let hello_world_chunk_ids_in_order = Buffer.Buffer(hello_world_chunks.size());

for (chunk in hello_world_chunks.vals()) {
    let chunk_id = await assets.create_chunk(batch_id, chunk);
    hello_world_chunk_ids_in_order.add(chunk_id);
};

let create_hello_world_file_args = {
    key = hello_world_file.key;
    content_type = hello_world_file.content_type;
    max_age = null;
    headers = null;
    enable_aliasing = null;
    allow_raw_access = ?false;
};

let set_hello_world_file_content_args = {
    key = hello_world_file.key;
    content_encoding = hello_world_file.content_encoding;
    chunk_ids = Buffer.toArray(hello_world_chunk_ids_in_order);
    sha256 = null;
};

let operations = [
    #CreateAssetArguments(create_hello_world_file_args),
    #SetAssetContentArguments(set_hello_world_file_content_args),
];

await assets.commit_batch(batch_id, operations);

```

You can easily create multiple files in this batch request by uploading the chunks of each file like in the example and adding the operations to the operations array.
Note that the operations have to be ordered correctly to ensure that the asset is created before the content is set.

```motoko
    // ... upload html file chunks

    let operations = [
        #CreateAssetArguments(create_hello_world_file_args),
        #SetAssetContentArguments(set_hello_world_file_content_args),
        #CreateAssetArguments(create_html_file_args),
        #SetAssetContentArguments(set_html_file_content_args),
        ...
    ];

    await assets.commit_batch(batch_id, operations);
```

#### **Editing Assets**

The asset library allows you to update the contents of an asset by using a `#SetAssetContentArguments` operation in a batch request. This operation overwrites the existing content of the asset with the new content provided in the operation.

```motoko

  let new_hello_world_file_content = "👋 Hello, World!";

  let chunks = Assets.split_into_chunks(new_hello_world_file_content);

  let batch_id = await assets.create_batch();
  let chunks_in_order = Buffer.Buffer(chunks.size());

  for (chunk in chunks.vals()) {
      let chunk_id = await assets.create_chunk(batch_id, chunk);
      chunks_in_order.add(chunk_id);
  };

  let set_hello_world_file_content_args = {
      key = "/assets/hello.txt";
      content_encoding = "identity";
      chunk_ids = Buffer.toArray(chunks_in_order);
      sha256 = null;
  };

  await assets.commit_batch(
      batch_id,
      [#SetAssetContentArguments(set_hello_world_file_content_args)]
  );

```

#### **Remove content encoding of an asset**

```motoko

  let batch_id = await assets.create_batch();

  await assets.commit_batch(
      batch_id,
      [#UnsetAssetContent("/assets/hello.txt", "identity")]
  );

```

#### **Delete Asset**

```motoko

    let batch_id = await assets.create_batch();

    await assets.commit_batch(
        batch_id,
        [#DeleteAsset("/assets/hello.txt")]
    );

```

## Unit Testing

- Install [mops](https://mops.one)
- Install zx with `npm install -g zx`
- Install dfinity's [`idl2json` package](https://github.com/dfinity/idl2json?tab=readme-ov-file#with-cargo-install)
- Run the following commands:

```bash
    dfx start --background
    zx -i ./z-scripts/canister-tests.mjs
```

## Credits & References

- [Dfinity SDK](https://github.com/dfinity/sdk/tree/master/src/canisters/frontend)
- [Response Verification Standard](https://github.com/dfinity/interface-spec/blob/master/spec/http-gateway-protocol-spec.md#response-verification)
- Libraries: [ic-certification](https://github.com/nomeata/ic-certification), [sha2](https://mops.one/sha2)
