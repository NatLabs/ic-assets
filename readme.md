# Assets Library and Canister

## Introduction

A motoko library implementation of the [Assets Canister](https://github.com/dfinity/sdk/blob/master/docs/design/asset-canister-interface.md) with v2 certification. Allows you to serve files from a canister and access it via the `<canister-id>.icp0.io` domain instead of `<canister-id>.raw.ic0.io` domain.

**Demo:** [A simple frontend for uploading and serving certified files](https://ehvmc-gqaaa-aaaap-ahkwa-cai.icp0.io/homepage)

> The code for the demo is in the [example/main.mo](./example/main.mo) file.

### Asset Canister Interface

This document describes the interface with enough detail to aid in understanding how the asset canister works and in interacting with the asset canister at the code level. It does not describe the interface in sufficient detail to rise to the level of a specification.

- [Asset Canister Interface](asset-canister-interface.md) (copied from the rust implementation)

## Usage

You can either import the library into you canister, deploy it as a standalone canister or deploy it as a subcanister.

### Initialization and Configuration

#### Importing the library

There are couple things you need to do to use the Assets library in your canister:

- You need ot set the `canister_id` of the canister that will be serving the assets. This is used to verify who has access to create, update and delete assets.
- You need to define the `http_request_streaming_callback` function. A public actor function that will be called when the canister needs to stream files larger than the `2MB` transfer limit to the client. We have a helper function in the assets lib to help with this all you need to do is expose it in a public actor function. Once that is done you need to set it via `set_streamin_callback` so the library knows which function to call when streaming.
- Finally, all the previous things need to be done before the library can be used. You can add these to an init function and either call it manually when the canister is created or call it once at the top of the function you are going to use it in.

- The last function you need to define is the `http_request` function which allows users to access all of the files certified in the assets canister if they know their url

```motoko
    import Assets "mo:ic-assets";

    actor class() = this_canister {

        stable var assets_sstore = Assets.init_stable_store(owner);
        assets_sstore := Assets.upgrade(assets_sstore);

        let canister_id  = Principal.fromActor(this_canister);

        let assets = Assets.Assets(assets_sstore,);
        assets.set_canister_id(canister_id); // required

        public query func http_request_streaming_callback(token : Assets.StreamingToken) : async ?(Assets.StreamingCallbackResponse) {
            ?assets.http_request_streaming_callback(token);
        };

        assets.set_streaming_callback(http_request_streaming_callback); // required

        public query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
            assets.http_request(request);
        };
    }
```

#### Deploying as a standalone canister

- `git clone https://github.com/NatLabs/ic-assets`
- `mops install`
- `dfx start --background`
- `dfx deploy assets_canister`
- `dfx canister call assets_canister init`

#### Deploying as a subcanister

```motoko
    import Text "mo:base/Text";
    import Option "mo:base/Option";
    import Cycles "mo:base/ExperimentalCycles";

    import Assets "mo:ic-assets";
    import AssetsCanister "mo:ic-assets/Canister";

    actor {
        stable var opt_assets : ?AssetsCanister.AssetsCanister = null;

        func assets() : AssetsCanister.AssetsCanister {
            let ?a = opt_assets; return a;
        };

        public shared func create_assets() : async () {
            switch(opt_assets) {
                case (?_) { };
                case null {
                    Cycles.add(1_000_000_000_000);
                    opt_assets := ?(await AssetsCanister.AssetsCanister(#Init({})););
                    assets().init();
                };
            };
        };

        public shared func store_text_file(): async (){
            let args = AssetsCanister.StoreArgs {
                key = "/assets/hello.txt";
                content_type = "text/plain";
                content = "Hello, World!";
                sha256 = null;
                content_encoding = "identity";
                is_aliased = ?true;
            };

            await assets().store(args);

            let file = await assets().get({
                key = "/assets/hello.txt";
                accept_encodings = [];
            });

            assert result.content == "Hello, World!";
            assert result.content_type == "text/plain";
            assert result.content_encoding == "identity";
            assert result.total_length == 13;
            assert Option.isSome(result.sha256);
        };

        /// Redirects all requests with the prefix '/assets/' to the assets canister
        public query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
            if (Text.startWith(request.url, "/assets/")) {
                let assets_canister_id = Principal.toText(Principal.fromActor(assets()));

                let asset_url = assets_canister_id # ".icp0.io/" # request.url;

                // redirect the request to the asset canister
                let http_response = {
                    status_code = 307;
                    headers = [("Location", asset_url)];
                    body = "";
                    upgrade = null;
                    streaming_strategy = null;
                };

                return http_response;

                // return await assets().http_request(request); - doesn't work, as CertifiedData.getCertificate() cannot be called in either composite query calls or inter-canister calls
            };

            return {
                status_code = 404;
                headers = [];
                body = "Not found";
                upgrade = null;
                streaming_strategy = null;
            };
        };
    }
```

## Usage Examples

A list of example cases for using the assets library.

### Storing Assets

> Check out the [storing assets section](./asset-canister-interface.md#storing-assets) of the asset canister interface to see how to store assets in the canister.

#### Storing Small Assets

For storing small assets less than the `2MB` limit you can use the `store` function.

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

- Content Encoding

- Aliasing Assets

#### Storing Large Assets

##### Batch Updates

The usual method of updating data in the asset canister is by calling the following methods:

1. [create_batch()](#method-create_batch) once.
2. [create_chunk()](#method-create_chunk) one or more times, which can occur concurrently.
3. [commit_batch()](#method-commit_batch) once with the batch ID from step 1.

Unlike the rust implementation that requires you to call the `commit_batch` multiple times because of the instruction limit.
You only need to call it once and it will create or update all the assets in the batch.
We are able to avoid running into the instruction limit by breaking the request in the batch into multiple asynchronous calls internally to certify and store the updated assets.

> My arguement
> Is that choosing this method instead of the method in the rust implementation is that it is more user friendly and easier to use.
> Also these are update methods as they change the data in the canister so each call will have the same 2s delay required for update calls as individual hidden async calls required to process the batch request.
> The only difference is the user doesn't have to call it multiple times
> An added benefit is if the user does not need to retrieve the data immediately they don't need to wait for the result of the `commit_batch` request. Instead they can just call it without using `await` and the data will be certified and updated in the background without blocking the user.

> show an example, batching multiple files

- **Creating an Asset**
  Assets are stored by their content encoding, so a single asset can store multiple versions of the same file with different content encodings. The encodings could be one of `identity`, `gzip`, or `br`, where `identity` is for the raw or plain text file while `gzip` and `br` are for compressed files.

```motoko

let hello_file = Assets.StoreArgs {
    key = "/assets/hello.txt";
    content_type = "text/plain";
    content = "Hello, World!";
    sha256 = null;
    content_encoding = "identity";
    is_aliased = ?true;
};

let batch_id = await assets.create_batch();

let hello_chunks = Assets.split_into_chunks(hello_file.content);
let hello_chunk_ids_in_order = Buffer.Buffer(hello_chunks.size());

for (chunk in hello_chunks.vals()) {
    let chunk_id = await assets.create_chunk(batch_id, chunk);
    hello_chunk_ids_in_order.add(chunk_id);
};

let create_hello_file_args = {
    key = hello_file.key;
    content_type = hello_file.content_type;
    max_age = null;
    headers = null;
    enable_aliasing = null;
    allow_raw_access = ?false;
};

let set_hello_file_content_args = {
    key = hello_file.key;
    content_encoding = hello_file.content_encoding;
    chunk_ids = Buffer.toArray(hello_chunk_ids_in_order);
    sha256 = null;
};

let operations = [
    #CreateAssetArguments(create_hello_file_args),
    #SetAssetContentArguments(set_hello_file_content_args),
];

await assets.commit_batch(batch_id, operations);

```

You can easily create multiple files in this batch request by uploading the chunks of each file like in the example and adding the operations to the operations array.

```motoko
    // ... upload goodbye file

    let operations = [
        #CreateAssetArguments(create_hello_file_args),
        #SetAssetContentArguments(set_hello_file_content_args),
        #CreateAssetArguments(create_goodbye_file_args),
        #SetAssetContentArguments(set_goodbye_file_content_args),
        ...
    ];

    await assets.commit_batch(batch_id, operations);
```

- **Editing Assets**
  The asset library allows you to update the contents of an asset by using a `#SetAssetContentArguments` operation in a batch request. This operation overwrites the existing content of the asset with the new content provided in the operation.

  ```motoko

    let new_hello_file_content = "👋 Hello, World!";

    // if the content is larger than 2MB
    let chunks = Assets.split_into_chunks(new_hello_file_content);

    let batch_id = await assets.create_batch();
    let chunks_in_order = Buffer.Buffer(chunks.size());

    for (chunk in chunks.vals()) {
        let chunk_id = await assets.create_chunk(batch_id, chunk);
        chunks_in_order.add(chunk_id);
    };

    let set_hello_file_content_args = {
        key = "/assets/hello.txt";
        content_encoding = "identity";
        chunk_ids = Buffer.toArray(chunks_in_order);
        sha256 = null;
    };

    await assets.commit_batch(
        batch_id,
        [#SetAssetContentArguments(set_hello_file_content_args)]
    );

  ```

- **Remove content encoding of an asset**

  ```motoko

    let batch_id = await assets.create_batch();

    await assets.commit_batch(
        batch_id,
        [#UnsetAssetContent("/assets/hello.txt", "identity")]
    );

  ```

- **Delete Asset**

```motoko

    let batch_id = await assets.create_batch();

    await assets.commit_batch(
        batch_id,
        [#DeleteAsset("/assets/hello.txt")]
    );

```
