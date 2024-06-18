## IC-Assets

A motoko library implementation of the [Assets Canister](https://github.com/dfinity/sdk/blob/master/docs/design/asset-canister-interface.md) with v2 certification.

Demo: [A simple frontend for uploading and serving certified files](https://ehvmc-gqaaa-aaaap-ahkwa-cai.icp0.io/homepage)
The code for the demo is in the [example/main.mo](./example/main.mo) file.

### Usage

You can either import the library into you canister, deploy it as a standalone canister or deploy it as a subcanister.

#### Importing the library

```motoko
    import Assets "mo:ic-assets";

    actor class() = this_canister {
        stable var assets_sstore = Assets.init_stable_store(owner);
        assets_sstore := Assets.migrate(assets_sstore);

        let assets = Assets.Assets(assets_sstore);

        /// Need to call this function the first time the canister is created
        public shared func init() : async () {
            let id = Principal.fromActor(this_canister);
            assets.set_canister_id(id);
            assets.set_streaming_callback(http_request_streaming_callback);
        };
        
        public shared ({ caller }) func store(args : Assets.StoreArgs) : async () {
            assets.store(caller, args);
        };

        ... // other functions from the Assets interface

        public query func http_request_streaming_callback(token_blob : Assets.StreamingToken) : async ?(Assets.StreamingCallbackResponse) {
            ?assets.http_request_streaming_callback(token_blob);
        };

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
                key = "/hello.txt";
                content_type = "text/plain";
                key = "/hello";
                content = "Hello, World!";
                sha256 = null;
                content_encoding = "identity";
                is_aliased = ?true;
            };

            await assets().store(args);

            let file = await assets().get({
                key = "/hello.txt";
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
            if (Text.startWith(request.url.path, "/assets/")) {
                let assets_canister_id = Principal.toText(Principal.fromActor(assets()));

                let path = Text.trimStart(request.url.path, "/assets/");
                let asset_url = assets_canister_id # ".icp0.io/" # path;

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