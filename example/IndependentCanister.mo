import Text "mo:base/Text";
import Option "mo:base/Option";
import Cycles "mo:base/ExperimentalCycles";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";

import Assets "../src";
import AssetsCanister "../src/Canister";

actor {

    // will fail unless create_assets_canister() is called first
    stable var assets : AssetsCanister.AssetsCanister = actor ("aaaaa-aa");

    public shared func create_assets_canister() : async () {
        if (
            Principal.toText(Principal.fromActor(assets)) == "aaaaa-aa"
        ) {
            assets := await AssetsCanister.AssetsCanister(#Init({}));
        };
    };

    public shared func store_hello_file() : async () {
        let args : Assets.StoreArgs = {
            key = "/assets/hello.txt";
            content_type = "text/plain";
            content = "Hello, World!";
            sha256 = null;
            content_encoding = "identity";
            is_aliased = ?true;
        };

        await assets.store(args);

        let file = await assets.get({
            key = "/assets/hello.txt";
            accept_encodings = ["identity"];
        });

        assert file.content == "Hello, World!";
        assert file.content_type == "text/plain";
        assert file.content_encoding == "identity";
        assert file.total_length == 13;
        assert Option.isSome(file.sha256);
    };

    /// Redirects all requests with the prefix '/assets/' to the assets canister
    public query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
        Debug.print("Received request: " # request.url);
        if (Text.startsWith(request.url, #text "/assets/")) {
            let assets_canister_id = Principal.toText(Principal.fromActor(assets));

            let asset_url = assets_canister_id # ".icp0.io/" # request.url;

            // redirect the request to the asset canister
            let http_response : Assets.HttpResponse = {
                status_code = 307;
                headers = [("Location", asset_url)];
                body = "";
                upgrade = null;
                streaming_strategy = null;
            };

            return http_response;

            // return await assets.http_request(request); - doesn't work, as CertifiedData.getCertificate() cannot be called in either composite or inter-canister query calls
        };

        return {
            status_code = 404;
            headers = [];
            body = "Not found";
            upgrade = null;
            streaming_strategy = null;
        };
    };
};
