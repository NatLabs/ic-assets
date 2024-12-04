import Cycles "mo:base/ExperimentalCycles";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Char "mo:base/Char";
import Option "mo:base/Option";
import Error "mo:base/Error";
import Principal "mo:base/Principal";

import Map "mo:map/Map";
import Vector "mo:vector";
import HttpParser "mo:http-parser";
import Itertools "mo:itertools/Iter";
// import Web "mo:web-io";

import Assets "../src";
import { get_fallback_page } "FallbackPage";
import { homepage } "Homepage";
import V0 "../src/Migrations/V0/upgrade";

shared ({ caller = owner }) actor class () = this_canister {

    type File = {
        content_type : Text;
        chunk_ids : Buffer.Buffer<Nat>;
        exists : Bool;
        var is_committed : Bool;
    };

    let { nhash; thash } = Map;

    type Map<K, V> = Map.Map<K, V>;
    let current_uploads : Map<Nat, Map<Text, File>> = Map.new();

    let canister_id = Principal.fromActor(this_canister);
    stable var assets_sstore = Assets.init_stable_store(canister_id, owner);
    stable let assets_sstore_2 = Assets.upgrade(assets_sstore_clone);

    let assets = Assets.Assets(assets_sstore_2);

    assets.set_canister_id(canister_id);

    public query func http_request_streaming_callback(token : Assets.StreamingToken) : async (Assets.StreamingCallbackResponse) {
        assets.http_request_streaming_callback(token);
    };

    assets.set_streaming_callback(http_request_streaming_callback);

    public query func get_certified_endpoints() : async [Assets.EndpointRecord] {
        assets.get_certified_endpoints();
    };

    // Acts as the init function for the canister.
    // > Warning: using the system timer will cause the Timer.mo library to not function properly.
    // using this method because post_upgrade doesn't work during init, or the first upgrade but only after the first upgrade
    system func timer(setGlobalTimer : Nat64 -> ()) : async () {
        await* certify_fallback_page();
        await* update_homepage();
        Debug.print("Re-Certified 404 page and updated homepage");
    };

    func create_batch() : (Assets.BatchId) {
        let #ok({ batch_id }) = assets.create_batch(owner, {});
        let new_batch = Map.new<Text, File>();
        ignore Map.put(current_uploads, nhash, batch_id, new_batch);
        batch_id;
    };

    func upload_chunks(batch_id : Assets.BatchId, chunks : [Blob]) : [Assets.ChunkId] {
        let chunk_ids = Buffer.Buffer<Assets.ChunkId>(8);

        for (chunk in chunks.vals()) {
            let #ok({ chunk_id }) = assets.create_chunk(owner, { batch_id = batch_id; content = chunk });
            chunk_ids.add(chunk_id);
        };

        return Buffer.toArray(chunk_ids);
    };

    func upload(batch_id : Nat, file_name : Text, content_type : Text, chunks : [Blob]) : async* () {
        let ?batch = Map.get(current_uploads, nhash, batch_id) else Debug.trap("Batch not found");
        let file = switch (Map.get(batch, thash, file_name)) {
            case (?file) file;
            case (null) {

                let file : File = {
                    content_type;
                    chunk_ids = Buffer.Buffer<Nat>(8);
                    exists = assets.exists(file_name);
                    var is_committed = false;
                };

                ignore Map.put(batch, thash, file_name, file);
                file;
            };
        };

        let chunk_ids = upload_chunks(batch_id, chunks);
        for (chunk_id in chunk_ids.vals()) {
            file.chunk_ids.add(chunk_id);
        };

    };

    func commit_batch(batch_id : Assets.BatchId) : async* () {
        let ?batch = Map.remove(current_uploads, nhash, batch_id) else Debug.trap("Batch not found");
        let operations = Buffer.Buffer<Assets.BatchOperationKind>(8);

        for ((file_name, file) in Map.entries(batch)) {
            let { content_type; chunk_ids; exists } = file;

            if (not exists) {
                operations.add(
                    #CreateAsset({
                        key = file_name;
                        content_type;
                        max_age = ?100_000_000_000;
                        headers = ?[];
                        enable_aliasing = if (file_name == "/homepage") ?true else ?false;
                        allow_raw_access = ?true;
                    }),

                );
            };

            operations.add(#SetAssetContent({ key = file_name; content_encoding = "identity"; chunk_ids = Buffer.toArray(chunk_ids); sha256 = null }));
        };

        // Debug.print("Committing batch: " # debug_show batch_id);
        // Debug.print("Operations: " # debug_show Buffer.toArray(operations));

        ignore (
            await* assets.commit_batch(
                owner,
                {
                    batch_id;
                    operations = Buffer.toArray(operations);
                },
            )
        );

    };

    func update_homepage() : async* () {
        let files = (assets.list({}));
        let root = homepage(files);

        let batch_id = create_batch();
        await* upload(batch_id, "/homepage", "text/html", [Text.encodeUtf8(root)]);
        await* commit_batch(batch_id);
    };

    func certify_fallback_page() : async* () {
        let _fallback_page = get_fallback_page();

        let batch_id = create_batch();
        await* upload(batch_id, "/fallback/index.html", "text/html", [Text.encodeUtf8(_fallback_page)]);
        await* commit_batch(batch_id);
    };

    public composite query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {

        if (request.method == "POST" or request.method == "PUT" or request.method == "DELETE") {
            return {
                upgrade = ?true;
                headers = [];
                status_code = 200;
                body = "";
                streaming_strategy = null;
            };
        };

        switch (assets.http_request(request)) {
            case (#ok(response)) response;
            case (#err(error_message)) {
                Debug.print("Error: " # error_message);
                Debug.print(request.url);
                if (Text.startsWith(request.url, #text("/favicon.ico"))) return response(200, "");

                Assets.redirect_to(assets, request, "/404.html", [("Set-Cookie", "__ic_asset_motoko_lib_error__=" # error_message)]);
            };
        };
    };

    func response(status_code : Nat16, message : Text) : Assets.HttpResponse {
        {
            status_code;
            body = Text.encodeUtf8(message);
            headers = [];
            streaming_strategy = null;
            upgrade = null;
        };
    };

    public func http_request_update(_request : Assets.HttpRequest) : async Assets.HttpResponse {
        try {
            let url = HttpParser.URL(_request.url, HttpParser.Headers([]));
            // Debug.print("request after parsing: " # debug_show { url = url.original });

            if (_request.method == "POST" and url.path.array[0] == "upload" and url.path.array.size() == 1) {
                // Debug.print("new request: creating batch");
                let batch_id = create_batch();
                // Debug.print("Create Upload batch: " # debug_show batch_id);
                return response(200, debug_show batch_id);

            } else if (_request.method == "PUT" and url.path.array[0] == "upload") {
                // Debug.print("uploading chunks: " # debug_show url.original);

                let batch_id = nat_from_text(url.path.array[1]);
                let chunk_id = nat_from_text(url.path.array[2]);

                let ?filename = url.queryObj.get("filename");

                // Debug.print("upload chunks: " # debug_show ({ batch_id; filename; chunk_id }));

                let files_batch = switch (Map.get(current_uploads, nhash, batch_id)) {
                    case (?files_batch) { files_batch };
                    case (null) {
                        let files_batch = Map.new<Text, File>();
                        ignore Map.put(current_uploads, nhash, batch_id, files_batch);
                        files_batch;
                    };
                };

                let file = switch (Map.get(files_batch, thash, filename)) {
                    case (?file) file;
                    case (null) {
                        let ?content_type = url.queryObj.get("content-type");
                        // Debug.print("Create new file: " # debug_show ({ batch_id; filename; content_type }));
                        let ?total_chunks_text = (url.queryObj.get("total_chunks"));
                        let total_chunks = nat_from_text(total_chunks_text);

                        let file : File = {
                            content_type;
                            chunk_ids = Buffer.fromArray<Nat>(
                                Array.tabulate<Nat>(
                                    total_chunks,
                                    func(i : Nat) : Nat { 0 },
                                )
                            );
                            exists = assets.exists(filename);
                            var is_committed = false;
                        };

                        ignore Map.put(files_batch, thash, filename, file);
                        file;

                    };
                };

                let uploaded_chunk_ids = upload_chunks(batch_id, [_request.body]);

                file.chunk_ids.put(chunk_id, uploaded_chunk_ids[0]);

                return response(200, "Successfully uploaded chunk " # debug_show ({ batch_id; filename; chunk_id }));

            } else if (_request.method == "POST" and url.path.array[0] == "upload" and url.path.array[1] == "commit") {
                let parts = url.path.array;
                // Debug.print("committing: " # debug_show parts);

                let batch_id = nat_from_text(parts[2]);
                let ?filename = url.queryObj.get("filename");

                // Debug.print("Commit Upload batch: " # debug_show batch_id);

                let ?files_batch = Map.get(current_uploads, nhash, batch_id) else return response(404, "Batch not found");
                let ?file = Map.get(files_batch, thash, filename) else return response(404, "File not found");

                file.is_committed := true;

                if (Itertools.all(Map.vals(files_batch), func(file : File) : Bool { file.is_committed })) {

                    // Debug.print("Committing batch: " # debug_show batch_id);
                    await* commit_batch(batch_id);
                    await* update_homepage();
                };

                ignore Map.remove(current_uploads, nhash, batch_id);

                return response(200, debug_show batch_id);

            } else if (_request.method == "DELETE" and url.path.array[0] == "upload") {

                let ?key = url.queryObj.get("filename");

                // Debug.print("deleting: " # debug_show key);
                switch (assets.delete_asset(owner, { key })) {
                    case (#ok()) {};
                    case (#err(msg)) {
                        Debug.print("Error: " # msg);
                        return response(404, msg);
                    };
                };

                await* update_homepage();

                return response(200, "Successfully deleted " # debug_show key);
            };

            return response(404, "Not found");
        } catch (e) {
            Debug.print("this might be a trap!");
            Debug.print("Error: " # debug_show (Error.code(e), Error.message(e)));
            throw e;
        };
    };

    func nat_from_text(text : Text) : Nat {
        var n : Nat = 0;

        for (c in text.chars()) {
            if (Char.isDigit(c)) {
                n := n * 10 + Nat32.toNat(Char.toNat32(c) - Char.toNat32('0'));
            } else {
                Debug.trap("Invalid character in number: " # Char.toText(c));
            };
        };

        return n;
    };

};
