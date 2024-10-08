import Array "mo:base/Array";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Iter "mo:base/Iter";
import Blob "mo:base/Blob";
import Time "mo:base/Time";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Order "mo:base/Order";
import Text "mo:base/Text";
import Debug "mo:base/Debug";
import Error "mo:base/Error";
import Buffer "mo:base/Buffer";

import Set "mo:map/Set";
import Map "mo:map/Map";
import IC "mo:ic";
import CertifiedAssets "mo:certified-assets/Stable";
import Itertools "mo:itertools/Iter";
import Sha256 "mo:sha2/Sha256";
import Vector "mo:vector";
import HttpParser "mo:http-parser";
import Hex "mo:encoding/Hex";

import Utils "Utils";
import T "Types";
import Migrations "Migrations";

// import Evidence "Evidence";

module {

    type Map<K, V> = Map.Map<K, V>;
    type Set<V> = Set.Set<V>;
    type Buffer<V> = Buffer.Buffer<V>;
    type Result<T, E> = Result.Result<T, E>;
    type Time = Time.Time;
    type Vector<A> = Vector.Vector<A>;
    type Order = Order.Order;
    type Assets = T.Assets;
    type AssetEncoding = T.AssetEncoding;
    type Batch = T.Batch;
    type Key = T.Key;
    type Chunk = T.Chunk;

    type URL = HttpParser.URL;
    let URL = HttpParser.URL;
    let Headers = HttpParser.Headers;

    let { ic } = IC;

    let { phash } = Set;
    let { thash; nhash } = Map;

    type StableStore = Migrations.StableStoreV0;

    public func get_permission_set(self : StableStore, permission : T.Permission) : Set<Principal> {
        switch (permission) {
            case (#Commit) self.commit_principals;
            case (#Prepare) self.prepare_principals;
            case (#ManagePermissions) self.manage_permissions_principals;
        };
    };

    public func has_permission(self : StableStore, principal : Principal, permission : T.Permission) : Bool {
        let permission_set = get_permission_set(self, permission);
        return Set.has(permission_set, phash, principal);
    };

    public func has_permission_result(self : StableStore, principal : Principal, permission : T.Permission) : Result<(), Text> {
        if (has_permission(self, principal, permission)) return #ok();

        #err("Caller does not have " # debug_show (permission) # " permission.");
    };

    public func can_perform_action(self : StableStore, principal : Principal, action : T.Permission) : Result<(), Text> {
        var bool = has_permission(self, principal, action);
        bool := bool or (action == #Prepare and has_permission(self, principal, #Commit));

        if (bool) return #ok();
        #err("Caller does not have " # debug_show (action) # " permission.");
    };

    public func can_prepare(self : StableStore, principal : Principal) : Result<(), Text> {
        can_perform_action(self, principal, #Prepare);
    };

    public func can_commit(self : StableStore, principal : Principal) : Result<(), Text> {
        can_perform_action(self, principal, #Commit);
    };

    public func get_permission_list(self : StableStore, permission : T.Permission) : [Principal] {
        let permission_set = get_permission_set(self, permission);
        return Set.toArray(permission_set);
    };

    public func is_controller(self : StableStore, caller : Principal) : async* Result<(), Text> {
        let canister_id = switch (self.canister_id) {
            case (?id) id;
            case (null) return #err("Canister ID not set.");
        };

        let info = await ic.canister_info({
            canister_id;
            num_requested_changes = ?0;
        });

        let res = Array.find(info.controllers, func(p : Principal) : Bool = p == caller);
        switch (res) {
            case (?_) #ok();
            case (_) #err("Caller is not a controller.");
        };
    };

    public func is_manager(self : StableStore, caller : Principal) : Bool {
        has_permission(self, caller, #ManagePermissions);
    };

    public func is_manager_or_controller(self : StableStore, caller : Principal) : async* Result<(), Text> {
        if (is_manager(self, caller)) return #ok();

        let #err(error_msg) = await* is_controller(self, caller) else return #ok();

        #err("Caller does not have " # debug_show #ManagePermissions # " permission and is not a controller because '" # error_msg # "'.");
    };

    public func grant_permission(self : StableStore, principal : Principal, permission : T.Permission) {
        let permission_set = get_permission_set(self, permission);
        ignore Set.put(permission_set, phash, principal);
    };

    public func revoke_permission(self : StableStore, principal : Principal, permission : T.Permission) {
        let permission_set = get_permission_set(self, permission);
        ignore Set.remove(permission_set, phash, principal);
    };

    // key : Key;
    //     content : Blob;
    //     sha256 : ?Blob;
    //     content_type : Text;
    //     content_encoding : Text;

    func new_asset() : T.Assets {
        {
            encodings = Map.new();
            headers = Map.new();
            var content_type = "";
            var is_aliased = null;
            var max_age = null;
            var allow_raw_access = null;
        };
    };

    func new_encoding() : T.AssetEncoding {
        {
            var modified = Time.now();
            var content_chunks = Vector.new();
            var total_length = 0;
            var certified = false;
            var sha256 = "";
        };
    };

    public func certify_encoding(self : StableStore, asset_key : Text, asset : Assets, encoding_name : Text) : Result<(), Text> {

        Debug.print("Certify encoding called on " # asset_key # " with encoding " # encoding_name);

        let ?encoding = Map.get(asset.encodings, thash, encoding_name) else return #err("Encoding not found.");

        let aliases = if (asset.is_aliased == ?true) get_key_aliases(self, asset_key) else Itertools.empty();

        let key_and_aliases = Itertools.add(aliases, asset_key);

        // Debug.print("is_aliased: " # debug_show (asset.is_aliased));
        // Debug.print("key_and_aliases: " # debug_show Iter.toArray(key_and_aliases));

        for (key_or_alias in key_and_aliases) {

            Debug.print("Certifying " # key_or_alias # " with encoding " # encoding_name);
            let headers = build_headers(asset, encoding_name);
            let headers_array = Map.toArray(headers);

            let success_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().hash(encoding.sha256) // the content's hash is inserted directly instead of computing it from the content
            .response_headers(headers_array).status(200);

            let not_modified_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().response_headers(headers_array).status(304);

            CertifiedAssets.certify(self.certificate_store, success_endpoint);
            CertifiedAssets.certify(self.certificate_store, not_modified_endpoint);

        };

        #ok();
    };

    public func remove_encoding_certificate(self : StableStore, asset_key : Text, asset : Assets, encoding_name : Text) : Result<(), Text> {

        let ?encoding = Map.get(asset.encodings, thash, encoding_name) else return #err("Encoding not found.");

        let aliases = get_key_aliases(self, asset_key);

        for (key_or_alias in Itertools.add(aliases, asset_key)) {

            let headers = build_headers(asset, encoding_name);
            let headers_array = Map.toArray(headers);

            let success_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().hash(encoding.sha256) // the content's hash is inserted directly instead of computing it from the content
            .response_headers(headers_array).status(200);

            let not_modified_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().response_headers(headers_array).status(304);

            CertifiedAssets.remove(self.certificate_store, success_endpoint);
            CertifiedAssets.remove(self.certificate_store, not_modified_endpoint);
        };

        #ok();
    };

    // certifies all encodings of an asset
    // > remember to delete all previous certifications associated with the asset's key
    // > before their data is modified by calling remove_asset_certificates

    func certify_asset(self : StableStore, key : Text, asset : Assets, opt_encoding_name : ?Text) {

        let encodings = switch (opt_encoding_name) {
            case (?encoding_name) switch (Map.get(asset.encodings, thash, encoding_name)) {
                case (?encoding) [(encoding_name, encoding)].vals();
                case (_) Debug.trap("certify_asset(): Encoding not found.");
            };
            case (_) Map.entries(asset.encodings);
        };

        for ((encoding_name, encoding) in encodings) {
            ignore certify_encoding(self, key, asset, encoding_name);
            encoding.certified := true;
        };
    };

    func remove_asset_certificates(self : StableStore, key : Text, asset : Assets, opt_encoding_name : ?Text) {
        let encoding_names = switch (opt_encoding_name) {
            case (?encoding_name) [encoding_name].vals();
            case (_) Map.keys(asset.encodings);
        };

        for (encoding_name in encoding_names) {
            let ?encoding = Map.get(asset.encodings, thash, encoding_name) else Debug.trap("remove_asset_certificates(): Encoding not found.");
            ignore remove_encoding_certificate(self, key, asset, encoding_name);
            encoding.certified := false;

        };
    };

    func get_key_aliases(self : StableStore, key : Text) : Iter.Iter<Text> {
        if (Text.endsWith(key, #text ".html")) return Itertools.empty();

        let aliases = if (Text.endsWith(key, #text "/")) {
            [key # "index.html"];
        } else if (Map.has(self.assets, thash, key # "/")) {
            [key # ".html"];
        } else {
            [
                key # ".html",
                key # "/index.html",
            ];
        };

        Iter.filter(
            aliases.vals(),
            func(alias : Text) : Bool {
                not Map.has(self.assets, thash, alias);
            },
        )

    };

    func get_key_from_aliase(self : StableStore, alias : Text) : ?Text {
        if (not Text.endsWith(alias, #text ".html")) return null;

        let ?sans_html = Text.stripEnd(alias, #text ".html") else return null;

        let ?sans_index = Text.stripEnd(sans_html, #text "/index") else return ?sans_html;

        return ?sans_index;
    };

    public func store(self : StableStore, args : T.StoreArgs) : Result<(), Text> {
        let formatted_key = format_key(args.key);
        let asset = Utils.map_get_or_put(self.assets, thash, formatted_key, new_asset);

        let hash = Sha256.fromBlob(#sha256, args.content);

        switch (args.sha256) {
            case (?provided_hash) {
                if (hash != provided_hash) {
                    return #err("Provided hash does not match computed hash.");
                };
            };
            case (_) ();
        };

        // remove previous certificates
        remove_asset_certificates(self, formatted_key, asset, null);

        let encoding = Utils.map_get_or_put(asset.encodings, thash, args.content_encoding, new_encoding);

        encoding.modified := Time.now();
        Vector.add(encoding.content_chunks, args.content);
        encoding.total_length := args.content.size();
        encoding.certified := false;
        encoding.sha256 := hash;

        asset.content_type := args.content_type;
        asset.is_aliased := args.is_aliased;

        certify_asset(self, formatted_key, asset, null);

        #ok();
    };

    public func retrieve(self : StableStore, key : T.Key) : Result<Blob, Text> {
        let formatted_key = format_key(key);
        let ?asset = Map.get(self.assets, thash, formatted_key) else return #err("Assets not found.");

        let ?id_encoding = Map.get(asset.encodings, thash, "identity") else return #err("No Identity encoding.");

        if (Vector.size(id_encoding.content_chunks) > 1) {
            return #err("Assets too large. Use get() and get_chunk() instead.");
        };

        #ok(Vector.get(id_encoding.content_chunks, 0));
    };

    func format_key(key : T.Key) : T.Key {
        if (not Text.startsWith(key, #text "/")) {
            "/" # key;
        } else {
            key;
        };
    };

    public func get(self : StableStore, args : T.GetArgs) : Result<T.EncodedAsset, Text> {
        let formatted_key = format_key(args.key);
        var opt_asset = Map.get(self.assets, thash, formatted_key);

        switch (opt_asset) {
            case (null) {
                let alias = formatted_key;
                switch (get_key_from_aliase(self, alias)) {
                    case (?key) opt_asset := Map.get(self.assets, thash, key);
                    case (_) {};
                };
            };
            case (?asset) {};
        };

        let ?asset = opt_asset else return #err("Assets not found.");

        label for_loop for (encoding in args.accept_encodings.vals()) {
            let encoded_asset = switch (Map.get(asset.encodings, thash, encoding)) {
                case (?encoded_asset) encoded_asset;
                case (_) continue for_loop;
            };

            return #ok({
                content_type = asset.content_type;
                content = Vector.get(encoded_asset.content_chunks, 0);
                content_encoding = encoding;
                total_length = encoded_asset.total_length;
                sha256 = ?encoded_asset.sha256;
            });
        };

        #err("No matching encoding found for " # debug_show args.accept_encodings);
    };

    public func list(self : StableStore, args : T.ListArgs) : [T.AssetDetails] {
        let assets = Vector.new<T.AssetDetails>();

        for ((key, asset) in Map.entries(self.assets)) {
            let encodings = Vector.new<T.AssetEncodingDetails>();

            for ((encoding, details) in Map.entries(asset.encodings)) {
                let encoding_details : T.AssetEncodingDetails = {
                    content_encoding = encoding;
                    modified = details.modified;
                    length = details.total_length;
                    sha256 = ?details.sha256;
                };

                Vector.add(encodings, encoding_details);
            };

            Vector.add(
                assets,
                {
                    key = key;
                    content_type = asset.content_type;
                    encodings = Vector.toArray(encodings);
                },
            );
        };

        Vector.toArray(assets);
    };

    public func get_chunk(self : StableStore, args : T.GetChunkArgs) : Result<T.ChunkContent, Text> {
        let formatted_key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, formatted_key) else return #err("Assets not found.");
        let ?encoding = Map.get(asset.encodings, thash, args.content_encoding) else return #err("Encoding not found.");

        switch (args.sha256) {
            case (?provided_hash) {
                if (encoding.sha256 != provided_hash) return #err("SHA256 Hash mismatch.");
            };
            case (null) {};
        };

        if (args.index >= Vector.size(encoding.content_chunks)) return #err("Chunk index out of bounds.");

        let content = Vector.get(encoding.content_chunks, args.index);

        #ok({ content });
    };

    public func create_asset(self : StableStore, args : T.CreateAssetArguments) : Result<(), Text> {
        let formatted_key = format_key(args.key);
        if (Map.has(self.assets, thash, formatted_key)) return #err("Assets already exists.");

        let asset = new_asset();

        asset.content_type := args.content_type;
        asset.is_aliased := args.enable_aliasing;
        asset.max_age := args.max_age;
        asset.allow_raw_access := args.allow_raw_access;

        switch (args.headers) {
            case (?headers) {
                for ((key, value) in headers.vals()) {
                    ignore Map.put(asset.headers, thash, key, value);
                };
            };
            case (_) {};
        };

        ignore Map.put(self.assets, thash, formatted_key, asset);

        #ok();
    };

    func hash_bytes(sha256 : Sha256.Digest, bytes : [Blob]) : async () {
        for (content in bytes.vals()) {
            sha256.writeBlob(content);
        };
    };

    public let MAX_CHUNK_SIZE : Nat = 2_097_152;

    // overwrites the content of an asset with the provided content
    public func set_asset_content(self : StableStore, args : T.SetAssetContentArguments) : async* Result<(), Text> {
        let formatted_key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, formatted_key) else return #err("Assets not found.");
        let encoding = Utils.map_get_or_put(asset.encodings, thash, args.content_encoding, new_encoding);

        var total_length = 0;

        // extract chunks and place them into fixed sized chunks of MAX_CHUNK_SIZE in the content_chunks vector

        for (chunk_id in args.chunk_ids.vals()) {
            let ?chunk = Map.get(self.chunks, nhash, chunk_id) else return #err("Chunk with id " # debug_show chunk_id # " not found.");
            total_length += chunk.content.size();
        };

        let bytes = Buffer.Buffer<Nat8>(MAX_CHUNK_SIZE * 3);
        let content_chunks = Buffer.Buffer<Blob>(bytes.size() + MAX_CHUNK_SIZE - 1 / MAX_CHUNK_SIZE); // ceil division

        for (chunk_id in args.chunk_ids.vals()) {
            let ?chunk = Map.remove(self.chunks, nhash, chunk_id) else return #err("Chunk with id " # debug_show chunk_id # " not found.");

            if (chunk.content.size() <= MAX_CHUNK_SIZE) {
                content_chunks.add(chunk.content);
            } else {
                for (byte in Blob.toArray(chunk.content).vals()) {
                    bytes.add(byte);
                };

                for (chunk in Itertools.chunks(bytes.vals(), MAX_CHUNK_SIZE)) {
                    content_chunks.add(Blob.fromArray(chunk));
                };

                bytes.clear();

            };

        };

        // Debug.print("(chunk_ids, content_chunks, total_length): " # debug_show (args.chunk_ids.size(), content_chunks.size(), total_length));

        let hash = do {
            // need to make multiple async calls to hash the content
            // to bypass the 40B instruction limit

            // From the Sha256 benchmarks we know that hashing 1MB of data uses about 320M instructions
            // So we can safely hash about 60MB of data before we hit the 40B instruction limit
            // Assuming each chunk is less than 2MB (the suggested transfer limit for the IC), we can hash
            // 60 in a single call

            let sha256 = Sha256.Digest(#sha256);

            for (chunked_contents in Itertools.chunks(content_chunks.vals(), 60)) {
                await hash_bytes(sha256, chunked_contents);
            };

            sha256.sum();

        };

        // do we need to check if the hash is correct? - probably
        switch (args.sha256) {
            case (?provided_hash) if (hash != provided_hash) {
                return #err("Provided hash does not match computed hash.");
            };
            case (_) {};
        };

        remove_asset_certificates(self, formatted_key, asset, ?args.content_encoding);

        encoding.modified := Time.now();
        encoding.total_length := total_length;
        encoding.sha256 := hash;

        Vector.clear(encoding.content_chunks);
        Vector.addFromIter<Blob>(
            encoding.content_chunks,
            content_chunks.vals(),
        );

        // Debug.print("encoding content chunks: " # debug_show Vector.size(encoding.content_chunks));
        let res = certify_asset(self, formatted_key, asset, ?args.content_encoding);

        Debug.print("certified endpoints: " # debug_show (Iter.toArray(CertifiedAssets.endpoints(self.certificate_store))));

        #ok

    };

    public func unset_asset_content(self : StableStore, args : T.UnsetAssetContentArguments) : Result<(), Text> {
        let formatted_key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, formatted_key) else return #err("Assets not found.");

        let ?encoding = Map.remove(asset.encodings, thash, args.content_encoding) else return #err("Encoding not found.");

        let #ok(_) = remove_encoding_certificate(self, formatted_key, asset, args.content_encoding) else return #err("Failed to remove encoding certificate.");

        #ok();
    };

    public func delete_asset(self : StableStore, args : T.DeleteAssetArguments) : Result<(), Text> {
        // Debug.print("deleting: " # debug_show args.key);
        let formatted_key = format_key(args.key);
        // Debug.print("formatted_key: " # debug_show formatted_key);
        let ?asset = Map.remove(self.assets, thash, formatted_key) else return #err("Assets not found.");
        // Debug.print("retrieved asset");

        for (encoding in Map.keys(asset.encodings)) {
            ignore remove_encoding_certificate(self, formatted_key, asset, encoding); // should automatically recertify fallback paths, if any is affected
        };

        // Debug.print("removed encodings and certificates");

        #ok();
    };

    public func clear(self : StableStore, args : T.ClearArguments) {
        Map.clear(self.assets);
        Map.clear(self.chunks);
        // Map.clear(self.batches);
        self.next_chunk_id := 1;
        // self.next_batch_id := 0;
        CertifiedAssets.clear(self.certificate_store);
    };

    public func get_asset_properties(self : StableStore, key : T.Key) : Result<T.AssetProperties, Text> {
        let formatted_key = format_key(key);
        let ?asset = Map.get(self.assets, thash, formatted_key) else return #err("Assets not found.");

        // let encodings = Vector.new<T.AssetProperties>();

        #ok({
            is_aliased = asset.is_aliased;
            max_age = asset.max_age;
            allow_raw_access = asset.allow_raw_access;
            headers = ?Iter.toArray(Map.entries(asset.headers));
        });
    };

    public func set_asset_properties(self : StableStore, args : T.SetAssetPropertiesArguments) : Result<(), Text> {
        let formatted_key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, formatted_key) else return #err("Assets not found.");

        switch (args.is_aliased) {
            case (??is_aliased) {
                if (asset.is_aliased == ?true and is_aliased == false) {
                    remove_asset_certificates(self, formatted_key, asset, null); // add option to certify only aliases
                } else if (asset.is_aliased == ?false and is_aliased == true) {
                    certify_asset(self, formatted_key, asset, null); // add option to certify only aliases
                };

                asset.is_aliased := ?is_aliased;
            };
            case (?null) asset.is_aliased := null;
            case (_) {};
        };

        switch (args.max_age) {
            case (?max_age) asset.max_age := max_age;
            case (_) {};
        };

        switch (args.allow_raw_access) {
            case (?allow_raw_access) asset.allow_raw_access := allow_raw_access;
            case (_) {};
        };

        switch (args.headers) {
            case (??headers) {
                Map.clear(asset.headers);

                for ((key, value) in headers.vals()) {
                    ignore Map.put(asset.headers, thash, key, value);
                };
            };
            case (?null) Map.clear(asset.headers);
            case (_) {};
        };

        #ok();
    };

    let BATCH_EXPIRY_NANOS : Nat = 300_000_000_000;

    func new_batch() : Batch {
        {
            var expires_at = Time.now() + BATCH_EXPIRY_NANOS;
            var commit_batch_arguments = null;
            var evidence_computation = null;
            var chunk_content_total_size = 0;
        };
    };

    public func create_batch(self : StableStore, curr_time : Time) : Result<T.CreateBatchResponse, Text> {
        var batch_with_commit_args_exists : ?(T.BatchId, ?T.EvidenceComputation) = null;

        for ((batch_id, batch) in Map.entries(self.batches)) {
            if (batch.expires_at < curr_time) {
                // or evidence computation is done
                ignore Map.remove(self.batches, nhash, batch_id);
            };

            if (Option.isSome(batch.commit_batch_arguments) and Option.isNull(batch_with_commit_args_exists)) {
                batch_with_commit_args_exists := ?(batch_id, batch.evidence_computation);
            };
        };

        for ((chunk_id, chunk) in Map.entries(self.chunks)) {
            if (not Map.has(self.batches, nhash, chunk.batch_id)) {
                ignore Map.remove(self.chunks, nhash, chunk_id);
            };
        };

        switch (batch_with_commit_args_exists) {
            case (?(batch_id, ? #Computed(_))) {
                return #err("Batch " # debug_show # batch_id # " is already proposed.  Delete or execute it to propose another.");
            };
            case (?(batch_id, _)) {
                return #err("Batch " # debug_show # batch_id # " has not completed evidence computation.  Wait for it to expire or delete it to propose another.");
            };
            case (_) {};
        };

        switch (self.configuration.max_batches) {
            case (?max_batches) {
                if (Nat64.fromNat(Map.size(self.batches)) >= max_batches) {
                    return #err("Maximum number of batches reached.");
                };
            };
            case (_) {};
        };

        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let batch = new_batch();

        ignore Map.put(self.batches, nhash, batch_id, batch);

        #ok({ batch_id });

    };

    public func create_chunk(self : StableStore, args : T.Chunk) : Result<T.ChunkId, Text> {
        switch (self.configuration.max_chunks) {
            case (?max_chunks) {
                if (Nat64.fromNat(Map.size(self.chunks)) >= max_chunks) {
                    return #err("Maximum number of chunks reached.");
                };
            };
            case (_) {};
        };

        switch (self.configuration.max_bytes) {
            case (?max_bytes) {
                var total_bytes = 0;

                for (batch in Map.vals(self.batches)) {
                    total_bytes += batch.chunk_content_total_size;
                };

                if (Nat64.fromNat(total_bytes) >= max_bytes) {
                    return #err("Maximum number of bytes reached.");
                };
            };
            case (_) {};
        };

        let batch = switch (Map.get(self.batches, nhash, args.batch_id)) {
            case (?batch) {
                if (Option.isSome(batch.commit_batch_arguments)) {
                    return #err("Batch " # debug_show (args.batch_id) # " has already been proposed.");
                };

                batch;
            };
            case (_) {
                return #err("Batch not found.");
            };
        };

        batch.expires_at := Time.now() + BATCH_EXPIRY_NANOS;

        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;

        let chunk = {
            batch_id = args.batch_id;
            content = args.content;
        };

        ignore Map.put(self.chunks, nhash, chunk_id, chunk);

        #ok(chunk_id);
    };

    public func execute_batch_operation(self : StableStore, operation : T.BatchOperationKind) : async* Result<(), Text> {
        let res : Result<(), Text> = switch (operation) {
            case (#Clear(args)) #ok(clear(self, args));
            case (#CreateAsset(args)) create_asset(self, args);
            case (#SetAssetContent(args)) await* set_asset_content(self, args);
            case (#UnsetAssetContent(args)) unset_asset_content(self, args);
            case (#DeleteAsset(args)) delete_asset(self, args);
            case (#SetAssetProperties(args)) set_asset_properties(self, args);
        };

        // Debug.print(debug_show operation);

        let #ok(_) = res else return Utils.send_error(res);

        #ok();
    };

    public func execute_batch_operations_sequentially(self : StableStore, operations : [T.BatchOperationKind]) : async* Result<(), Text> {

        for (operation in operations.vals()) {
            let res = await* execute_batch_operation(self, operation);
            let #ok(_) = res else return Utils.send_error(res);
        };

        #ok();
    };

    public func commit_batch(self : StableStore, args : T.CommitBatchArguments) : async* Result<(), Text> {
        // why do we need to group? - because we want to execute all operations in parallel
        // but some operations need to be sequential (e.g. create_asset needs to be called before you can set_asset_content)
        // so we are going to group operations with the same key together so they run sequentially within the group
        // and then run all groups in parallel

        let groups = Map.new<T.Key, Buffer<T.BatchOperationKind>>();

        label grouping for (op in args.operations.vals()) {
            if (op == #Clear({})) {
                clear(self, {});
                Map.clear(groups);
                continue grouping;
            };

            let #CreateAsset({ key }) or #SetAssetContent({ key }) or #UnsetAssetContent({
                key;
            }) or #DeleteAsset({ key }) or #SetAssetProperties({ key }) = op else Debug.trap("unreachable");

            let group = Utils.map_get_or_put(groups, thash, key, func() : Buffer<T.BatchOperationKind> = Buffer.Buffer(8));
            group.add(op);

        };

        let parallel = Buffer.Buffer<async* Result<(), Text>>(groups.size());
        for (group in Map.vals(groups)) {
            let res = execute_batch_operations_sequentially(self, Buffer.toArray(group));
            parallel.add(res);
        };

        // test to see if it works better than single for await loop
        for (async_call in parallel.vals()) {
            let res = await* async_call;
            let #ok(_) = res else return Utils.send_error(res);
        };

        ignore Map.remove(self.batches, nhash, args.batch_id);
        // certifify 404 page if necessary
        #ok();
    };

    public func propose_commit_batch(self : StableStore, args : T.CommitBatchArguments) : Result<(), Text> {
        let ?batch = Map.get(self.batches, nhash, args.batch_id) else return #err("Batch not found.");

        if (Option.isSome(batch.commit_batch_arguments)) return #err("Batch already has proposed T.CommitBatchArguments");

        batch.commit_batch_arguments := ?args;

        #ok();
    };

    func validate_commit_proposed_batch_args(self : StableStore, args : T.CommitProposedBatchArguments) : Result<(), Text> {
        let ?(batch) = Map.get(self.batches, nhash, args.batch_id) else return #err("Batch not found.");

        if (Option.isNull(batch.commit_batch_arguments)) return #err("Batch does not have proposed CommitBatchArguments");

        let ?(#Computed(evidence)) = batch.evidence_computation else return #err("Batch does not have computed evidence.");

        if (evidence != args.evidence) return #err("Batch computed evidence (" # debug_show (evidence) # ") does not match provided evidence (" # debug_show args.evidence # ").");

        #ok();
    };

    public func commit_proposed_batch(self : StableStore, args : T.CommitProposedBatchArguments) : async* Result<(), Text> {
        let validate_result = validate_commit_proposed_batch_args(self, args);
        let #ok(_) = validate_result else return Utils.send_error(validate_result);

        let ?batch = Map.get(self.batches, nhash, args.batch_id) else return #err("Batch not found.");
        let ?commit_batch_arguments = batch.commit_batch_arguments else return #err("Batch does not have proposed T.CommitBatchArguments");

        ignore (await* commit_batch(self, commit_batch_arguments));

        batch.commit_batch_arguments := null;

        #ok();
    };

    let DEFAULT_MAX_COMPUTE_EVIDENCE_ITERATIONS : Nat16 = 20;

    public func compute_evidence(self : StableStore, args : T.ComputeEvidenceArguments) : Result<?Blob, Text> {
        let ?batch = Map.get(self.batches, nhash, args.batch_id) else return #err("Batch not found.");
        let ?commit_batch_args = batch.commit_batch_arguments else return #err("Batch does not have CommitBatchArguments");

        let max_iterations = switch (args.max_iterations) {
            case (?max_iterations) max_iterations;
            case (_) DEFAULT_MAX_COMPUTE_EVIDENCE_ITERATIONS;
        };

        let _evidence_computation : T.EvidenceComputation = switch (batch.evidence_computation) {
            case (?evidence_computation) {
                batch.evidence_computation := null;
                evidence_computation;
            };
            case (_) {
                #NextOperation {
                    operation_index = 0;
                    hasher_state = do {
                        let digest = Sha256.Digest(#sha256);
                        digest.share();
                    };
                };
            };
        };

        var evidence_computation = _evidence_computation;

        label for_loop for (_ in Iter.range(1, Nat16.toNat(max_iterations))) {
            // evidence_computation := Evidence.advance(commit_batch_args, self.chunks);

            switch (evidence_computation) {
                case (#Computed(_)) break for_loop;
                case (_) {};
            };
        };

        batch.evidence_computation := ?evidence_computation;

        switch (evidence_computation) {
            case (#Computed(evidence)) #ok(?evidence);
            case (_) #ok(null);
        };
    };

    public func delete_batch(self : StableStore, args : T.DeleteBatchArguments) : Result<(), Text> {
        let ?batch = Map.remove(self.batches, nhash, args.batch_id) else return #err("Batch not found.");

        for (chunk_id in Map.keys(self.chunks)) {
            ignore Map.remove(self.chunks, nhash, chunk_id);
        };

        #ok();
    };

    func redirect_to_certified_domain(self : StableStore, url : URL) : T.HttpResponse {
        let canister_id = switch (self.canister_id) {
            case (?id) id;
            case (null) return Debug.trap("Canister ID not set.");
        };

        let path = url.path.original;
        let domain = url.host.original;
        let location = if (Text.contains(domain, #text("ic0.app"))) {
            "https://" # Principal.toText(canister_id) # ".ic0.app" # path;
        } else {
            "https://" # Principal.toText(canister_id) # ".icp0.io" # path;
        };

        return {
            status_code = 308; // Permanent Redirect
            headers = [("Location", location)];
            body = "";
            upgrade = null;
            streaming_strategy = null;
        };
    };

    let FALLBACK_FILE : Text = "/index.html";

    let ENCODING_CERTIFICATION_ORDER : [Text] = ["identity", "gzip", "compress", "deflate", "br"];

    public func encoding_order(accept_encodings : [Text]) : [Text] {
        Array.sort(
            accept_encodings,
            func(a : Text, b : Text) : Order {
                let a_index = Array.indexOf(a, ENCODING_CERTIFICATION_ORDER, Text.equal);
                let b_index = Array.indexOf(b, ENCODING_CERTIFICATION_ORDER, Text.equal);

                switch (a_index, b_index) {
                    case (?a_index, ?b_index) {
                        if (a_index < b_index) return #less;
                        if (a_index > b_index) return #greater;
                        return #equal;
                    };
                    case (_, ?_) #greater;
                    case (?_, _) #less;
                    case (_, _) #equal;
                };
            },
        );
    };

    func build_headers(asset : Assets, encoding_name : Text) : Map<Text, Text> {
        let headers = Map.new<Text, Text>();
        ignore Map.put(headers, thash, "content-type", asset.content_type);

        switch (asset.max_age) {
            case (?max_age) {
                ignore Map.put(headers, thash, "cache-control", "max-age=" # debug_show (max_age));
            };
            case (_) {};
        };

        if (encoding_name != "identity") {
            ignore Map.put(headers, thash, "content-encoding", encoding_name);
        };

        for ((key, value) in Map.entries(asset.headers)) {
            ignore Map.put(headers, thash, key, value);
        };

        headers;
    };

    public func http_request_streaming_callback(self : StableStore, token : T.StreamingToken) : T.StreamingCallbackResponse {

        Debug.print("Streaming token: " # debug_show token);

        let ?asset = Map.get(self.assets, thash, token.key) else Debug.trap("Assets not found.");
        let ?encoding = Map.get(asset.encodings, thash, token.content_encoding) else Debug.trap("Encoding not found.");

        if (?encoding.sha256 != token.sha256) Debug.trap("SHA256 hash mismatch");

        let chunk : Blob = if (token.index < Vector.size(encoding.content_chunks)) Vector.get(encoding.content_chunks, token.index) else "";
        // Debug.print("Content chunk: " # debug_show chunk.size());

        let next_token : T.CustomStreamingToken = {
            key = token.key;
            content_encoding = token.content_encoding;
            index = token.index + 1;
            sha256 = ?encoding.sha256;
        };

        // Debug.print("Next token: " # debug_show next_token);
        let content_chunks_size = Vector.size(encoding.content_chunks);

        let response : T.StreamingCallbackResponse = {
            body = chunk;
            token = if (next_token.index < content_chunks_size) ?(next_token) else (null);
        };

        (response);

    };

    func build_ok_response(
        self : StableStore,
        key : T.Key,
        asset : Assets,
        encoding_name : Text,
        encoding : AssetEncoding,
        chunk_index : Nat,
        etags : [Text],
        http_req : T.HttpRequest,
    ) : T.HttpResponse {

        let headers = build_headers(asset, encoding_name);
        let next_token : T.StreamingToken = {
            key;
            content_encoding = encoding_name;
            index = chunk_index + 1;
            sha256 = ?encoding.sha256;
        };

        let ?callback : ?T.StreamingCallback = self.streaming_callback else return Debug.trap("Streaming callback not set");
        let streaming_strategy : T.StreamingStrategy = #Callback({
            token = (next_token);
            callback;
        });

        // Debug.print("next token: " # debug_show (next_token));

        let contains_hash = Option.isSome(
            Array.find(
                etags,
                func(etag : Text) : Bool {
                    let unwrapped_etag = Text.replace(etag, #text("\""), "");

                    let #ok(etag_bytes) = Hex.decode(unwrapped_etag) else return false;
                    Blob.fromArray(etag_bytes) == encoding.sha256;
                },
            )
        );

        // Debug.print("encoding chunks: " # debug_show Vector.size(encoding.content_chunks));

        let content_chunk = Vector.get(encoding.content_chunks, chunk_index);

        // Debug.print("content (index, bytes): " # debug_show (chunk_index, content_chunk.size()));
        assert content_chunk.size() <= 2 * (1024 ** 2);

        // Debug.print("contains_hash: " # debug_show contains_hash);

        let (status_code, body, opt_body_hash) : (Nat16, Blob, ?Blob) = if (contains_hash) {
            (304, "", null);
            // (200, content_chunk);
        } else {

            let hex = Hex.encode(Blob.toArray(encoding.sha256));
            let etag_value = "\"" # hex # "\"";
            ignore Map.put(headers, thash, "etag", etag_value);

            (200, content_chunk, ?encoding.sha256);
        };

        let headers_buffer = Buffer.Buffer<(Text, Text)>(Map.size(headers));
        for ((key, value) in Map.entries(headers)) {
            headers_buffer.add((key, value));
        };

        let http_res = {
            status_code;
            headers = Buffer.toArray(headers_buffer);
            body;
            upgrade = null;
            streaming_strategy = null;
        };

        // Debug.print("http_res: " # debug_show { http_res with body = "" });

        // Debug.print(debug_show { http_req; http_res = { http_res with streaming_strategy = null } });
        let certified_headers_result = CertifiedAssets.get_certificate(self.certificate_store, http_req, http_res, opt_body_hash);

        switch (certified_headers_result) {
            case (#ok(certified_headers)) {

                for ((key, value) in certified_headers.vals()) {
                    headers_buffer.add((key, value));
                };

                let certified_res : T.HttpResponse = {
                    status_code;
                    headers = Buffer.toArray(headers_buffer);
                    body;
                    upgrade = null;
                    streaming_strategy = if (Vector.size(encoding.content_chunks) > 1) ?streaming_strategy else null;
                };

                return certified_res;
            };
            case (#err(err_msg)) return Debug.trap("CertifiedAssets.get_certificate failed: " # err_msg);
        };

    };

    func get_asset(self : StableStore, key : Text) : ?Assets {
        switch (Map.get(self.assets, thash, key)) {
            case (?asset) return ?asset;
            case (_) {};
        };

        let reverse_alias = switch (get_key_from_aliase(self, key)) {
            case (?key) key;
            case (_) return null;
        };

        switch (Map.get(self.assets, thash, reverse_alias)) {
            case (?asset) {
                if (asset.is_aliased != ?true) return null;
                return ?asset;
            };
            case (_) return null;
        };
    };

    public func build_http_response(self : StableStore, req : T.HttpRequest, url : URL, encodings : [Text]) : T.HttpResponse {
        let path = url.path.original;

        let cert_version : Nat16 = switch (req.certificate_version) {
            case (?v) v;
            case (_) 2;
        };

        let opt_asset = get_asset(self, path);
        let opt_fallback = Map.get(self.assets, thash, FALLBACK_FILE);

        let asset = switch (opt_asset, opt_fallback) {
            case (?asset, _) {
                if ((not Option.get(asset.allow_raw_access, true)) and Text.contains(url.host.original, #text "raw.ic")) {
                    return redirect_to_certified_domain(self, url);
                };
                asset;
            };

            case (null, ?fallback) {
                if ((not Option.get(fallback.allow_raw_access, true)) and Text.contains(url.host.original, #text "raw.ic")) {
                    return redirect_to_certified_domain(self, url);
                };
                fallback;
            };

            case (_) return {
                status_code = 403;
                headers = [];
                body = "";
                upgrade = null;
                streaming_strategy = null;
            };
        };

        let etag_value = Array.find(
            req.headers,
            func(header : (Text, Text)) : Bool {
                header.0 == "if-none-match";
            },
        );

        let etag_values = switch (etag_value) {
            case (?(field, val)) [val];
            case (_) [];
        };

        let ordered_encodings = encoding_order(encodings);
        label loop_1 for (encoding_name in ordered_encodings.vals()) {
            let ?encoding = Map.get(asset.encodings, thash, encoding_name) else continue loop_1;

            if (cert_version == 1 and encoding_name != ordered_encodings[0]) break loop_1;

            // let headers = build_headers(asset, encoding_name);
            let sha256 = encoding.sha256;
            // let sha256_hex = Hex.encode(Blob.toArray(sha256));

            let res = build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req);
            return res;
            // switch (res) {
            //     case (#ok(res)) return #ok(res);
            //     case (#err(_)) if (cert_version == 1) break loop_1; // fallback if cert version is v1
            // };

        };

        // not necessary because CertifiedAssets will return whatever
        // encoding was stored last. The only issue is the encoding
        // might be different than the one set in the header. So there needs
        // be a way to know the best encoding that has already been hashed for v1.
        // if (req.certified_version == 1){

        // };

        label loop_2 for (encoding_name in ENCODING_CERTIFICATION_ORDER.vals()) {
            let ?encoding = Map.get(asset.encodings, thash, encoding_name) else continue loop_2;
            // let sha256_hex = Hex.encode(Blob.toArray(encoding.sha256));
            // let headers = build_headers(asset, encoding_name);
            let res = build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req);
            return res;
            // switch (res) {
            //     case (#ok(res)) return res;
            //     case (#err(_)) break loop_2;
            // };
        };

        return {
            status_code = 403;
            headers = [];
            body = "";
            upgrade = null;
            streaming_strategy = null;
        };

    };
};
