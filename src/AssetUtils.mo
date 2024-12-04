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
import Nat "mo:base/Nat";

import Set "mo:map/Set";
import Map "mo:map/Map";
import IC "mo:ic";
import CertifiedAssets "mo:certified-assets/Stable";
import Itertools "mo:itertools/Iter";
import PeekableIter "mo:itertools/PeekableIter";
import RevIter "mo:itertools/RevIter";
import Sha256 "mo:sha2/Sha256";
import Vector "mo:vector";
import HttpParser "mo:http-parser";
import Hex "mo:encoding/Hex";

import Utils "Utils";
import T "Types";
import Evidence "Evidence";

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
    type StableStore = T.StableStore;

    type URL = HttpParser.URL;
    let URL = HttpParser.URL;
    let Headers = HttpParser.Headers;

    let { ic } = IC;

    let { phash } = Set;
    let { thash; nhash } = Map;

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
        let canister_id = self.canister_id;

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

    public func create_new_asset_encoding() : AssetEncoding = {
        var modified = Time.now();
        var content_chunks = [];
        var content_chunks_prefix_sum = [];
        var total_length = 0;
        var certified = false;
        var sha256 = "";
    };

    func create_new_asset_record() : Assets = {
        encodings = Map.new();
        headers = Map.new();
        var content_type = "";
        var is_aliased = null;
        var max_age = null;
        var allow_raw_access = null;
        var last_certified_encoding = null;
    };

    func create_and_store_new_asset(assets : Map<Key, Assets>, _key : Key) : Assets {

        let key = format_key(_key);

        let asset = create_new_asset_record();
        ignore Map.put(assets, thash, key, asset);

        asset

    };

    public func certify_encoding(self : StableStore, asset_key : Text, asset : Assets, encoding_name : Text) : Result<(), Text> {

        // Debug.print("Certify encoding called on " # asset_key # " with encoding " # encoding_name);

        let ?encoding = Map.get(asset.encodings, thash, encoding_name) else return #err(ErrorMessages.encoding_not_found(asset_key, encoding_name));

        let aliases = if (asset.is_aliased == ?true) get_key_aliases(self, asset_key) else Itertools.empty();

        let key_and_aliases = Itertools.add(aliases, asset_key);

        // Debug.print("is_aliased: " # debug_show (asset.is_aliased));
        // Debug.print("key_and_aliases: " # debug_show Iter.toArray(key_and_aliases));

        for (key_or_alias in key_and_aliases) {

            // Debug.print("Certifying " # key_or_alias # " with encoding " # encoding_name);
            let headers = build_headers(asset, encoding_name, encoding.sha256);
            let headers_array = Map.toArray(headers);

            let success_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().hash(encoding.sha256) // the content's hash is inserted directly instead of computing it from the content
            .response_headers(headers_array).status(200);

            let not_modified_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().response_headers(headers_array).status(304);

            CertifiedAssets.certify(self.certificate_store, success_endpoint);
            CertifiedAssets.certify(self.certificate_store, not_modified_endpoint);

        };

        asset.last_certified_encoding := ?encoding_name;

        #ok();
    };

    func remove_encoding_certificate(self : StableStore, asset_key : Text, asset : Assets, encoding_name : Text, encoding : T.AssetEncoding, only_aliases : Bool) {
        // verify that `only_aliases is set to true only if the asset is aliased
        // if not, then we are probably calling this function incorrectly
        Debug.print(debug_show { asset_key; is_aliased = asset.is_aliased; only_aliases });
        assert (asset.is_aliased != ?true and only_aliases == false) or asset.is_aliased == ?true;

        let aliases = if (asset.is_aliased == ?true) get_key_aliases(self, asset_key) else Itertools.empty();
        let keys = if (only_aliases) aliases else Itertools.add(aliases, asset_key);

        for (key_or_alias in keys) {
            Debug.print("Removing certification for " # key_or_alias # " (alias of " # debug_show asset_key # ") with encoding " # encoding_name);

            let headers = build_headers(asset, encoding_name, encoding.sha256);
            let headers_array = Map.toArray(headers);

            let success_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().hash(encoding.sha256) // the content's hash is inserted directly instead of computing it from the content
            .response_headers(headers_array).status(200);

            let not_modified_endpoint = CertifiedAssets.Endpoint(key_or_alias, null).no_request_certification().response_headers(headers_array).status(304);

            CertifiedAssets.remove(self.certificate_store, success_endpoint);
            CertifiedAssets.remove(self.certificate_store, not_modified_endpoint);
        };

        if (not only_aliases) {
            encoding.certified := false;

            // only used for certification v1
            asset.last_certified_encoding := null;

        };

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

    public func recertify(self : StableStore, key : Text) {
        let asset = switch (Map.get(self.assets, thash, key)) {
            case (?asset) asset;
            case (_) return;
        };

        remove_asset_certificates(self, key, asset, true);
        certify_asset(self, key, asset, null);
    };

    func remove_asset_certificates(self : StableStore, key : Text, asset : Assets, only_aliases : Bool) {
        for ((encoding_name, encoding) in Map.entries(asset.encodings)) {
            remove_encoding_certificate(self, key, asset, encoding_name, encoding, only_aliases);
        };
    };

    func remove_encoding(self : StableStore, asset_key : Text, asset : Assets, content_encoding : Text) : Result<T.AssetEncoding, Text> {
        let ?encoding = Map.remove(asset.encodings, thash, content_encoding) else return #err(ErrorMessages.encoding_not_found(asset_key, content_encoding));
        remove_encoding_certificate(self, asset_key, asset, content_encoding, encoding, false);

        #ok(encoding);
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

        // an alias cannot overwrite an existing asset
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

        let key = format_key(args.key);

        let asset = switch (get_asset_using_aliases(self, key, false)) {
            case (?asset) asset;
            case (null) create_and_store_new_asset(self.assets, key);
        };

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
        remove_asset_certificates(self, key, asset, false);

        let encoding = Utils.map_get_or_put(asset.encodings, thash, args.content_encoding, func() : T.AssetEncoding = create_new_asset_encoding());

        encoding.modified := Time.now();
        encoding.content_chunks := [Blob.toArray(args.content)];
        encoding.content_chunks_prefix_sum := [args.content.size()];
        encoding.total_length := args.content.size();
        encoding.certified := false;
        encoding.sha256 := hash;

        asset.content_type := args.content_type;
        asset.is_aliased := args.is_aliased;

        certify_asset(self, key, asset, null);

        #ok();
    };

    func format_key(key : T.Key) : T.Key {
        let url = HttpParser.URL(key, HttpParser.Headers([]));

        // Debug.print("original path: " # url.path.original);
        let formatted = "/" # Text.join("/", url.path.array.vals());

        // Debug.print("formatted path: " # formatted);

        let extra = "/" # Text.join("/", Text.tokens(url.path.original, #text "/"));

        // Debug.print("extra effort: " # extra);

        return "/" # Text.join("/", url.path.array.vals());

    };

    public func exists(self : StableStore, key : T.Key) : Bool {
        Option.isSome(get_asset_using_aliases(self, key, false));
    };

    module ErrorMessages {
        public func asset_not_found(key : T.Key) : Text {
            "Asset not found for path: " # debug_show key;
        };

        public func encoding_not_found(asset_key : T.Key, encoding_name : Text) : Text {
            "Encoding not found for asset " # debug_show asset_key # " with encoding " # encoding_name;
        };

        public func batch_not_found(batch_id : Nat) : Text {
            "Batch not found with id " # debug_show batch_id;
        };

    };

    public func get(self : StableStore, args : T.GetArgs) : Result<T.EncodedAsset, Text> {
        let key = format_key(args.key);
        var opt_asset = get_asset_using_aliases(self, key, true);
        let ?asset = opt_asset else return #err(ErrorMessages.asset_not_found(key));

        label for_loop for (encoding_key in args.accept_encodings.vals()) {
            let encoding = switch (Map.get(asset.encodings, thash, encoding_key)) {
                case (?encoding) encoding;
                case (_) continue for_loop;
            };

            return #ok({
                content_type = asset.content_type;
                content = switch (get_encoding_chunk(encoding, 0)) {
                    case (null) "";
                    case (?content) content;
                };
                content_encoding = encoding_key;
                total_length = encoding.total_length;
                sha256 = ?encoding.sha256;
            });
        };

        #err("No matching encoding found for " # debug_show args.accept_encodings);
    };

    public func list(self : StableStore, args : T.ListArgs) : [T.AssetDetails] {
        let asset_entries = Map.entries(self.assets);

        let assets = Array.tabulate(
            Map.size(self.assets),
            func(i : Nat) : T.AssetDetails {
                let ?(asset_key, asset) = asset_entries.next() else Debug.trap("list(): Asset entry not found.");

                let encodings = Array.tabulate(
                    Map.size(asset.encodings),
                    func(j : Nat) : T.AssetEncodingDetails {
                        let ?(encoding_name, encoding) = Map.entries(asset.encodings).next() else Debug.trap("list(): Encoding entry not found.");

                        let encoding_details : T.AssetEncodingDetails = {
                            content_encoding = encoding_name;
                            modified = encoding.modified;
                            length = encoding.total_length;
                            sha256 = ?encoding.sha256;
                        };

                        encoding_details;
                    },
                );

                let asset_details : T.AssetDetails = {
                    key = asset_key;
                    content_type = asset.content_type;
                    encodings = encodings;
                };

                asset_details;
            },
        );

        assets;
    };

    public func binary_search<A>(arr : [A], cmp : (A, A) -> Order, search_key : A) : ?Nat {
        let arr_len = arr.size();

        if (arr_len == 0) return null;
        var l = 0;

        var r = arr_len - 1 : Nat;

        while (l < r) {
            let mid = (l + r) / 2;

            let val = arr[mid];

            let result = cmp(search_key, val);
            if (result == #less) {
                r := mid;
            } else if (result == #greater) {
                l := mid + 1;
            } else {
                return ?mid;
            };

        };

        let insertion = l;

        // Check if the insertion point is valid
        // return the insertion point but negative and subtracting 1 indicating that the key was not found
        // such that the insertion index for the key is Int.abs(insertion) - 1
        // [0,  1,  2]
        //  |   |   |
        // -1, -2, -3
        let result = cmp(search_key, arr[insertion]);

        let index = if (result == #less or result == #equal) insertion else insertion + 1;

        if (index >= arr_len) return null;

        ?index;

    };

    public func get_encoding_chunk_with_given_size(
        encoding : T.AssetEncoding,
        GIVEN_CHUNK_SIZE : Nat,
        virtual_chunk_index : Nat,
    ) : ?Blob {
        if (encoding.content_chunks.size() == 0) return null;

        let start = virtual_chunk_index * GIVEN_CHUNK_SIZE;
        let end = Nat.min(
            encoding.content_chunks_prefix_sum[encoding.content_chunks_prefix_sum.size() - 1],
            start + GIVEN_CHUNK_SIZE,
        );

        let left_chunk_index = switch (binary_search(encoding.content_chunks_prefix_sum, Nat.compare, start)) {
            case (null) return null;
            case (?index) index;
        };

        let left_chunk_start_index = if (left_chunk_index == 0) 0 else encoding.content_chunks_prefix_sum[left_chunk_index - 1];

        var left_chunk_nested_start_index = start - left_chunk_start_index;

        var right_chunk_index = left_chunk_index;

        while (encoding.content_chunks_prefix_sum[right_chunk_index] < end) {
            right_chunk_index += 1;
        };

        let right_chunk_start_index = if (right_chunk_index == 0) 0 else encoding.content_chunks_prefix_sum[right_chunk_index - 1];

        let right_chunk_nested_end_index = end - right_chunk_start_index;

        let size = end - start;

        var current_chunk_index = left_chunk_index;
        var nested_index = left_chunk_nested_start_index;

        let chunk = Array.tabulate(
            size,
            func(_ : Nat) : Nat8 {

                while (nested_index == encoding.content_chunks[current_chunk_index].size()) {
                    current_chunk_index += 1;
                    nested_index := 0;
                };

                let byte = encoding.content_chunks[current_chunk_index][nested_index];
                nested_index += 1;

                byte

            },
        );

        ?Blob.fromArray(chunk);

    };

    public func get_encoding_chunk(encoding : T.AssetEncoding, virtual_chunk_index : Nat) : ?Blob {
        get_encoding_chunk_with_given_size(encoding, MAX_CHUNK_SIZE, virtual_chunk_index);
    };

    func get_encoding_bytes_length(encoding : T.AssetEncoding) : Nat {
        if (encoding.content_chunks.size() == 0) return 0;

        encoding.content_chunks_prefix_sum[encoding.content_chunks_prefix_sum.size() - 1];
    };

    public func div_ceiling(a : Nat, b : Nat) : Nat {
        (a + (b - 1)) / b;
    };

    func get_num_of_encoding_chunks(encoding : T.AssetEncoding) : Nat {
        if (encoding.content_chunks.size() == 0) return 0;

        let total_bytes = get_encoding_bytes_length(encoding);

        div_ceiling(total_bytes, MAX_CHUNK_SIZE);
    };

    public func get_chunk(self : StableStore, args : T.GetChunkArgs) : Result<T.ChunkContent, Text> {
        let key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, key) else return #err(ErrorMessages.asset_not_found(key));
        let ?encoding = Map.get(asset.encodings, thash, args.content_encoding) else return #err(ErrorMessages.encoding_not_found(key, args.content_encoding));

        switch (args.sha256) {
            case (?provided_hash) {
                if (encoding.sha256 != provided_hash) return #err("SHA256 Hash mismatch.");
            };
            case (null) {};
        };

        let num_chunks = get_num_of_encoding_chunks(encoding);
        if (args.index >= num_chunks) return #err("Chunk index out of bounds.");

        switch (get_encoding_chunk(encoding, args.index)) {
            case (?content) #ok({ content });
            case (null) #err("Chunk '" # debug_show args.index # "' not found.");
        };

    };

    public func create_asset(self : StableStore, args : T.CreateAssetArguments) : Result<(), Text> {
        let key = format_key(args.key);

        // Debug.print("Creating asset with key: " # key);

        if (Map.has(self.assets, thash, key)) return #err("Assets already exists.");

        let asset = create_and_store_new_asset(self.assets, key);

        asset.content_type := args.content_type;
        asset.is_aliased := args.enable_aliasing;
        asset.max_age := args.max_age;
        asset.allow_raw_access := args.allow_raw_access;

        switch (args.headers) {
            case (?headers) {
                for ((field, value) in headers.vals()) {
                    ignore Map.put(asset.headers, thash, field, value);
                };
            };
            case (_) {};
        };

        #ok();
    };

    public func hash_blob_chunks(content_chunks : [Blob], opt_prefix_sum_array_of_chunk_sizes : ?[Nat]) : async* Blob {
        let content_chunks_bytes = Array.map(content_chunks, Blob.toArray);
        await* hash_chunks(content_chunks_bytes, opt_prefix_sum_array_of_chunk_sizes);
    };

    func hash_bytes(sha256 : Sha256.Digest, chunks : Iter.Iter<[Nat8]>) : async () {
        for (content in chunks) {
            sha256.writeArray(content);
        };
    };

    public func hash_chunks(content_chunks : [[Nat8]], opt_prefix_sum_array_of_chunk_sizes : ?[Nat]) : async* Blob {
        // need to make multiple async calls to hash the content
        // to bypass the 40B instruction limit

        // From the Sha256 benchmarks we know that hashing 1MB of data uses about 320M instructions
        // So we can safely hash about 60MB of data before we hit the 40B instruction limit
        // Assuming each chunk is less than 2MB (the suggested transfer limit for the IC), we can hash
        // 60 in a single call

        let buffer = Buffer.Buffer<Nat>(content_chunks.size());

        var prev_accumulated_size = 0;
        let content_chunks_prefix_sum = switch (opt_prefix_sum_array_of_chunk_sizes) {
            case (?prefix_sum_array_of_chunk_sizes) prefix_sum_array_of_chunk_sizes;
            case (null) Array.tabulate(
                content_chunks.size(),
                func(i : Nat) : Nat {
                    let curr = prev_accumulated_size + content_chunks[i].size();
                    prev_accumulated_size := curr;
                    curr;
                },
            );
        };

        assert content_chunks_prefix_sum.size() == content_chunks.size();
        prev_accumulated_size := 0;
        for ((i, accumulated_size) in Itertools.enumerate(content_chunks_prefix_sum.vals())) {
            assert accumulated_size >= prev_accumulated_size;

            let is_exceeding_limit = accumulated_size - prev_accumulated_size > MAX_HASHING_BYTES_PER_CALL;
            let is_last_chunk = i == content_chunks.size() - 1;

            if (is_exceeding_limit) {
                buffer.add(i);
                prev_accumulated_size := accumulated_size;
            };

            if (is_last_chunk) {
                buffer.add(i + 1);
            };
        };

        var prev_chunk_index = 0;
        let hashable_chunks_per_call = Iter.map(
            buffer.vals(),
            func(end_index : Nat) : Iter.Iter<[Nat8]> {
                let slice = Itertools.fromArraySlice(content_chunks, prev_chunk_index, end_index);
                prev_chunk_index := end_index;
                slice;
            },
        );

        let sha256 = Sha256.Digest(#sha256);

        for (chunked_contents in hashable_chunks_per_call) {
            await hash_bytes(sha256, chunked_contents);
        };

        sha256.sum();

    };

    public let MAX_CHUNK_SIZE : Nat = 2_097_152;
    public let MAX_HASHING_BYTES_PER_CALL : Nat = 62_914_560;

    // overwrites the content of an asset with the provided content
    public func set_asset_content(self : StableStore, args : T.SetAssetContentArguments) : async* Result<(), Text> {
        let key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, key) else return #err(ErrorMessages.asset_not_found(key));
        let encoding = Utils.map_get_or_put(asset.encodings, thash, args.content_encoding, func() : T.AssetEncoding = create_new_asset_encoding());

        var total_length = 0;

        // extract chunks and place them as is in the content_chunks array
        // avoids additional computation to split the content into chunks
        // and allows large files to be processed in a single call

        var error_msg : ?Text = null;

        let content_chunks : [[Nat8]] = Array.tabulate(
            args.chunk_ids.size(),
            func(i : Nat) : [Nat8] {
                let chunk_id = args.chunk_ids[i];
                switch (Map.get(self.chunks, nhash, chunk_id)) {
                    case (?chunk) {
                        total_length += chunk.content.size();
                        chunk.content;
                    };
                    case (null) {
                        error_msg := ?("Chunk with id " # debug_show chunk_id # " not found.");
                        [];
                    };
                };
            },
        );

        switch (error_msg) {
            case (?msg) return #err(msg);
            case (_) {};
        };

        var prev_prefix_sum = 0;

        let content_chunks_prefix_sum = Array.tabulate(
            content_chunks.size(),
            func(i : Nat) : Nat {
                let curr = prev_prefix_sum + content_chunks[i].size();

                prev_prefix_sum := curr;

                curr;
            },
        );

        // Debug.print("(chunk_ids, content_chunks, total_length): " # debug_show (args.chunk_ids.size(), content_chunks.size(), total_length));

        let hash = await* hash_chunks(content_chunks, ?content_chunks_prefix_sum);

        // do we need to check if the hash is correct? - probably
        switch (args.sha256) {
            case (?provided_hash) if (hash != provided_hash) {
                return #err("Provided hash does not match computed hash.");
            };
            case (_) {};
        };

        remove_encoding_certificate(self, key, asset, args.content_encoding, encoding, false);

        encoding.modified := Time.now();
        encoding.total_length := total_length;
        encoding.sha256 := hash;
        encoding.content_chunks := content_chunks;
        encoding.content_chunks_prefix_sum := content_chunks_prefix_sum;

        certify_asset(self, key, asset, ?args.content_encoding);

        // Debug.print("certified endpoints: " # debug_show (Iter.toArray(CertifiedAssets.endpoints(self.certificate_store))));

        #ok

    };

    public func unset_asset_content(self : StableStore, args : T.UnsetAssetContentArguments) : Result<(), Text> {
        let key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, key) else return #err(ErrorMessages.asset_not_found(key));

        let res = remove_encoding(self, key, asset, args.content_encoding);
        let #ok(_) = res else return Utils.send_error(res);

        #ok();
    };

    func remove_asset(self : StableStore, key : Text) : Result<T.Assets, Text> {

        let ?asset = Map.remove(self.assets, thash, key) else return #err(ErrorMessages.asset_not_found(key));
        remove_asset_certificates(self, key, asset, false);

        #ok(asset);
    };

    public func delete_asset(self : StableStore, args : T.DeleteAssetArguments) : Result<(), Text> {
        let key = format_key(args.key);

        switch (remove_asset(self, key)) {
            case (#ok(_)) #ok();
            case (#err(msg)) #err(msg);
        };

    };

    public func clear(self : StableStore, _args : T.ClearArguments) {
        Map.clear(self.assets);
        Map.clear(self.batches);
        self.next_batch_id := 0;
        Map.clear(self.chunks);
        self.next_chunk_id := 0;

        CertifiedAssets.clear(self.certificate_store);
        Map.clear(self.copy_on_write_batches);

        // Set.clear(self.commit_principals);
        // Set.clear(self.prepare_principals);
        // Set.clear(self.manage_permissions_principals);

    };

    public func get_asset_properties(self : StableStore, _key : T.Key) : Result<T.AssetProperties, Text> {
        let key = format_key(_key);
        let ?asset = get_asset_using_aliases(self, key, true) else return #err(ErrorMessages.asset_not_found(key));

        #ok({
            is_aliased = asset.is_aliased;
            max_age = asset.max_age;
            allow_raw_access = asset.allow_raw_access;
            headers = if (Map.size(asset.headers) == 0) { null } else {
                ?Iter.toArray(Map.entries(asset.headers));
            };
        });
    };

    public func set_asset_properties(self : StableStore, args : T.SetAssetPropertiesArguments) : Result<(), Text> {
        let key = format_key(args.key);
        let ?asset = Map.get(self.assets, thash, key) else return #err(ErrorMessages.asset_not_found(key));

        switch (asset.is_aliased, args.is_aliased) {
            case (?true, ??false or ?null) {
                Debug.print("Removing aliases for " # key);
                // remove only aliases
                remove_asset_certificates(self, key, asset, true);

                // only revoke support for aliases after the certificates have been removed
                asset.is_aliased := Option.get(args.is_aliased, ?false);
            };
            case (?false or null, ??true) {
                Debug.print("Adding aliases for " # key);
                // needs to be updated first so certify_asset knows to certify for the aliases
                asset.is_aliased := ?true;
                certify_asset(self, key, asset, null);
            };
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

    public let BATCH_EXPIRY_NANOS : Nat = 300_000_000_000;

    func create_new_batch() : Batch {
        {
            var expires_at = Time.now() + BATCH_EXPIRY_NANOS;
            var commit_batch_arguments = null;
            var evidence_computation = null;
            var total_bytes = 0;
            chunk_ids = Vector.new();
        };
    };

    public func create_batch(self : StableStore) : Result<T.CreateBatchResponse, Text> {

        let curr_time = Time.now();

        var batch_with_commit_args_exists : ?(T.BatchId, ?T.EvidenceComputation) = null;

        for ((batch_id, batch) in Map.entries(self.batches)) {
            // remove expired batches only if the evidence has not been computed
            if (batch.expires_at < curr_time and Option.isNull(batch.evidence_computation)) {
                ignore remove_batch(self, batch_id);
            };

            // or evidence computation is done
            if (Option.isSome(batch.commit_batch_arguments) and Option.isNull(batch_with_commit_args_exists)) {
                batch_with_commit_args_exists := ?(batch_id, batch.evidence_computation);
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

        let batch = create_new_batch();

        ignore Map.put(self.batches, nhash, batch_id, batch);

        #ok({ batch_id });

    };

    func get_next_chunk_id(self : StableStore) : Nat {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
        chunk_id;
    };

    // todo - replace the for loop with a better solution for checking if all the bytes are within the limit
    public func create_chunk(self : StableStore, args : T.Chunk) : Result<T.CreateChunkResponse, Text> {

        switch (self.configuration.max_chunks) {
            case (?max_chunks) {
                if (Nat64.fromNat(Map.size(self.chunks)) >= max_chunks) {
                    return #err("Maximum number of chunks reached.");
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
            case (_) return #err(ErrorMessages.batch_not_found(args.batch_id));
        };

        let total_bytes_plus_new_chunk = Nat64.fromNat(batch.total_bytes) + Nat64.fromNat(args.content.size());

        switch (self.configuration.max_bytes) {
            case (?max_bytes) if (total_bytes_plus_new_chunk > max_bytes) {
                return #err("Maximum number of bytes reached. Can only add " # debug_show (max_bytes - Nat64.fromNat(batch.total_bytes)) # " more bytes but trying to add " # debug_show args.content.size());
            };
            case (_) {};
        };

        let chunk_id = get_next_chunk_id(self);

        let chunk = {
            batch_id = args.batch_id;
            content = Blob.toArray(args.content);
        };

        ignore Map.put(self.chunks, nhash, chunk_id, chunk);
        batch.expires_at := Time.now() + BATCH_EXPIRY_NANOS;
        batch.total_bytes += args.content.size();
        Vector.add(batch.chunk_ids, chunk_id);

        #ok({ chunk_id });
    };

    func create_async_chunk(self : StableStore, args : T.Chunk) : async Result<T.CreateChunkResponse, Text> {
        let res = create_chunk(self, args);
        let #ok({ chunk_id }) = res else return Utils.send_error(res);
        #ok({ chunk_id });
    };

    public func create_chunks(self : StableStore, args : T.CreateChunksArguments) : async* Result<T.CreateChunksResponse, Text> {
        let parallel = Buffer.Buffer<(async Result<T.CreateChunkResponse, Text>)>(args.content.size());
        let chunk_ids = Buffer.Buffer<Nat>(args.content.size());

        for (chunk in args.content.vals()) {
            let async_call = create_async_chunk(self, { batch_id = args.batch_id; content = chunk });
            parallel.add(async_call);
        };

        for (async_call in parallel.vals()) {
            let res = await async_call;
            let #ok({ chunk_id }) = res else return Utils.send_error(res);
            chunk_ids.add(chunk_id);
        };

        #ok({ chunk_ids = Buffer.toArray<Nat>(chunk_ids) });
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

    public func execute_batch_operations_with_same_key_sequentially(self : StableStore, operations : [T.BatchOperationKind]) : async* Result<(), Text> {

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
        // and then run all asset_groups in parallel

        let asset_groups = Map.new<T.Key, Buffer<T.BatchOperationKind>>();

        label grouping_by_key for (op in args.operations.vals()) {
            if (op == #Clear({})) {
                clear(self, {});
                Map.clear(asset_groups);
                continue grouping_by_key;
            };

            let #CreateAsset({ key }) or #SetAssetContent({ key }) or #UnsetAssetContent({
                key;
            }) or #DeleteAsset({ key }) or #SetAssetProperties({ key }) = op else Debug.trap("unreachable");

            let group = Utils.map_get_or_put(asset_groups, thash, key, func() : Buffer<T.BatchOperationKind> = Buffer.Buffer(8));
            group.add(op);

        };

        // store assets previous state before modifying them
        do {

            // there should only be backups during the execution of the batch
            assert Option.isNull(Map.get(self.copy_on_write_batches, nhash, args.batch_id));

            let backed_assets = Iter.toArray(
                Iter.map<T.Key, (T.Key, ?Assets)>(
                    Map.keys(asset_groups),
                    func(key : T.Key) : (T.Key, ?Assets) {
                        switch (get_asset_using_aliases(self, key, false)) {
                            case (?asset) (key, ?deep_copy_asset(self, asset));
                            case (null) (key, null);
                        };
                    },
                )
            );

            ignore Map.put(self.copy_on_write_batches, nhash, args.batch_id, backed_assets);

        };

        let parallel = Buffer.Buffer<(async* Result<(), Text>)>(asset_groups.size());
        for (group in Map.vals(asset_groups)) {
            let res = execute_batch_operations_with_same_key_sequentially(self, Buffer.toArray(group));
            parallel.add(res);
        };

        var opt_error_msg : ?Text = null;

        label waiting_for_parallel_calls for (async_call in parallel.vals()) {
            switch (await* async_call) {
                case (#ok(_)) {};
                case (#err(error)) {
                    opt_error_msg := ?error;
                    break waiting_for_parallel_calls;
                };
            };
        };

        switch (opt_error_msg) {
            case (?error_msg) {
                // restore assets to their previous state
                let ?backed_assets = Map.remove(self.copy_on_write_batches, nhash, args.batch_id) else return #err("Batch not found in copy_on_write_batches.");

                for ((key, opt_asset) in backed_assets.vals()) switch (opt_asset) {
                    case (?asset) {
                        // re-certify the asset to overwrite any certificates that were created during the failed batch
                        certify_asset(self, key, asset, null);
                        ignore Map.put(self.assets, thash, key, asset);
                    };
                    case (null) {
                        // remove any new assets that were created during the failed batch
                        remove_asset_and_certificate(self, key);
                    };
                };

                return #err(error_msg);
            };
            case (_) {};
        };

        ignore remove_batch(self, args.batch_id);
        ignore Map.remove(self.copy_on_write_batches, nhash, args.batch_id);

        // certifify 404 page if necessary
        #ok();
    };

    func remove_asset_and_certificate(self : StableStore, key : T.Key) {
        let ?asset = Map.remove(self.assets, thash, key) else return;
        remove_asset_certificates(self, key, asset, false);
    };

    func remove_batch(self : StableStore, batch_id : Nat) : ?Batch {
        let ?batch = Map.remove(self.batches, nhash, batch_id) else return null;

        for (chunk_id in Vector.vals(batch.chunk_ids)) {
            ignore Map.remove(self.chunks, nhash, chunk_id);
        };

        ?batch;
    };

    public func propose_commit_batch(self : StableStore, args : T.CommitBatchArguments) : Result<(), Text> {
        let ?batch = Map.get(self.batches, nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));

        if (Option.isSome(batch.commit_batch_arguments)) return #err("Batch already has proposed T.CommitBatchArguments");

        batch.commit_batch_arguments := ?args;

        #ok();
    };

    func validate_commit_proposed_batch_args(self : StableStore, args : T.CommitProposedBatchArguments) : Result<(), Text> {
        let ?(batch) = Map.get(self.batches, nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));

        if (Option.isNull(batch.commit_batch_arguments)) return #err("Batch does not have proposed CommitBatchArguments");

        let ?(#Computed(evidence)) = batch.evidence_computation else return #err("Batch does not have computed evidence.");

        if (evidence != args.evidence) return #err("Batch computed evidence (" # debug_show (evidence) # ") does not match provided evidence (" # debug_show args.evidence # ").");

        #ok();
    };

    public func commit_proposed_batch(self : StableStore, args : T.CommitProposedBatchArguments) : async* Result<(), Text> {
        let validate_result = validate_commit_proposed_batch_args(self, args);
        let #ok(_) = validate_result else return Utils.send_error(validate_result);

        let ?batch = Map.get(self.batches, nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));
        let ?commit_batch_arguments = batch.commit_batch_arguments else return #err("Batch does not have proposed CommitBatchArguments");

        switch (await* commit_batch(self, commit_batch_arguments)) {
            case (#ok(_)) {};
            // not that commit_batch reverts the changes if the batch fails
            case (#err(error)) return #err(error);
        };

        batch.commit_batch_arguments := null;

        #ok();
    };

    let DEFAULT_MAX_COMPUTE_EVIDENCE_ITERATIONS : Nat16 = 20;

    public func compute_evidence(self : StableStore, args : T.ComputeEvidenceArguments) : async* Result<?Blob, Text> {
        let ?batch = Map.get(self.batches, nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));
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
            evidence_computation := Evidence.advance(self.chunks, commit_batch_args, evidence_computation);

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
        switch (remove_batch(self, args.batch_id)) {
            case (?batch) #ok();
            case (null) #err(ErrorMessages.batch_not_found(args.batch_id));
        };
    };

    func redirect_to_certified_domain(self : StableStore, url : URL) : T.HttpResponse {
        let canister_id = self.canister_id;

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

    func build_headers(asset : Assets, encoding_name : Text, encoding_sha256 : Blob) : Map<Text, Text> {
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

        let hex = Hex.encode(Blob.toArray(encoding_sha256));
        let etag_value = "\"" # hex # "\"";
        ignore Map.put(headers, thash, "etag", etag_value);

        for ((key, value) in Map.entries(asset.headers)) {
            ignore Map.put(headers, thash, key, value);
        };

        headers;
    };

    public func http_request_streaming_callback(self : StableStore, token : T.StreamingToken) : T.StreamingCallbackResponse {

        // Debug.print("Streaming token: " # debug_show token);

        let ?asset = get_asset_using_aliases(self, token.key, true) else Debug.trap("http_request_streaming_callback(): Assets not found.");
        let ?encoding = Map.get(asset.encodings, thash, token.content_encoding) else Debug.trap("http_request_streaming_callback(): Encoding not found.");

        if (?encoding.sha256 != token.sha256) Debug.trap("http_request_streaming_callback(): SHA256 hash mismatch");

        let num_chunks = get_num_of_encoding_chunks(encoding);
        // Debug.print("num_chunks: " # debug_show num_chunks);

        let chunk : Blob = switch (get_encoding_chunk(encoding, token.index)) {
            case (?chunk) chunk;
            case (null) "";
        };

        let next_token : T.CustomStreamingToken = {
            key = token.key;
            content_encoding = token.content_encoding;
            index = token.index + 1;
            sha256 = ?encoding.sha256;
        };

        // Debug.print("Next token: " # debug_show next_token);
        let response : T.StreamingCallbackResponse = {
            body = chunk;
            // token = if (next_token.index < Nat.max(6, num_chunks)) ?(next_token) else (null);
            token = if (next_token.index < num_chunks) ?(next_token) else (null);
        };

        // Debug.print("response: " # debug_show response);

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
        opt_fallback_key : ?T.Key,
    ) : Result<T.HttpResponse, Text> {

        let headers = build_headers(asset, encoding_name, encoding.sha256);
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

        let content_chunk : Blob = switch (get_encoding_chunk(encoding, chunk_index)) {
            case (?content) content;
            case (null) "";
        };

        // Debug.print("content (index, bytes): " # debug_show (chunk_index, content_chunk.size()));
        assert content_chunk.size() <= 2 * (1024 ** 2);

        Debug.print("contains_hash: " # debug_show contains_hash);

        let (status_code, body, opt_body_hash) : (Nat16, Blob, ?Blob) = if (contains_hash) {
            (304, "", null);
            // (200, content_chunk, ?encoding.sha256);
        } else {

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
        let certified_headers_result = switch (opt_fallback_key) {
            case (?fallback_key) {
                CertifiedAssets.get_fallback_certificate(self.certificate_store, http_req, fallback_key, http_res, opt_body_hash);
            };
            case (null) CertifiedAssets.get_certificate(self.certificate_store, http_req, http_res, opt_body_hash);
        };

        switch (certified_headers_result) {
            case (#ok(certified_headers)) {

                for ((key, value) in certified_headers.vals()) {
                    headers_buffer.add((key, value));
                };

                let num_chunks = get_num_of_encoding_chunks(encoding);
                // Debug.print("num_chunks: " # debug_show num_chunks);
                // Debug.print("encoding.total_length: " # debug_show encoding.total_length);

                let certified_res : T.HttpResponse = {
                    status_code;
                    headers = Buffer.toArray(headers_buffer);
                    body;
                    upgrade = null;
                    streaming_strategy = if (num_chunks > 1) ?streaming_strategy else null;
                };

                // Debug.print("certified_res: " # debug_show { certified_res with streaming_strategy = false });

                return #ok(certified_res);
            };
            case (#err(err_msg)) return #err("CertifiedAssets.get_certificate failed: " # err_msg # "\n" # debug_show { http_req; http_res = { http_res with streaming_strategy = null } });
        };
    };

    func deep_copy_asset(self : StableStore, asset : Assets) : Assets {

        let new_asset = create_new_asset_record();

        new_asset.content_type := asset.content_type;
        new_asset.is_aliased := asset.is_aliased;
        new_asset.max_age := asset.max_age;
        new_asset.allow_raw_access := asset.allow_raw_access;

        for ((key, value) in Map.entries(asset.headers)) {
            ignore Map.put(new_asset.headers, thash, key, value);
        };

        for ((key, encoding) in Map.entries(asset.encodings)) {
            let new_encoding = create_new_asset_encoding();

            new_encoding.modified := encoding.modified;
            new_encoding.total_length := encoding.total_length;
            new_encoding.sha256 := encoding.sha256;

            // encoding chunks are immutable
            new_encoding.content_chunks := encoding.content_chunks;
            new_encoding.content_chunks_prefix_sum := encoding.content_chunks_prefix_sum;

            ignore Map.put(new_asset.encodings, thash, key, new_encoding);
        };

        new_asset;

    };

    func get_asset_using_aliases(self : StableStore, _key : Text, using_aliases : Bool) : ?Assets {

        let key = format_key(_key);
        switch (Map.get(self.assets, thash, key)) {
            case (?asset) return ?asset;
            case (_) {};
        };

        if (not using_aliases) return null;

        let reverse_alias = switch (get_key_from_aliase(self, key)) {
            case (?key) key;
            case (_) return null;
        };

        // Debug.print("reverse_alias: " # debug_show reverse_alias);

        switch (Map.get(self.assets, thash, reverse_alias)) {
            case (?asset) {
                // Debug.print("asset found using alias: " # debug_show reverse_alias);
                // Debug.print("but is it aliased? " # debug_show asset.is_aliased);
                if (asset.is_aliased != ?true) return null;
                return ?asset;
            };
            case (_) return null;
        };
    };

    func get_fallback_asset(self : StableStore, key : Text) : ?(Text, Assets) {

        let paths = Iter.toArray(Text.split(key, #text("/")));

        for (i in RevIter.range(0, paths.size()).rev()) {
            let slice = Itertools.fromArraySlice(paths, 0, i + 1);
            let possible_fallback_prefix = Text.join(("/"), slice);
            let possible_fallback_key = possible_fallback_prefix # "/index.html";

            switch (get_asset_using_aliases(self, possible_fallback_key, false)) {
                case (?asset) return ?(possible_fallback_key, asset);
                case (_) {};
            };
        };

        null;

    };

    public func build_http_response(self : StableStore, _req : T.HttpRequest, url : URL, encodings : [Text]) : Result<T.HttpResponse, Text> {
        let path = url.path.original;
        var key = format_key(path);
        var opt_fallback_key : ?Text = null;
        var req = _req;

        let cert_version : Nat16 = switch (req.certificate_version) {
            case (?v) v;
            case (_) 2;
        };

        let asset = switch (get_asset_using_aliases(self, key, true)) {
            case (?asset) {
                if ((not Option.get(asset.allow_raw_access, true)) and Text.contains(url.host.original, #text "raw.ic")) {
                    return redirect_to_certified_domain(self, url) |> #ok(_);
                };

                asset;
            };
            case (_) switch (get_fallback_asset(self, key)) {
                case (?(fallback_key, asset)) {
                    if ((not Option.get(asset.allow_raw_access, true)) and Text.contains(url.host.original, #text "raw.ic")) {
                        return redirect_to_certified_domain(self, url) |> #ok(_);
                    };

                    opt_fallback_key := ?fallback_key;

                    asset;
                };
                case (null) return #err(ErrorMessages.asset_not_found(path));
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

        if (cert_version == 1) {
            switch (asset.last_certified_encoding) {
                case (?encoding_name) {
                    let ?encoding = Map.get(asset.encodings, thash, encoding_name) else return #err("Asset.http_request(): asset.last_certified_encoding not found in asset.encodings");
                    return (build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req, opt_fallback_key));
                };
                case (null) {};
            };
        };

        let ordered_encodings = encoding_order(encodings);
        label search_for_matching_encoding for (encoding_name in ordered_encodings.vals()) {
            let ?encoding = Map.get(asset.encodings, thash, encoding_name) else continue search_for_matching_encoding;
            return (build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req, opt_fallback_key));
        };

        label search_for_encoding_in_default_list for (encoding_name in ENCODING_CERTIFICATION_ORDER.vals()) {
            let ?encoding = Map.get(asset.encodings, thash, encoding_name) else continue search_for_encoding_in_default_list;
            return (build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req, opt_fallback_key));
        };

        #err("No encoding found for " # debug_show url.path.original);

    };
};
