import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Iter "mo:base/Iter";
import Time "mo:base/Time";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";
import Text "mo:base/Text";
import Nat8 "mo:base/Nat8";
import Debug "mo:base/Debug";

import Set "mo:map/Set";
import Map "mo:map/Map";
import IC "mo:ic";
import CertifiedAssets "mo:certified-assets/Stable";
import Itertools "mo:itertools/Iter";
import Sha256 "mo:sha2/Sha256";
import Vector "mo:vector";

import Utils "Utils";
import T "Types";

module {
    type Map<K, V> = Map.Map<K, V>;
    type Set<V> = Set.Set<V>;
    type Result<T, E> = Result.Result<T, E>;
    type Time = Time.Time;
    type Vector<A> = Vector.Vector<A>;

    type StaticSha256 = Sha256.StaticSha256;

    let { nhash; thash } = Map;

    func advance(
        args : T.CommitBatchArguments,
        chunks : Map<T.ChunkId, T.Chunk>,
        evidence_computation : T.EvidenceComputation,
    ) : T.EvidenceComputation {
        switch evidence_computation {
            case (#NextOperation({ operation_index; hasher_state })) {
                let sha256 = Sha256.Digest(#sha256);
                sha256.unshare(hasher_state);
                next_operation(args, operation_index, sha256);
            };
            case (#NextChunkIndex({ operation_index; chunk_index; hasher_state })) {
                let sha256 = Sha256.Digest(#sha256);
                sha256.unshare(hasher_state);
                next_chunk_index(args, operation_index, chunk_index, sha256, chunks);
            };
            case (#Computed(evidence)) #Computed(evidence);
        };
    };

    func get_opt<A>(arr : [A], index : Nat) : ?A {
        if (index < arr.size()) {
            ?arr[index];
        } else {
            null;
        };
    };

    func next_chunk_index(args : T.CommitBatchArguments, operation_index : Nat, chunk_index : Nat, sha256 : Sha256.Digest, chunks : Map<T.ChunkId, T.Chunk>) : T.EvidenceComputation {
        let operation = args.operations[operation_index];
        switch (get_opt(args.operations, operation_index)) {
            case (? #SetAssetContent(asset_content)) switch (get_opt(asset_content.chunk_ids, chunk_index)) {
                case (?chunk_id) {
                    switch (Map.get(chunks, nhash, chunk_id)) {
                        case (?chunk) { sha256.writeBlob(chunk.content) };
                        case (_) {};
                    };

                    if (chunk_index + 1 < asset_content.chunk_ids.size()) {
                        let hasher_state = sha256.share();

                        return #NextChunkIndex({
                            operation_index;
                            chunk_index = chunk_index + 1;
                            hasher_state;
                        })

                    };
                };
                case (_) {};
            };
            case (_) {};
        };

        let hasher_state = sha256.share();

        #NextOperation({
            operation_index = operation_index + 1;
            hasher_state;
        })

    };

    let TAG_FALSE : [Nat8] = [0];
    let TAG_TRUE : [Nat8] = [1];
    let TAG_NONE : [Nat8] = [2];
    let TAG_SOME : [Nat8] = [3];

    let TAG_CREATE_ASSET : [Nat8] = [4];
    let TAG_SET_ASSET_CONTENT : [Nat8] = [5];
    let TAG_UNSET_ASSET_CONTENT : [Nat8] = [6];
    let TAG_DELETE_ASSET : [Nat8] = [7];
    let TAG_CLEAR : [Nat8] = [8];
    let TAG_SET_ASSET_PROPERTIES : [Nat8] = [9];

    func nat64_to_big_endian_bytes(n : Nat64) : [Nat8] {

        [
            Nat8.fromNat(Nat64.toNat(n >> 56)),
            Nat8.fromNat(Nat64.toNat(n >> 48)),
            Nat8.fromNat(Nat64.toNat(n >> 40)),
            Nat8.fromNat(Nat64.toNat(n >> 32)),
            Nat8.fromNat(Nat64.toNat(n >> 24)),
            Nat8.fromNat(Nat64.toNat(n >> 16)),
            Nat8.fromNat(Nat64.toNat(n >> 8)),
            Nat8.fromNat(Nat64.toNat(n)),
        ]

    };

    func hash_create_asset(sha256 : Sha256.Digest, args : T.CreateAssetArguments) {
        sha256.writeArray(TAG_CREATE_ASSET);
        sha256.writeBlob(Text.encodeUtf8(args.key));
        sha256.writeBlob(Text.encodeUtf8(args.content_type));

        switch (args.max_age) {
            case (?max_age) {
                sha256.writeArray(TAG_SOME);

                let max_age_big_endian_bytes = nat64_to_big_endian_bytes(max_age);
                sha256.writeArray(max_age_big_endian_bytes);
            };
            case (null) {
                sha256.writeArray(TAG_NONE);
            };
        };

        hash_headers(sha256, args.headers);
        hash_opt_bool(sha256, args.allow_raw_access);
        hash_opt_bool(sha256, args.enable_aliasing);
    };

    func hash_set_asset_content(sha256 : Sha256.Digest, args : T.SetAssetContentArguments) {
        sha256.writeArray(TAG_SET_ASSET_CONTENT);
        sha256.writeBlob(Text.encodeUtf8(args.key));
        sha256.writeBlob(Text.encodeUtf8(args.content_encoding));
        hash_opt(sha256, args.sha256, func(_ : Sha256.Digest, blob : Blob) = sha256.writeBlob(blob));
    };

    func hash_opt<A>(sha256 : Sha256.Digest, opt : ?A, hash_option_type : (Sha256.Digest, A) -> ()) {
        switch (opt) {
            case (?value) {
                sha256.writeArray(TAG_SOME);
                hash_option_type(sha256, value);
            };
            case (null) {
                sha256.writeArray(TAG_NONE);
            };
        };
    };

    func hash_bool(sha256 : Sha256.Digest, bool : Bool) {
        if (bool) {
            sha256.writeArray(TAG_TRUE);
        } else {
            sha256.writeArray(TAG_FALSE);
        };
    };

    func hash_opt_bool(sha256 : Sha256.Digest, opt_bool : ?Bool) {
        hash_opt(
            sha256,
            opt_bool,
            func(_ : Sha256.Digest, bool : Bool) = hash_bool(sha256, bool),
        );
    };

    func hash_headers(sha256 : Sha256.Digest, opt_headers_array : ?[(Text, Text)]) {
        switch (opt_headers_array) {
            case (?headers_array) {

                let headers = Map.fromIter<Text, Text>(headers_array.vals(), thash);
                sha256.writeArray(TAG_SOME);

                let keys = Buffer.Buffer<Text>(8);
                for (key in Map.keys(headers)) { keys.add(key) };

                keys.sort(Text.compare);

                for (key in keys.vals()) {
                    let ?value = Map.get(headers, thash, key) else Debug.trap("hash_headers: key '" # key # "' not found in headers");
                    sha256.writeBlob(Text.encodeUtf8(key));
                    sha256.writeBlob(Text.encodeUtf8(value));
                };
            };
            case (null) {
                sha256.writeArray(TAG_NONE);
            };
        };
    };

    func hash_unset_asset_content(sha256 : Sha256.Digest, args : T.UnsetAssetContentArguments) {
        sha256.writeArray(TAG_UNSET_ASSET_CONTENT);
        sha256.writeBlob(Text.encodeUtf8(args.key));
        sha256.writeBlob(Text.encodeUtf8(args.content_encoding));
    };

    func hash_delete_asset(sha256 : Sha256.Digest, args : T.DeleteAssetArguments) {
        sha256.writeArray(TAG_DELETE_ASSET);
        sha256.writeBlob(Text.encodeUtf8(args.key));
    };

    func hash_clear(sha256 : Sha256.Digest, args : T.ClearArguments) {
        sha256.writeArray(TAG_CLEAR);
    };

    func hash_set_asset_properties(sha256 : Sha256.Digest, args : T.SetAssetPropertiesArguments) {
        sha256.writeArray(TAG_SET_ASSET_PROPERTIES);
        sha256.writeBlob(Text.encodeUtf8(args.key));

        hash_opt(
            sha256,
            args.max_age,
            func(_ : Sha256.Digest, max_age : ?Nat64) {
                hash_opt(
                    sha256,
                    max_age,
                    func(_ : Sha256.Digest, max_age : Nat64) {
                        let max_age_big_endian_bytes = nat64_to_big_endian_bytes(max_age);
                        sha256.writeArray(max_age_big_endian_bytes);
                    },
                );
            },
        );

        hash_opt(sha256, args.headers, hash_headers);

        hash_opt(sha256, args.allow_raw_access, hash_opt_bool);
        hash_opt(sha256, args.is_aliased, hash_opt_bool);

    };

    func next_operation(
        args : T.CommitBatchArguments,
        operation_index : Nat,
        sha256 : Sha256.Digest,
    ) : T.EvidenceComputation {

        let opt_operation = if (args.operations.size() > operation_index) {
            ?args.operations[operation_index];
        } else {
            null;
        };

        switch (opt_operation) {
            case (null) {
                let hash = sha256.sum();
                #Computed(hash);
            };
            case (? #CreateAsset(args)) {
                hash_create_asset(sha256, args);
                let hasher_state = sha256.share();

                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher_state;
                };
            };
            case (? #SetAssetContent(args)) {
                hash_set_asset_content(sha256, args);
                let hasher_state = sha256.share();

                #NextChunkIndex {
                    operation_index;
                    chunk_index = 0;
                    hasher_state;
                };
            };
            case (? #UnsetAssetContent(args)) {
                hash_unset_asset_content(sha256, args);
                let hasher_state = sha256.share();
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher_state;
                };
            };
            case (? #DeleteAsset(args)) {
                hash_delete_asset(sha256, args);
                let hasher_state = sha256.share();
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher_state;
                };
            };
            case (? #Clear(args)) {
                hash_clear(sha256, args);
                let hasher_state = sha256.share();
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher_state;
                };
            };
            case (? #SetAssetProperties(args)) {
                hash_set_asset_properties(sha256, args);
                let hasher_state = sha256.share();
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher_state;
                };
            };
        };
    };

};
