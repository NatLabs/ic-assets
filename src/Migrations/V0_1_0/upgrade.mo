import Result "mo:base/Result";
import Array "mo:base/Array";
import Time "mo:base/Time";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Option "mo:base/Option";
import Principal "mo:base/Principal";

import Set "mo:map/Set";
import Map "mo:map/Map";
import CertifiedAssets "mo:certified-assets/Stable";
import SHA256 "mo:sha2/Sha256";
import Vector "mo:vector";

import V0_types "../V0/types";
import V0_1_0_types "./types";

module {
    type Vector<A> = Vector.Vector<A>;
    type Map<K, V> = Map.Map<K, V>;
    type Set<V> = Set.Set<V>;
    type Result<T, E> = Result.Result<T, E>;
    type Time = Time.Time;
    let { thash } = Map;

    public func upgrade_from_v0(v0 : V0_types.StableStore) : V0_1_0_types.StableStore {

        let v1 : V0_1_0_types.StableStore = {
            var canister_id = Option.get(v0.canister_id, Principal.fromText("aaaaa-aa"));
            var streaming_callback = null;

            assets = upgrade_assets(v0.assets);
            certificate_store = v0.certificate_store;
            chunks = Map.new();
            batches = Map.new();
            copy_on_write_batches = Map.new();

            commit_principals = v0.commit_principals;
            prepare_principals = v0.prepare_principals;
            manage_permissions_principals = v0.manage_permissions_principals;

            configuration = v0.configuration;

            var next_chunk_id = v0.next_chunk_id;
            var next_batch_id = v0.next_batch_id;

        };

    };

    public func init_stable_store(canister_id : Principal, owner : Principal) : V0_1_0_types.StableStore {
        let state : V0_1_0_types.StableStore = {
            var canister_id = canister_id;
            var streaming_callback = null;
            assets = Map.new();
            certificate_store = CertifiedAssets.init_stable_store();

            configuration = {
                var max_batches = null;
                var max_chunks = null;
                var max_bytes = null;
            };

            chunks = Map.new();
            var next_chunk_id = 1;

            batches = Map.new();
            copy_on_write_batches = Map.new();
            var next_batch_id = 1;

            commit_principals = Set.new();
            prepare_principals = Set.new();
            manage_permissions_principals = Set.new();

        };

        state;
    };

    func upgrade_content_chunks(content_chunks_vec : Vector<Blob>) : [[Nat8]] {
        Array.tabulate(
            Vector.size(content_chunks_vec),
            func(i : Nat) : [Nat8] {
                let content_chunks = Vector.get(content_chunks_vec, i);
                Blob.toArray(content_chunks);
            },
        );
    };

    func get_prefix_sum(array : [Blob]) : [Nat] {
        var sum = 0;

        Array.tabulate(
            array.size(),
            func(i : Nat) : Nat {
                sum += array.get(i).size();
                sum;
            },
        );
    };

    func upgrade_asset_encodings(encodings : Map<Text, V0_types.AssetEncoding>) : Map<Text, V0_1_0_types.AssetEncoding> {

        Map.fromIter<Text, V0_1_0_types.AssetEncoding>(
            Iter.map<(Text, V0_types.AssetEncoding), (Text, V0_1_0_types.AssetEncoding)>(
                Map.entries(encodings),
                func((encoding_id, encoding) : (Text, V0_types.AssetEncoding)) : (Text, V0_1_0_types.AssetEncoding) {
                    let new_encoding : V0_1_0_types.AssetEncoding = {
                        var modified = encoding.modified;
                        var total_length = encoding.total_length;
                        var certified = encoding.certified;
                        var sha256 = encoding.sha256;

                        var content_chunks : [[Nat8]] = upgrade_content_chunks(encoding.content_chunks);
                        var content_chunks_prefix_sum : [Nat] = get_prefix_sum(Vector.toArray<Blob>(encoding.content_chunks));
                    };

                    (encoding_id, new_encoding);
                },
            ),
            thash,
        );

    };

    func upgrade_assets(assets : Map<Text, V0_types.Assets>) : Map<Text, V0_1_0_types.Assets> {

        Map.fromIter<Text, V0_1_0_types.Assets>(
            Iter.map<(Text, V0_types.Assets), (Text, V0_1_0_types.Assets)>(
                Map.entries(assets),
                func((asset_id, asset) : (Text, V0_types.Assets)) : (Text, V0_1_0_types.Assets) {
                    let new_asset = {
                        var content_type = asset.content_type;
                        headers = asset.headers;
                        var is_aliased = asset.is_aliased;
                        var max_age = asset.max_age;
                        var allow_raw_access = asset.allow_raw_access;

                        var last_certified_encoding = Map.keys(asset.encodings).next();
                        encodings = upgrade_asset_encodings(asset.encodings);
                    };

                    (asset_id, new_asset);
                },
            ),
            thash,
        );

    };

    // func upgrade_batch_chunks(batch_chunks : Map<Nat, V0_types.Chunk>) : Map<Nat, V0_1_0_types.StoredChunk> {
    //     Map.fromIter(
    //         Iter.map(
    //             Map.entries(batch_chunks),
    //             func((chunk_id, { content; batch_id }) : (Nat, V0_types.Chunk)) : (Nat, V0_1_0_types.StoredChunk) {
    //                 (chunk_id, { content = Blob.toArray(content); batch_id });
    //             },
    //         ),
    //         thash,
    //     );
    // };

    // func upgrade_batches(batches : Map<Nat, V0_types.Batch>) : Map<Nat, V0_1_0_types.Batch> {
    //     Map.fromIter(
    //         Iter.map(
    //             Map.entries(batches),
    //             func((batch_id, batch) : (Nat, V0_types.Batch)) : (Nat, V0_1_0_types.Batch) {
    //                 (
    //                     batch_id,
    //                     {
    //                         var expires_at = batch.expires_at;
    //                         var commit_batch_arguments = batch.commit_batch_arguments;
    //                         var evidence_computation = batch.evidence_computation;
    //                         var total_bytes = ;
    //                         chunk_ids = Vector.new<ChunkId>();
    //                     },
    //                 );
    //             },
    //         ),
    //         thash,
    //     );
    // };

};
