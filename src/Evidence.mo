import Array "mo:base/Array";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Iter "mo:base/Iter";
import Time "mo:base/Time";
import Nat64 "mo:base/Nat64";
import Nat16 "mo:base/Nat16";

import Set "mo:map/Set";
import Map "mo:map/Map";
import IC "mo:ic";
import CertifiedAssets "mo:certified-assets";
import Itertools "mo:itertools/Iter";
import SHA256 "mo:sha2-shared/Sha256/Static";
import Vector "mo:vector";

import Utils "Utils";
import T "Types";

module {
    type Map<K, V> = Map.Map<K, V>;
    type Set<V> = Set.Set<V>;
    type Result<T, E> = Result.Result<T, E>;
    type Time = Time.Time;
    type Vector<A> = Vector.Vector<A>;

    type SHA256 = SHA256.StaticSha256;

    func advance(args: T.CommitBatchArguments, chunks: Map<T.ChunkId, T.Chunk>, evidence_computation: T.EvidenceComputation) : T.EvidenceComputation {
         switch evidence_computation {
            case (#NextOperation({ operation_index })){
                next_operation(args, operation_index, hasher);
            };
            case (#NextChunkIndex({ operation_index; chunk_index})){
                next_chunk_index(args, operation_index, chunk_index, hasher, chunks);
            };
            case (#Computed(evidence )) #Computed(evidence);
        };
    };

    func next_operation(
        args: T.CommitBatchArguments,
        operation_index: Nat,
        hasher: SHA256,
    ) : T.EvidenceComputation {
        switch (args.operations[operation_index]) {
            case(null) {
                let sha256: Blob = Sha256.sum(hasher);
                Computed(sha256)
            };
            case (?#CreateAsset(args)) {
                hash_create_asset(hasher, args);
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher;
                }
            };
            case (?#SetAssetContent(args)){
                hash_set_asset_content(hasher, args);
                NextChunkIndex {
                    operation_index;
                    chunk_index = 0;
                    hasher;
                };
            };
            case (?#UnsetAssetContent(args)){
                hash_unset_asset_content(hasher, args);
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher;
                }
            };
            case (?#DeleteAsset(args)){
                hash_delete_asset(hasher, args);
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher;
                }
            };
            case (?#Clear(args)){
                hash_clear(hasher, args);
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher;
                }
            };
            case (?#SetAssetProperties(args)){
                hash_set_asset_properties(hasher, args);
                #NextOperation {
                    operation_index = operation_index + 1;
                    hasher;
                }
            };
        }
    };


}