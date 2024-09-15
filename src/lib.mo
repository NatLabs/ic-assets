import Debug "mo:base/Debug";
import Result "mo:base/Result";

import { URL; Headers } "mo:http-parser";
import Map "mo:map/Map";

import T "Types";
import BaseAsset "BaseAsset";
import Migrations "Migrations";

module {
    let { thash } = Map;
    type Result<T, E> = Result.Result<T, E>;

    public type Key = T.Key;
    public type Path = T.Path;
    public type BatchId = T.BatchId;
    public type ChunkId = T.ChunkId;
    public type Time = Int;

    public type CreateAssetArguments = T.CreateAssetArguments;
    public type SetAssetContentArguments = T.SetAssetContentArguments;
    public type UnsetAssetContentArguments = T.UnsetAssetContentArguments;
    public type DeleteAssetArguments = T.DeleteAssetArguments;
    public type ClearArguments = T.ClearArguments;

    public type SetAssetPropertiesArguments = T.SetAssetPropertiesArguments;
    public type BatchOperation = T.BatchOperation;
    public type BatchOperationKind = T.BatchOperationKind;
    public type AssetDetails = T.AssetDetails;
    public type AssetEncodingDetails = T.AssetEncodingDetails;
    public type CommitBatchArguments = T.CommitBatchArguments;
    public type CommitProposedBatchArguments = T.CommitProposedBatchArguments;
    public type ComputeEvidenceArguments = T.ComputeEvidenceArguments;
    public type DeleteBatchArguments = T.DeleteBatchArguments;
    public type StreamingCallbackToken = T.StreamingCallbackToken;
    public type ConfigurationResponse = T.ConfigurationResponse;
    public type ConfigureArguments = T.ConfigureArguments;
    public type Permission = T.Permission;
    public type GrantPermission = T.GrantPermission;
    public type RevokePermission = T.RevokePermission;
    public type ListPermitted = T.ListPermitted;
    public type InitArgs = T.InitArgs;
    public type UpgradeArgs = T.UpgradeArgs;
    public type SetPermissions = T.SetPermissions;
    public type GetArgs = T.GetArgs;
    public type EncodedAsset = T.EncodedAsset;
    public type GetChunkArgs = StreamingCallbackToken;
    public type ChunkContent = T.ChunkContent;
    public type Chunk = T.Chunk;
    public type ListArgs = T.ListArgs;
    public type CertifiedTree = T.CertifiedTree;
    public type AssetProperties = T.AssetProperties;
    public type StoreArgs = T.StoreArgs;
    public type ValidationResult = T.ValidationResult;
    public type CreateBatchResponse = T.CreateBatchResponse;
    public type CreateChunkArguments = T.CreateChunkArguments;
    public type CreateChunkResponse = T.CreateChunkResponse;
    public type CreateBatchArguments = T.CreateBatchArguments;
    public type StreamingCallback = T.StreamingCallback;
    public type StreamingCallbackResponse = T.StreamingCallbackResponse;
    public type StreamingToken = T.StreamingToken;
    public type CustomStreamingToken = T.CustomStreamingToken;
    public type HttpRequest = T.HttpRequest;
    public type HttpResponse = T.HttpResponse;
    public type Service = T.Service;
    public type CanisterArgs = T.CanisterArgs;
    public type CanisterInterface = T.CanisterInterface;

    public type VersionedStableStore = T.VersionedStableStore;
    type StableStoreV0 = T.StableStoreV0;

    public func init_stable_store(owner : Principal) : VersionedStableStore {
        let stable_store_v0 = BaseAsset.init_stable_store(owner);
        #v0(stable_store_v0);
    };

    public func migrate(asset_versions : VersionedStableStore) : VersionedStableStore {
        Migrations.migrate(asset_versions);
    };

    public class Assets(sstore : VersionedStableStore) {
        let state = Migrations.get_current_state(sstore);

        public func api_version() : Nat16 = BaseAsset.api_version();

        public func set_canister_id(id : Principal) : () {
            BaseAsset.set_canister_id(state, id);
        };

        public func set_streaming_callback(callback : StreamingCallback) {
            BaseAsset.set_streaming_callback(state, callback);
        };

        public func get_streaming_callback() : ?StreamingCallback {
            BaseAsset.get_streaming_callback(state);
        };

        public func get_canister_id() : ?Principal {
            BaseAsset.get_canister_id(state);
        };

        public func http_request_streaming_callback(token_blob : StreamingToken) : StreamingCallbackResponse {
            BaseAsset.http_request_streaming_callback(state, token_blob);
        };

        public func exists(key : T.Key) : Bool {
            let ?_ = Map.get(state.assets, thash, key) else return false;
            true;
        };

        public func certified_tree() : T.CertifiedTree {
            BaseAsset.certified_tree(state);
        };

        public func retrieve(caller : Principal, key : T.Key) : Blob {
            BaseAsset.retrieve(state, caller, key);
        };

        public func get(args : T.GetArgs) : T.EncodedAsset {
            BaseAsset.get(state, args);
        };

        public func get_chunk(args : T.GetChunkArgs) : T.ChunkContent {
            BaseAsset.get_chunk(state, args);
        };

        public func list(args : {}) : [T.AssetDetails] {
            BaseAsset.list(state, args);
        };

        public func store(caller : Principal, args : StoreArgs) : () {
            BaseAsset.store(state, caller, args);
        };

        public func create_asset(caller : Principal, args : CreateAssetArguments) : () {
            BaseAsset.create_asset(state, caller, args);
        };

        public func set_asset_content(caller : Principal, args : SetAssetContentArguments) : async* () {
            await* BaseAsset.set_asset_content(state, caller, args);
        };

        public func unset_asset_content(caller : Principal, args : T.UnsetAssetContentArguments) : () {
            BaseAsset.unset_asset_content(state, caller, args);
        };

        public func delete_asset(caller : Principal, args : DeleteAssetArguments) : () {
            BaseAsset.delete_asset(state, caller, args);
        };

        public func get_asset_properties(caller : Principal, key : T.Key) : T.AssetProperties {
            BaseAsset.get_asset_properties(state, caller, key);
        };

        public func set_asset_properties(caller : Principal, args : SetAssetPropertiesArguments) : () {
            BaseAsset.set_asset_properties(state, caller, args);
        };

        public func clear(caller : Principal, args : ClearArguments) : () {
            BaseAsset.clear(state, caller, args);
        };

        public func create_batch(caller : Principal, args : {}) : (CreateBatchResponse) {
            BaseAsset.create_batch(state, caller, args);
        };

        public func create_chunk(caller : Principal, args : T.Chunk) : (T.CreateChunkResponse) {
            BaseAsset.create_chunk(state, caller, args);
        };

        public func commit_batch(caller : Principal, args : CommitBatchArguments) : async* () {
            await* BaseAsset.commit_batch(state, caller, args);
        };

        public func propose_commit_batch(caller : Principal, args : CommitBatchArguments) : () {
            BaseAsset.propose_commit_batch(state, caller, args);
        };

        public func commit_proposed_batch(caller : Principal, args : CommitProposedBatchArguments) : async* () {
            await* BaseAsset.commit_proposed_batch(state, caller, args);
        };

        public func compute_evidence(caller : Principal, args : ComputeEvidenceArguments) : (?Blob) {
            BaseAsset.compute_evidence(state, caller, args);
        };

        public func delete_batch(caller : Principal, args : DeleteBatchArguments) : () {
            BaseAsset.delete_batch(state, caller, args);
        };

        public func authorize(caller : Principal, principal : Principal) : async* () {
            await* BaseAsset.authorize(state, caller, principal);
        };

        public func deauthorize(caller : Principal, principal : Principal) : async* () {
            await* BaseAsset.deauthorize(state, caller, principal);
        };

        public func list_authorized() : [Principal] {
            BaseAsset.list_authorized(state);
        };

        public func grant_permission(caller : Principal, args : T.GrantPermission) : async* () {
            await* BaseAsset.grant_permission(state, caller, args);
        };

        public func revoke_permission(caller : Principal, args : RevokePermission) : async* () {
            await* BaseAsset.revoke_permission(state, caller, args);
        };

        public func list_permitted(permitted : ListPermitted) : [Principal] {
            BaseAsset.list_permitted(state, permitted);
        };

        public func take_ownership(caller : Principal) : async* () {
            await* BaseAsset.take_ownership(state, caller);
        };

        public func get_configuration(caller : Principal) : T.ConfigurationResponse {
            BaseAsset.get_configuration(state, caller);
        };

        public func configure(caller : Principal, args : T.ConfigureArguments) : () {
            BaseAsset.configure(state, caller, args);
        };

        public func validate_grant_permission(args : GrantPermission) : Result<Text, Text> {
            BaseAsset.validate_grant_permission(state, args);
        };

        public func validate_revoke_permission(args : RevokePermission) : Result<Text, Text> {
            BaseAsset.validate_revoke_permission(state, args);
        };

        public func validate_take_ownership() : Result<Text, Text> {
            BaseAsset.validate_take_ownership();
        };

        public func validate_commit_proposed_batch(args : CommitProposedBatchArguments) : Result<Text, Text> {
            BaseAsset.validate_commit_proposed_batch(state, args);
        };

        public func validate_configure(args : T.ConfigureArguments) : Result<Text, Text> {
            BaseAsset.validate_configure(state, args);
        };

        public func http_request(req : T.HttpRequest) : T.HttpResponse {
            BaseAsset.http_request(state, req);
        };

    };

};
