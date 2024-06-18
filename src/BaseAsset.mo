import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Time "mo:base/Time";
import Blob "mo:base/Blob";

import Set "mo:map/Set";
import Map "mo:map/Map";
import IC "mo:ic";
import CertifiedAssets "mo:certified-assets";
import Itertools "mo:itertools/Iter";
import { URL; Headers } "mo:http-parser";
import HttpTypes "mo:http-types";
import HttpParser "mo:http-parser";

import T "Types";

import AssetUtils "AssetUtils";
import Utils "Utils";
import Migrations "Migrations";

module {

    type Map<K, V> = Map.Map<K, V>;
    type Set<V> = Set.Set<V>;
    type Result<T, E> = Result.Result<T, E>;

    let { ic } = IC;

    let { phash } = Set;

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
    public type CanisterArgs = T.CanisterArgs;
    public type InitArgs = T.InitArgs;
    public type UpgradeArgs = T.UpgradeArgs;
    public type SetPermissions = T.SetPermissions;
    public type GetArgs = T.GetArgs;
    public type EncodedAsset = T.EncodedAsset;
    public type GetChunkArgs = StreamingCallbackToken;
    public type Chunk = T.Chunk;
    public type ListArgs = T.ListArgs;
    public type CertifiedTree = T.CertifiedTree;
    public type AssetProperties = T.AssetProperties;
    public type StoreArgs = T.StoreArgs;
    public type ValidationResult = T.ValidationResult;
    public type CreateBatchResponse = T.CreateBatchResponse;
    public type CreateChunkArguments = T.CreateChunkArguments;
    public type CreateChunkResponse = T.CreateChunkResponse;

    public type StableStore = T.StableStoreV0;

    public func init_stable_store(owner : Principal) : StableStore {
        let state : StableStore = {
            var canister_id = null;
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
            var next_batch_id = 1;

            commit_principals = Set.new();
            prepare_principals = Set.new();
            manage_permissions_principals = Set.new();
        };

        AssetUtils.grant_permission(state, owner, #Commit);

        state;
    };

    public func set_canister_id(self : StableStore, canister_id : Principal) : () {
        self.canister_id := ?canister_id;
    };

    public func set_streaming_callback(self : StableStore, callback : HttpTypes.StreamingCallback) {
        self.streaming_callback := ?callback;
    };

    public func http_request_streaming_callback(self : StableStore, token_blob : T.StreamingToken) : T.StreamingCallbackResponse {
        let res = AssetUtils.http_request_streaming_callback(self, token_blob);
        let streaming_response = Utils.extract_result(res);
    };

    public func from_version(versions : T.VersionedStableStore) : StableStore {
        let migrated_store = Migrations.migrate(versions);
        Migrations.get_current_state(migrated_store);
    };

    public func share_version(self : StableStore) : T.VersionedStableStore {
        #v0(self);
    };

    public func api_version() : Nat16 = 1;

    public func certified_tree(self : StableStore) : T.CertifiedTree {
        let result = CertifiedAssets.get_certified_tree(self.certificate_store, null);
        Utils.extract_result(result);
    };

    public func retrieve(self : StableStore, caller : Principal, key : T.Key) : Blob {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        Utils.extract_result(AssetUtils.retrieve(self, key));
    };

    public func get(self : StableStore, args : T.GetArgs) : T.EncodedAsset {
        Utils.extract_result(AssetUtils.get(self, args));
    };

    public func get_chunk(self : StableStore, args : T.GetChunkArgs) : T.ChunkContent {
        Utils.extract_result(AssetUtils.get_chunk(self, args));
    };

    public func list(self : StableStore, args : {}) : [T.AssetDetails] {
        AssetUtils.list(self, args);
    };

    public func store(self : StableStore, caller : Principal, args : StoreArgs) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(AssetUtils.store(self, args));
    };

    public func create_asset(self : StableStore, caller : Principal, args : CreateAssetArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(AssetUtils.create_asset(self, args));
    };

    public func set_asset_content(self : StableStore, caller : Principal, args : SetAssetContentArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(AssetUtils.set_asset_content(self, args));
    };

    public func unset_asset_content(self : StableStore, caller : Principal, args : T.UnsetAssetContentArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(AssetUtils.unset_asset_content(self, args));
    };

    public func delete_asset(self : StableStore, caller : Principal, args : DeleteAssetArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(AssetUtils.delete_asset(self, args));
    };

    public func get_asset_properties(self : StableStore, caller : Principal, key : T.Key) : T.AssetProperties {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        Utils.extract_result(AssetUtils.get_asset_properties(self, key));
    };

    public func set_asset_properties(self : StableStore, caller : Principal, args : SetAssetPropertiesArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(AssetUtils.set_asset_properties(self, args));
    };

    public func clear(self : StableStore, caller : Principal, args : ClearArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        AssetUtils.clear(self, args);
    };

    public func create_batch(self : StableStore, caller : Principal, args : {}) : (T.CreateBatchResponse) {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        Utils.extract_result(AssetUtils.create_batch(self, Time.now()));
    };

    public func create_chunk(self : StableStore, caller : Principal, args : T.CreateChunkArguments) : (T.CreateChunkResponse) {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        let chunk_id = Utils.extract_result(AssetUtils.create_chunk(self, args));
        { chunk_id };
    };

    public func commit_batch(self : StableStore, caller : Principal, args : CommitBatchArguments) : async* () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(await* AssetUtils.commit_batch(self, args));
    };

    public func propose_commit_batch(self : StableStore, caller : Principal, args : CommitBatchArguments) : () {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        Utils.extract_result(AssetUtils.propose_commit_batch(self, args));
    };

    public func commit_proposed_batch(self : StableStore, caller : Principal, args : CommitProposedBatchArguments) : async* () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));
        Utils.extract_result(await* AssetUtils.commit_proposed_batch(self, args));
    };

    public func compute_evidence(self : StableStore, caller : Principal, args : ComputeEvidenceArguments) : (?Blob) {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        ?"";
    };

    public func delete_batch(self : StableStore, caller : Principal, args : DeleteBatchArguments) : () {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));
        Utils.extract_result(AssetUtils.delete_batch(self, args));
    };

    public func authorize(self : StableStore, caller : Principal, principal : Principal) : async* () {
        Utils.assert_result(await* AssetUtils.is_manager_or_controller(self, caller));

        AssetUtils.grant_permission(self, principal, #Commit);
    };

    public func deauthorize(self : StableStore, caller : Principal, principal : Principal) : async* () {
        var has_permission = if (principal == caller) {
            AssetUtils.has_permission(self, principal, #Commit);
        } else false;

        if (not has_permission) {
            Utils.assert_result(await* AssetUtils.is_controller(self, caller));
        };

        AssetUtils.revoke_permission(self, principal, #Commit);
    };

    public func list_authorized(self : StableStore) : [Principal] {
        AssetUtils.get_permission_list(self, #Commit);
    };

    public func grant_permission(self : StableStore, caller : Principal, args : T.GrantPermission) : async* () {
        Utils.assert_result(await* AssetUtils.is_manager_or_controller(self, caller));

        AssetUtils.grant_permission(self, args.to_principal, args.permission);
    };

    public func revoke_permission(self : StableStore, caller : Principal, args : RevokePermission) : async* () {
        var has_permission = if (args.of_principal == caller) {
            AssetUtils.has_permission(self, args.of_principal, args.permission);
        } else {
            AssetUtils.has_permission(self, caller, #ManagePermissions);
        };

        if (not has_permission) {
            Utils.assert_result(await* AssetUtils.is_controller(self, caller));
        };

        AssetUtils.revoke_permission(self, args.of_principal, args.permission);
    };

    public func list_permitted(self : StableStore, { permission } : ListPermitted) : [Principal] {
        AssetUtils.get_permission_list(self, permission);
    };

    public func take_ownership(self : StableStore, caller : Principal) : async* () {
        Utils.assert_result(await* AssetUtils.is_controller(self, caller));

        Set.clear(self.commit_principals);
        Set.clear(self.prepare_principals);
        Set.clear(self.manage_permissions_principals);

        ignore Set.put(self.commit_principals, phash, caller);
    };

    public func get_configuration(self : StableStore, caller : Principal) : T.ConfigurationResponse {
        Utils.assert_result(AssetUtils.can_prepare(self, caller));

        let config : T.ConfigurationResponse = {
            max_batches = self.configuration.max_batches;
            max_chunks = self.configuration.max_chunks;
            max_bytes = self.configuration.max_bytes;
        };

        config;
    };

    public func configure(self : StableStore, caller : Principal, args : T.ConfigureArguments) : () {
        Utils.assert_result(AssetUtils.can_commit(self, caller));

        switch (args.max_batches) {
            case (null) {};
            case (?max_batches) {
                self.configuration.max_batches := max_batches;
            };
        };

        switch (args.max_chunks) {
            case (null) {};
            case (?max_chunks) {
                self.configuration.max_chunks := max_chunks;
            };
        };

        switch (args.max_bytes) {
            case (null) {};
            case (?max_bytes) {
                self.configuration.max_bytes := max_bytes;
            };
        };
    };

    public func validate_grant_permission(self : StableStore, args : GrantPermission) : Result<Text, Text> {
        #ok(
            "grant " # debug_show args.permission # " permission to principal " # debug_show args.to_principal
        );
    };

    public func validate_revoke_permission(self : StableStore, args : RevokePermission) : Result<Text, Text> {
        #ok("revoke " # debug_show args.permission # " permission from principal " # debug_show args.of_principal);
    };

    public func validate_take_ownership() : Result<Text, Text> {
        #ok("revoke all permissions, then gives the caller Commit permissions");
    };

    public func validate_commit_proposed_batch(self : StableStore, args : CommitProposedBatchArguments) : Result<Text, Text> {
        #ok("commit proposed batch " # debug_show args.batch_id # "with evidence " # debug_show args.evidence);
    };

    public func validate_configure(self : StableStore, args : T.ConfigureArguments) : Result<Text, Text> {
        #ok("configure: " # debug_show args);
    };

    public func http_request(self : StableStore, req : T.HttpRequest) : T.HttpResponse {
        let headers = HttpParser.Headers(req.headers);
        let content_encoding = switch (headers.get("content-encoding")) {
            case (?encoding) { encoding };
            case (null) { ["identity"] };
        };

        let url = HttpParser.URL(req.url, headers);
        AssetUtils.build_http_response(self, req, url, content_encoding);
    };
};
