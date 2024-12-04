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
import CertifiedAssets "mo:certified-assets/Stable";
import Itertools "mo:itertools/Iter";
import { URL; Headers } "mo:http-parser";
import HttpParser "mo:http-parser";

import T "Types";

import AssetUtils "AssetUtils";
import Utils "Utils";

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

    public type StableStore = T.StableStore;

    public let MAX_CHUNK_SIZE = AssetUtils.MAX_CHUNK_SIZE;

    public func init_stable_store(canister_id : Principal, owner : Principal) : StableStore {
        let state : StableStore = {
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

        AssetUtils.grant_permission(state, owner, #Commit);

        state;
    };

    public func set_canister_id(self : StableStore, canister_id : Principal) : () {
        self.canister_id := canister_id;
    };

    public func set_streaming_callback(self : StableStore, callback : T.StreamingCallback) {
        self.streaming_callback := ?callback;
    };

    public func get_streaming_callback(self : StableStore) : ?T.StreamingCallback {
        self.streaming_callback;
    };

    public func get_canister_id(self : StableStore) : Principal {
        self.canister_id;
    };

    public func exists(self : StableStore, key : T.Key) : Bool {
        AssetUtils.exists(self, key);
    };

    public func http_request_streaming_callback(self : StableStore, token : T.StreamingToken) : T.StreamingCallbackResponse {
        AssetUtils.http_request_streaming_callback(self, token);
    };

    public func api_version() : Nat16 = 1;

    public func certified_tree(self : StableStore) : Result<T.CertifiedTree, Text> {
        CertifiedAssets.get_certified_tree(self.certificate_store, null);
    };

    public func get(self : StableStore, args : T.GetArgs) : Result<T.EncodedAsset, Text> {
        AssetUtils.get(self, args);
    };

    public func get_chunk(self : StableStore, args : T.GetChunkArgs) : Result<T.ChunkContent, Text> {
        AssetUtils.get_chunk(self, args);
    };

    public func list(self : StableStore, args : {}) : [T.AssetDetails] {
        AssetUtils.list(self, args);
    };

    public func store(self : StableStore, caller : Principal, args : StoreArgs) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) AssetUtils.store(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func create_asset(self : StableStore, caller : Principal, args : CreateAssetArguments) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) AssetUtils.create_asset(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func set_asset_content(self : StableStore, caller : Principal, args : SetAssetContentArguments) : async* Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) await* AssetUtils.set_asset_content(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func unset_asset_content(self : StableStore, caller : Principal, args : T.UnsetAssetContentArguments) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) AssetUtils.unset_asset_content(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func delete_asset(self : StableStore, caller : Principal, args : DeleteAssetArguments) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) AssetUtils.delete_asset(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func get_asset_properties(self : StableStore, key : T.Key) : Result<T.AssetProperties, Text> {
        AssetUtils.get_asset_properties(self, key);
    };

    public func set_asset_properties(self : StableStore, caller : Principal, args : SetAssetPropertiesArguments) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) AssetUtils.set_asset_properties(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func clear(self : StableStore, caller : Principal, args : ClearArguments) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) #ok(AssetUtils.clear(self, args));
            case (#err(msg)) #err(msg);
        };
    };

    public func create_batch(self : StableStore, caller : Principal, args : {}) : Result<(T.CreateBatchResponse), Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) AssetUtils.create_batch(self);
            case (#err(msg)) #err(msg);
        };
    };

    public func create_chunk(self : StableStore, caller : Principal, args : T.CreateChunkArguments) : Result<(T.CreateChunkResponse), Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) AssetUtils.create_chunk(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func create_chunks(self : StableStore, caller : Principal, args : T.CreateChunksArguments) : async* Result<T.CreateChunksResponse, Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) await* AssetUtils.create_chunks(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func commit_batch(self : StableStore, caller : Principal, args : CommitBatchArguments) : async* Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) await* AssetUtils.commit_batch(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func propose_commit_batch(self : StableStore, caller : Principal, args : CommitBatchArguments) : Result<(), Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) AssetUtils.propose_commit_batch(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func commit_proposed_batch(self : StableStore, caller : Principal, args : CommitProposedBatchArguments) : async* Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) await* AssetUtils.commit_proposed_batch(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func compute_evidence(self : StableStore, caller : Principal, args : ComputeEvidenceArguments) : async* Result<(?Blob), Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) await* AssetUtils.compute_evidence(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func delete_batch(self : StableStore, caller : Principal, args : DeleteBatchArguments) : Result<(), Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) AssetUtils.delete_batch(self, args);
            case (#err(msg)) #err(msg);
        };
    };

    public func authorize(self : StableStore, caller : Principal, principal : Principal) : async* Result<(), Text> {
        switch (await* AssetUtils.is_manager_or_controller(self, caller)) {
            case (#ok(_)) AssetUtils.grant_permission(self, principal, #Commit);
            case (#err(msg)) return #err(msg);
        };

        #ok

    };

    public func deauthorize(self : StableStore, caller : Principal, principal : Principal) : async* Result<(), Text> {

        var has_permission = if (principal == caller) {
            AssetUtils.has_permission(self, principal, #Commit);
        } else false;

        if (not has_permission) {
            switch (await* AssetUtils.is_controller(self, caller)) {
                case (#ok(_)) {};
                case (#err(msg)) return #err(msg);
            };
        };

        AssetUtils.revoke_permission(self, principal, #Commit);

        #ok();
    };

    public func list_authorized(self : StableStore) : [Principal] {
        AssetUtils.get_permission_list(self, #Commit);
    };

    public func grant_permission(self : StableStore, caller : Principal, args : T.GrantPermission) : async* Result<(), Text> {
        switch (await* AssetUtils.is_manager_or_controller(self, caller)) {
            case (#ok(_)) AssetUtils.grant_permission(self, args.to_principal, args.permission);
            case (#err(msg)) return #err(msg);
        };

        #ok;
    };

    public func revoke_permission(self : StableStore, caller : Principal, args : RevokePermission) : async* Result<(), Text> {
        let is_caller_trying_to_revoke_their_own_permission = args.of_principal == caller;

        if (is_caller_trying_to_revoke_their_own_permission) {
            // caller does not have said permission
            if (not AssetUtils.has_permission(self, args.of_principal, args.permission)) {
                return #ok();
            };
        };

        if (not is_caller_trying_to_revoke_their_own_permission) {
            // check if caller has manager or controller permissions
            switch (await* AssetUtils.is_manager_or_controller(self, caller)) {
                case (#ok(_)) {};
                case (#err(msg)) return #err(msg);
            };
        };

        AssetUtils.revoke_permission(self, args.of_principal, args.permission);

        #ok();
    };

    public func list_permitted(self : StableStore, { permission } : ListPermitted) : [Principal] {
        AssetUtils.get_permission_list(self, permission);
    };

    public func take_ownership(self : StableStore, caller : Principal) : async* Result<(), Text> {
        switch (await* AssetUtils.is_controller(self, caller)) {
            case (#ok(_)) {};
            case (#err(msg)) return #err(msg);
        };

        Set.clear(self.commit_principals);
        Set.clear(self.prepare_principals);
        Set.clear(self.manage_permissions_principals);

        ignore Set.put(self.commit_principals, phash, caller);

        #ok;
    };

    public func get_configuration(self : StableStore, caller : Principal) : Result<T.ConfigurationResponse, Text> {
        switch (AssetUtils.can_prepare(self, caller)) {
            case (#ok(_)) {};
            case (#err(msg)) return #err(msg);
        };

        let config : T.ConfigurationResponse = {
            max_batches = self.configuration.max_batches;
            max_chunks = self.configuration.max_chunks;
            max_bytes = self.configuration.max_bytes;
        };

        #ok(config);
    };

    public func configure(self : StableStore, caller : Principal, args : T.ConfigureArguments) : Result<(), Text> {
        switch (AssetUtils.can_commit(self, caller)) {
            case (#ok(_)) {};
            case (#err(msg)) return #err(msg);
        };

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

        #ok();
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

    public func http_request(self : StableStore, req : T.HttpRequest) : Result<T.HttpResponse, Text> {
        let headers = HttpParser.Headers(req.headers);
        let content_encoding = switch (headers.get("content-encoding")) {
            case (?encoding) { encoding };
            case (null) { ["identity"] };
        };

        let url = HttpParser.URL(req.url, headers);

        AssetUtils.build_http_response(self, req, url, content_encoding);

    };
};
