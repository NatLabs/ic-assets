import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Timer "mo:base/Timer";
import Error "mo:base/Error";

import Assets "";
import T "Types";
import Utils "Utils";

shared ({ caller = owner }) actor class AssetsCanister(canister_args : T.CanisterArgs) = this_canister {

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

    type Result<A, B> = Result.Result<A, B>;

    let canister_id = Principal.fromActor(this_canister);

    stable let assets_sstore_1 = Assets.init_stable_store(canister_id, owner);
    stable let assets_sstore_2 = Assets.upgrade(assets_sstore_1);

    let assets = Assets.Assets(assets_sstore_2);

    public query func http_request_streaming_callback(token : T.StreamingToken) : async (T.StreamingCallbackResponse) {
        assets.http_request_streaming_callback(token);
    };

    assets.set_streaming_callback(http_request_streaming_callback);

    public query func http_request(request : T.HttpRequest) : async T.HttpResponse {
        Utils.extract_result(assets.http_request(request));
    };

    public shared query func api_version() : async Nat16 {
        assets.api_version();
    };

    public shared query ({ caller }) func get(args : T.GetArgs) : async T.EncodedAsset {
        assets.get(args) |> Utils.extract_result(_);
    };

    public shared query ({ caller }) func get_chunk(args : T.GetChunkArgs) : async (T.ChunkContent) {
        assets.get_chunk(args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func grant_permission(args : T.GrantPermission) : async () {
        (await* assets.grant_permission(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func revoke_permission(args : T.RevokePermission) : async () {
        (await* assets.revoke_permission(caller, args)) |> Utils.extract_result(_);
    };

    public shared query ({ caller }) func list(args : {}) : async [T.AssetDetails] {
        assets.list(
            args
        );
    };

    public shared ({ caller }) func store(args : T.StoreArgs) : async () {
        assets.store(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_asset(args : T.CreateAssetArguments) : async () {
        assets.create_asset(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func set_asset_content(args : T.SetAssetContentArguments) : async () {
        (await* assets.set_asset_content(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func unset_asset_content(args : T.UnsetAssetContentArguments) : async () {
        assets.unset_asset_content(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func delete_asset(args : T.DeleteAssetArguments) : async () {
        assets.delete_asset(caller, args) |> Utils.extract_result(_);
    };

    public shared query func get_asset_properties(key : T.Key) : async (T.AssetProperties) {
        assets.get_asset_properties(key) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func set_asset_properties(args : T.SetAssetPropertiesArguments) : async () {
        assets.set_asset_properties(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func clear(args : T.ClearArguments) : async () {
        assets.clear(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_batch(args : {}) : async (T.CreateBatchResponse) {
        assets.create_batch(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_chunk(args : T.CreateChunkArguments) : async (T.CreateChunkResponse) {
        (assets.create_chunk(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_chunks(args : T.CreateChunksArguments) : async T.CreateChunksResponse {
        (await* assets.create_chunks(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func commit_batch(args : T.CommitBatchArguments) : async () {
        switch (await* assets.commit_batch(caller, args)) {
            case (#ok(_)) ();
            case (#err(msg)) throw Error.reject(msg);
        };
    };

    public shared ({ caller }) func propose_commit_batch(args : T.CommitBatchArguments) : async () {
        assets.propose_commit_batch(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func commit_proposed_batch(args : T.CommitProposedBatchArguments) : async () {
        switch (await* assets.commit_proposed_batch(caller, args)) {
            case (#ok(_)) ();
            case (#err(msg)) throw Error.reject(msg);
        };
    };

    public shared ({ caller }) func compute_evidence(args : T.ComputeEvidenceArguments) : async (?Blob) {
        (await* assets.compute_evidence(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func delete_batch(args : T.DeleteBatchArguments) : async () {
        assets.delete_batch(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func authorize(principal : Principal) : async () {
        (await* assets.authorize(caller, principal)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func deauthorize(principal : Principal) : async () {
        (await* assets.deauthorize(caller, principal)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func list_authorized() : async ([Principal]) {
        assets.list_authorized();
    };

    public shared ({ caller }) func list_permitted(args : T.ListPermitted) : async ([Principal]) {
        assets.list_permitted(args);
    };

    public shared ({ caller }) func take_ownership() : async () {
        (await* assets.take_ownership(caller)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func get_configuration() : async (T.ConfigurationResponse) {
        assets.get_configuration(caller) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func configure(args : T.ConfigureArguments) : async () {
        assets.configure(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func certified_tree({}) : async (T.CertifiedTree) {
        assets.certified_tree() |> Utils.extract_result(_);
    };

    public shared ({ caller }) func validate_grant_permission(args : T.GrantPermission) : async (Result<Text, Text>) {
        assets.validate_grant_permission(args);
    };

    public shared ({ caller }) func validate_revoke_permission(args : T.RevokePermission) : async (Result<Text, Text>) {
        assets.validate_revoke_permission(args);
    };

    public shared ({ caller }) func validate_take_ownership() : async (Result<Text, Text>) {
        assets.validate_take_ownership();
    };

    public shared ({ caller }) func validate_commit_proposed_batch(args : T.CommitProposedBatchArguments) : async (Result<Text, Text>) {
        assets.validate_commit_proposed_batch(args);
    };

    public shared ({ caller }) func validate_configure(args : T.ConfigureArguments) : async (Result<Text, Text>) {
        assets.validate_configure(args);
    };

};
