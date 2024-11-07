import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Timer "mo:base/Timer";
import Error "mo:base/Error";

import Assets "";
import Utils "Utils";

shared ({ caller = owner }) actor class AssetsCanister(canister_args : Assets.CanisterArgs) = this_canister {

    public type Key = Assets.Key;
    public type Path = Assets.Path;
    public type BatchId = Assets.BatchId;
    public type ChunkId = Assets.ChunkId;
    public type Time = Int;

    public type CreateAssetArguments = Assets.CreateAssetArguments;
    public type SetAssetContentArguments = Assets.SetAssetContentArguments;
    public type UnsetAssetContentArguments = Assets.UnsetAssetContentArguments;
    public type DeleteAssetArguments = Assets.DeleteAssetArguments;
    public type ClearArguments = Assets.ClearArguments;

    public type SetAssetPropertiesArguments = Assets.SetAssetPropertiesArguments;
    public type BatchOperationKind = Assets.BatchOperationKind;
    public type AssetDetails = Assets.AssetDetails;
    public type AssetEncodingDetails = Assets.AssetEncodingDetails;
    public type CommitBatchArguments = Assets.CommitBatchArguments;
    public type CommitProposedBatchArguments = Assets.CommitProposedBatchArguments;
    public type ComputeEvidenceArguments = Assets.ComputeEvidenceArguments;
    public type DeleteBatchArguments = Assets.DeleteBatchArguments;
    public type StreamingCallbackToken = Assets.StreamingCallbackToken;
    public type ConfigurationResponse = Assets.ConfigurationResponse;
    public type ConfigureArguments = Assets.ConfigureArguments;
    public type Permission = Assets.Permission;
    public type GrantPermission = Assets.GrantPermission;
    public type RevokePermission = Assets.RevokePermission;
    public type ListPermitted = Assets.ListPermitted;
    public type InitArgs = Assets.InitArgs;
    public type UpgradeArgs = Assets.UpgradeArgs;
    public type SetPermissions = Assets.SetPermissions;
    public type GetArgs = Assets.GetArgs;
    public type EncodedAsset = Assets.EncodedAsset;
    public type GetChunkArgs = StreamingCallbackToken;
    public type ChunkContent = Assets.ChunkContent;
    public type Chunk = Assets.Chunk;
    public type ListArgs = Assets.ListArgs;
    public type CertifiedTree = Assets.CertifiedTree;
    public type AssetProperties = Assets.AssetProperties;
    public type StoreArgs = Assets.StoreArgs;
    public type ValidationResult = Assets.ValidationResult;
    public type CreateBatchResponse = Assets.CreateBatchResponse;
    public type CreateChunkArguments = Assets.CreateChunkArguments;
    public type CreateChunkResponse = Assets.CreateChunkResponse;
    public type CreateBatchArguments = Assets.CreateBatchArguments;
    public type StreamingCallback = Assets.StreamingCallback;
    public type StreamingCallbackResponse = Assets.StreamingCallbackResponse;
    public type StreamingToken = Assets.StreamingToken;
    public type CustomStreamingToken = Assets.CustomStreamingToken;
    public type HttpRequest = Assets.HttpRequest;
    public type HttpResponse = Assets.HttpResponse;
    public type Service = Assets.Service;
    public type CanisterArgs = Assets.CanisterArgs;
    public type CanisterInterface = Assets.CanisterInterface;

    type Result<A, B> = Result.Result<A, B>;

    public func canister_id() : async Principal {
        Principal.fromActor(this_canister);
    };

    stable var assets_sstore = Assets.init_stable_store(owner);
    assets_sstore := Assets.migrate(assets_sstore);

    let assets = Assets.Assets(assets_sstore);

    public query func http_request_streaming_callback(token : Assets.StreamingToken) : async (Assets.StreamingCallbackResponse) {
        assets.http_request_streaming_callback(token);
    };

    public query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
        Utils.extract_result(assets.http_request(request));
    };

    public shared func init() : async () {
        let id = await canister_id();
        assets.set_canister_id(id);
        assets.set_streaming_callback(http_request_streaming_callback);
    };

    system func timer(setGlobalTimer : Nat64 -> ()) : async () {
        let id = await canister_id();
        assets.set_canister_id(id);
        assets.set_streaming_callback(http_request_streaming_callback);
    };

    public shared query func api_version() : async Nat16 {
        assets.api_version();
    };

    public shared query ({ caller }) func get(args : Assets.GetArgs) : async Assets.EncodedAsset {
        assets.get(args) |> Utils.extract_result(_);
    };

    public shared query ({ caller }) func get_chunk(args : Assets.GetChunkArgs) : async (Assets.ChunkContent) {
        assets.get_chunk(args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func grant_permission(args : Assets.GrantPermission) : async () {
        (await* assets.grant_permission(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func revoke_permission(args : Assets.RevokePermission) : async () {
        (await* assets.revoke_permission(caller, args)) |> Utils.extract_result(_);
    };

    public shared query ({ caller }) func list(args : {}) : async [Assets.AssetDetails] {
        assets.list(
            args
        );
    };

    public shared ({ caller }) func store(args : Assets.StoreArgs) : async () {
        assets.store(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_asset(args : Assets.CreateAssetArguments) : async () {
        assets.create_asset(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func set_asset_content(args : Assets.SetAssetContentArguments) : async () {
        (await* assets.set_asset_content(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func unset_asset_content(args : Assets.UnsetAssetContentArguments) : async () {
        assets.unset_asset_content(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func delete_asset(args : Assets.DeleteAssetArguments) : async () {
        assets.delete_asset(caller, args) |> Utils.extract_result(_);
    };

    public shared query func get_asset_properties(key : Assets.Key) : async (Assets.AssetProperties) {
        assets.get_asset_properties(key) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func set_asset_properties(args : Assets.SetAssetPropertiesArguments) : async () {
        assets.set_asset_properties(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func clear(args : Assets.ClearArguments) : async () {
        assets.clear(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_batch(args : {}) : async (Assets.CreateBatchResponse) {
        assets.create_batch(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_chunk(args : Assets.CreateChunkArguments) : async (Assets.CreateChunkResponse) {
        (await* assets.create_chunk(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func create_chunks(args : Assets.CreateChunksArguments) : async Assets.CreateChunksResponse {
        (await* assets.create_chunks(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func commit_batch(args : Assets.CommitBatchArguments) : async () {
        switch (await* assets.commit_batch(caller, args)) {
            case (#ok(_)) ();
            case (#err(msg)) throw Error.reject(msg);
        };
    };

    public shared ({ caller }) func propose_commit_batch(args : Assets.CommitBatchArguments) : async () {
        assets.propose_commit_batch(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func commit_proposed_batch(args : Assets.CommitProposedBatchArguments) : async () {
        switch (await* assets.commit_proposed_batch(caller, args)) {
            case (#ok(_)) ();
            case (#err(msg)) throw Error.reject(msg);
        };
    };

    public shared ({ caller }) func compute_evidence(args : Assets.ComputeEvidenceArguments) : async (?Blob) {
        (await* assets.compute_evidence(caller, args)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func delete_batch(args : Assets.DeleteBatchArguments) : async () {
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

    public shared ({ caller }) func list_permitted(args : Assets.ListPermitted) : async ([Principal]) {
        assets.list_permitted(args);
    };

    public shared ({ caller }) func take_ownership() : async () {
        (await* assets.take_ownership(caller)) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func get_configuration() : async (Assets.ConfigurationResponse) {
        assets.get_configuration(caller) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func configure(args : Assets.ConfigureArguments) : async () {
        assets.configure(caller, args) |> Utils.extract_result(_);
    };

    public shared ({ caller }) func certified_tree({}) : async (Assets.CertifiedTree) {
        assets.certified_tree() |> Utils.extract_result(_);
    };

    public shared ({ caller }) func validate_grant_permission(args : Assets.GrantPermission) : async (Result<Text, Text>) {
        assets.validate_grant_permission(args);
    };

    public shared ({ caller }) func validate_revoke_permission(args : Assets.RevokePermission) : async (Result<Text, Text>) {
        assets.validate_revoke_permission(args);
    };

    public shared ({ caller }) func validate_take_ownership() : async (Result<Text, Text>) {
        assets.validate_take_ownership();
    };

    public shared ({ caller }) func validate_commit_proposed_batch(args : Assets.CommitProposedBatchArguments) : async (Result<Text, Text>) {
        assets.validate_commit_proposed_batch(args);
    };

    public shared ({ caller }) func validate_configure(args : Assets.ConfigureArguments) : async (Result<Text, Text>) {
        assets.validate_configure(args);
    };

};
