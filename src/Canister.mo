import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Timer "mo:base/Timer";

import Asset "";

shared ({ caller = owner }) actor class AssetCanister(canister_args : Asset.CanisterArgs) = this_canister {

    public type Key = Asset.Key;
    public type Path = Asset.Path;
    public type BatchId = Asset.BatchId;
    public type ChunkId = Asset.ChunkId;
    public type Time = Int;

    public type CreateAssetArguments = Asset.CreateAssetArguments;
    public type SetAssetContentArguments = Asset.SetAssetContentArguments;
    public type UnsetAssetContentArguments = Asset.UnsetAssetContentArguments;
    public type DeleteAssetArguments = Asset.DeleteAssetArguments;
    public type ClearArguments = Asset.ClearArguments;

    public type SetAssetPropertiesArguments = Asset.SetAssetPropertiesArguments;
    public type BatchOperationKind = Asset.BatchOperationKind;
    public type AssetDetails = Asset.AssetDetails;
    public type AssetEncodingDetails = Asset.AssetEncodingDetails;
    public type CommitBatchArguments = Asset.CommitBatchArguments;
    public type CommitProposedBatchArguments = Asset.CommitProposedBatchArguments;
    public type ComputeEvidenceArguments = Asset.ComputeEvidenceArguments;
    public type DeleteBatchArguments = Asset.DeleteBatchArguments;
    public type StreamingCallbackToken = Asset.StreamingCallbackToken;
    public type ConfigurationResponse = Asset.ConfigurationResponse;
    public type ConfigureArguments = Asset.ConfigureArguments;
    public type Permission = Asset.Permission;
    public type GrantPermission = Asset.GrantPermission;
    public type RevokePermission = Asset.RevokePermission;
    public type ListPermitted = Asset.ListPermitted;
    public type InitArgs = Asset.InitArgs;
    public type UpgradeArgs = Asset.UpgradeArgs;
    public type SetPermissions = Asset.SetPermissions;
    public type GetArgs = Asset.GetArgs;
    public type EncodedAsset = Asset.EncodedAsset;
    public type GetChunkArgs = StreamingCallbackToken;
    public type ChunkContent = Asset.ChunkContent;
    public type Chunk = Asset.Chunk;
    public type ListArgs = Asset.ListArgs;
    public type CertifiedTree = Asset.CertifiedTree;
    public type AssetProperties = Asset.AssetProperties;
    public type StoreArgs = Asset.StoreArgs;
    public type ValidationResult = Asset.ValidationResult;
    public type CreateBatchResponse = Asset.CreateBatchResponse;
    public type CreateChunkArguments = Asset.CreateChunkArguments;
    public type CreateChunkResponse = Asset.CreateChunkResponse;
    public type CreateBatchArguments = Asset.CreateBatchArguments;
    public type StreamingCallback = Asset.StreamingCallback;
    public type StreamingCallbackResponse = Asset.StreamingCallbackResponse;
    public type StreamingToken = Asset.StreamingToken;
    public type CustomStreamingToken = Asset.CustomStreamingToken;
    public type HttpRequest = Asset.HttpRequest;
    public type HttpResponse = Asset.HttpResponse;
    public type Service = Asset.Service;
    public type CanisterArgs = Asset.CanisterArgs;
    public type CanisterInterface = Asset.CanisterInterface;

    type Result<A, B> = Result.Result<A, B>;

    public func canister_id() : async Principal {
        Principal.fromActor(this_canister);
    };

    stable var assets_sstore = Asset.init_stable_store(owner);
    assets_sstore := Asset.migrate(assets_sstore);

    let assets = Asset.Asset(assets_sstore);

    public query func http_request_streaming_callback(token_blob : Asset.StreamingToken) : async ?(Asset.StreamingCallbackResponse) {
        ?assets.http_request_streaming_callback(token_blob);
    };

    public query func http_request(request : Asset.HttpRequest) : async Asset.HttpResponse {
        assets.http_request(request);
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

    public shared query ({ caller }) func retrieve(key : Asset.Key) : async (Blob) {
        assets.retrieve(caller, key);
    };

    public shared query ({ caller }) func get(args : Asset.GetArgs) : async Asset.EncodedAsset {
        assets.get(args);
    };

    public shared query ({ caller }) func get_chunk(args : Asset.GetChunkArgs) : async (Asset.ChunkContent) {
        assets.get_chunk(args);
    };

    public shared ({ caller }) func grant_permission(args : Asset.GrantPermission) : async () {
        await* assets.grant_permission(caller, args);
    };

    public shared ({ caller }) func revoke_permission(args : Asset.RevokePermission) : async () {
        await* assets.revoke_permission(caller, args);
    };

    public shared query ({ caller }) func list(args : {}) : async [Asset.AssetDetails] {
        assets.list(
            args
        );
    };

    public shared ({ caller }) func store(args : Asset.StoreArgs) : async () {
        assets.store(caller, args);
    };

    public shared ({ caller }) func create_asset(args : Asset.CreateAssetArguments) : async () {
        assets.create_asset(caller, args);
    };

    public shared ({ caller }) func set_asset_content(args : Asset.SetAssetContentArguments) : async () {
        assets.set_asset_content(caller, args);
    };

    public shared ({ caller }) func unset_asset_content(args : Asset.UnsetAssetContentArguments) : async () {
        assets.unset_asset_content(caller, args);
    };

    public shared ({ caller }) func delete_asset(args : Asset.DeleteAssetArguments) : async () {
        assets.delete_asset(caller, args);
    };

    public shared query ({ caller }) func get_asset_properties(key : Asset.Key) : async (Asset.AssetProperties) {
        assets.get_asset_properties(caller, key);
    };

    public shared ({ caller }) func set_asset_properties(args : Asset.SetAssetPropertiesArguments) : async () {
        assets.set_asset_properties(caller, args);
    };

    public shared ({ caller }) func clear(args : Asset.ClearArguments) : async () {
        assets.clear(caller, args);
    };

    public shared ({ caller }) func create_batch(args : {}) : async (Asset.CreateBatchResponse) {
        assets.create_batch(caller, args);
    };

    public shared ({ caller }) func create_chunk(args : Asset.CreateChunkArguments) : async (Asset.CreateChunkResponse) {
        assets.create_chunk(caller, args);
    };

    public shared ({ caller }) func commit_batch(args : Asset.CommitBatchArguments) : async () {
        await* assets.commit_batch(caller, args);
    };

    public shared ({ caller }) func propose_commit_batch(args : Asset.CommitBatchArguments) : async () {
        assets.propose_commit_batch(caller, args);
    };

    public shared ({ caller }) func commit_proposed_batch(args : Asset.CommitProposedBatchArguments) : async () {
        await* assets.commit_proposed_batch(caller, args);
    };

    public shared ({ caller }) func compute_evidence(args : Asset.ComputeEvidenceArguments) : async (?Blob) {
        assets.compute_evidence(caller, args);
    };

    public shared ({ caller }) func delete_batch(args : Asset.DeleteBatchArguments) : async () {
        assets.delete_batch(caller, args);
    };

    public shared ({ caller }) func authorize(principal : Principal) : async () {
        await* assets.authorize(caller, principal);
    };

    public shared ({ caller }) func deauthorize(principal : Principal) : async () {
        await* assets.deauthorize(caller, principal);
    };

    public shared ({ caller }) func list_authorized() : async ([Principal]) {
        assets.list_authorized();
    };

    public shared ({ caller }) func list_permitted(args : Asset.ListPermitted) : async ([Principal]) {
        assets.list_permitted(args);
    };

    public shared ({ caller }) func take_ownership() : async () {
        await* assets.take_ownership(caller);
    };

    public shared ({ caller }) func get_configuration() : async (Asset.ConfigurationResponse) {
        assets.get_configuration(caller);
    };

    public shared ({ caller }) func configure(args : Asset.ConfigureArguments) : async () {
        assets.configure(caller, args);
    };

    public shared ({ caller }) func certified_tree({}) : async (Asset.CertifiedTree) {
        assets.certified_tree();
    };

    public shared ({ caller }) func validate_grant_permission(args : Asset.GrantPermission) : async (Result<Text, Text>) {
        assets.validate_grant_permission(args);
    };

    public shared ({ caller }) func validate_revoke_permission(args : Asset.RevokePermission) : async (Result<Text, Text>) {
        assets.validate_revoke_permission(args);
    };

    public shared ({ caller }) func validate_take_ownership() : async (Result<Text, Text>) {
        assets.validate_take_ownership();
    };

    public shared ({ caller }) func validate_commit_proposed_batch(args : Asset.CommitProposedBatchArguments) : async (Result<Text, Text>) {
        assets.validate_commit_proposed_batch(args);
    };

    public shared ({ caller }) func validate_configure(args : Asset.ConfigureArguments) : async (Result<Text, Text>) {
        assets.validate_configure(args);
    };

};
