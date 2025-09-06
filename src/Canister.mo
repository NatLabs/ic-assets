import Principal "mo:base@0/Principal";
import Result "mo:base@0/Result";
import Timer "mo:base@0/Timer";
import Error "mo:base@0/Error";

import Assets "";
import T "BaseAssets/Types";
import Utils "BaseAssets/Utils";

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
  public type AssetsInterface = T.AssetsInterface;

  type Result<A, B> = Result.Result<A, B>;

  let canister_id = Principal.fromActor(this_canister);

  stable var assets_sstore = Assets.init_stable_store(canister_id, owner);
  assets_sstore := Assets.upgrade_stable_store(assets_sstore);

  let opt_set_permissions = switch (canister_args) {
    case (#Init(init_args)) null;
    case (#Upgrade(upgrade_args)) upgrade_args.set_permissions;
  };

  let assets = Assets.Assets(assets_sstore, opt_set_permissions);

  public query func http_request_streaming_callback(token : T.StreamingToken) : async T.StreamingCallbackResponse {
    switch (assets.http_request_streaming_callback(token)) {
      case (#ok(response)) response;
      case (#err(err)) throw Error.reject(err);
    };
  };

  assets.set_streaming_callback(http_request_streaming_callback);

  public query func http_request(request : T.HttpRequest) : async T.HttpResponse {
    switch (assets.http_request(request)) {
      case (#ok(response)) response;
      case (#err(err)) throw Error.reject(err);
    };
  };

  public shared query func api_version() : async Nat16 {
    assets.api_version();
  };

  public shared query ({ caller }) func get(args : T.GetArgs) : async T.EncodedAsset {
    switch (assets.get(args)) {
      case (#ok(asset)) asset;
      case (#err(err)) throw Error.reject(err);
    };
  };

  public shared query ({ caller }) func get_chunk(args : T.GetChunkArgs) : async (T.ChunkContent) {
    switch (assets.get_chunk(args)) {
      case (#ok(chunk)) chunk;
      case (#err(err)) throw Error.reject(err);
    };
  };

  public shared ({ caller }) func grant_permission(args : T.GrantPermission) : async () {
    let res = await* assets.grant_permission(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func revoke_permission(args : T.RevokePermission) : async () {
    let res = await* assets.revoke_permission(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared query ({ caller }) func list(args : {}) : async [T.AssetDetails] {
    assets.list(args);
  };

  public shared ({ caller }) func store(args : T.StoreArgs) : async () {
    let res = assets.store(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func create_asset(args : T.CreateAssetArguments) : async () {
    let res = assets.create_asset(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func set_asset_content(args : T.SetAssetContentArguments) : async () {
    let res = await* assets.set_asset_content(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func unset_asset_content(args : T.UnsetAssetContentArguments) : async () {
    let res = assets.unset_asset_content(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func delete_asset(args : T.DeleteAssetArguments) : async () {
    let res = assets.delete_asset(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func set_asset_properties(args : T.SetAssetPropertiesArguments) : async () {
    let res = assets.set_asset_properties(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func clear(args : T.ClearArguments) : async () {
    let res = assets.clear(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func create_batch(args : {}) : async (T.CreateBatchResponse) {
    let res = assets.create_batch(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func create_chunk(args : T.CreateChunkArguments) : async (T.CreateChunkResponse) {
    let res = assets.create_chunk(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func create_chunks(args : T.CreateChunksArguments) : async T.CreateChunksResponse {
    let res = await* assets.create_chunks(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func commit_batch(args : T.CommitBatchArguments) : async () {
    let res = await* assets.commit_batch(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func propose_commit_batch(args : T.CommitBatchArguments) : async () {
    let res = assets.propose_commit_batch(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func commit_proposed_batch(args : T.CommitProposedBatchArguments) : async () {
    let res = await* assets.commit_proposed_batch(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func compute_evidence(args : T.ComputeEvidenceArguments) : async (?Blob) {
    let res = await* assets.compute_evidence(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func delete_batch(args : T.DeleteBatchArguments) : async () {
    let res = assets.delete_batch(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func authorize(principal : Principal) : async () {
    let res = await* assets.authorize(caller, principal);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func deauthorize(principal : Principal) : async () {
    let res = await* assets.deauthorize(caller, principal);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func list_authorized() : async ([Principal]) {
    let res = assets.list_authorized();
  };

  public shared ({ caller }) func list_permitted(args : T.ListPermitted) : async ([Principal]) {
    assets.list_permitted(args);
  };

  public shared ({ caller }) func take_ownership() : async () {
    let res = await* assets.take_ownership(caller);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func get_configuration() : async (T.ConfigurationResponse) {
    let res = assets.get_configuration(caller);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func configure(args : T.ConfigureArguments) : async () {
    let res = assets.configure(caller, args);
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
  };

  public shared ({ caller }) func certified_tree({}) : async (T.CertifiedTree) {
    let res = assets.certified_tree();
    await* Utils.throw_if_error(res);
    Utils.extract_result(res);
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
