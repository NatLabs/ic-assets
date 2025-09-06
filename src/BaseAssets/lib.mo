import Array "mo:base@0/Array";
import Debug "mo:base@0/Debug";
import Option "mo:base@0/Option";
import Principal "mo:base@0/Principal";
import Result "mo:base@0/Result";
import Time "mo:base@0/Time";
import Blob "mo:base@0/Blob";

import Set "mo:map@9/Set";
import Map "mo:map@9/Map";
import CertifiedAssets "mo:certified-assets@0/Stable";
import Itertools "mo:itertools@0/Iter";
import { URL; Headers } "mo:http-parser@0";
import HttpParser "mo:http-parser@0";
import MemoryRegion "mo:memory-region@1/MemoryRegion";

import T "Types";

import AssetUtils "AssetUtils";
import Utils "Utils";
import Permissions "Permissions";
import Upload "Upload";
import FileSystem "FileSystem";
import Http "Http";
import Const "Const";

/// This module contains the main logic for the assets library.
/// It is responsible for handling all the requests and managing the assets.
module {

  type Map<K, V> = Map.Map<K, V>;
  type Set<V> = Set.Set<V>;
  type Result<T, E> = Result.Result<T, E>;

  public type Key = T.Key;
  public type Path = T.Path;
  public type BatchId = T.BatchId;
  public type ChunkId = T.ChunkId;
  public type Time = Int;

  /// Initializes the stable store for the asset library with the given canister ID and owner's principal.
  public func init_stable_store(canister_id : Principal, owner : Principal) : T.StableStore {

    let shared_region = MemoryRegion.new();

    let state : T.StableStore = {
      var canister_id = canister_id;
      var streaming_callback = null;

      shared_region;
      fs = FileSystem.new(shared_region);
      upload = Upload.new(shared_region);
      permissions = Permissions.new();

    };

    Permissions.grant_permission(state.permissions, owner, #Commit);
    Permissions.grant_permission(state.permissions, canister_id, #Commit);

    state;
  };

  /// Sets the canister ID for the assets library.
  public func set_canister_id(self : T.StableStore, canister_id : Principal) : () {
    self.canister_id := canister_id;
  };

  /// Sets the streaming callback for the assets library.
  public func set_streaming_callback(self : T.StableStore, callback : T.StreamingCallback) {
    self.streaming_callback := ?callback;
  };

  /// Gets the streaming callback for the assets library.
  public func get_streaming_callback(self : T.StableStore) : ?T.StreamingCallback {
    self.streaming_callback;
  };

  /// Gets the canister ID for the assets library.
  public func get_canister_id(self : T.StableStore) : Principal {
    self.canister_id;
  };

  /// Checks if an asset exists in the assets library.
  public func exists(self : T.StableStore, key : T.Key) : Bool {
    AssetUtils.exists(self, key);
  };

  /// Handles HTTP request streaming callback.
  public func http_request_streaming_callback(self : T.StableStore, token : T.StreamingToken) : T.Result<T.StreamingCallbackResponse, Text> {
    Http.http_request_streaming_callback(self, token);
  };

  /// Returns the API version.
  public func api_version() : Nat16 = 1;

  /// Gets the certified tree for the assets library.
  public func certified_tree(self : T.StableStore) : Result<T.CertifiedTree, Text> {
    CertifiedAssets.get_certified_tree(self.fs.certs, null);
  };

  /// Gets an encoded asset from the assets library.
  public func get(self : T.StableStore, args : T.GetArgs) : T.Result<T.EncodedAsset, Text> {
    AssetUtils.get(self, { args with key = Utils.format_key(args.key) });
  };

  /// Gets a chunk of an asset from the assets library.
  public func get_chunk(self : T.StableStore, args : T.GetChunkArgs) : Result<T.ChunkContent, Text> {
    AssetUtils.get_chunk(self, args);
  };

  /// Lists all asset details in the assets library.
  public func list(self : T.StableStore, args : {}) : [T.AssetDetails] {
    AssetUtils.list(self, args);
  };

  /// Stores an asset in the assets library.
  public func store(self : T.StableStore, caller : Principal, args : T.StoreArgs) : T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.store(self, { args with key = Utils.format_key(args.key) });
      case (#err(msg)) #err(msg);
    };
  };

  /// Creates an asset in the assets library.
  public func create_asset(self : T.StableStore, caller : Principal, args : T.CreateAssetArguments) : T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.create_asset(self, { args with key = Utils.format_key(args.key) });
      case (#err(msg)) #err(msg);
    };
  };

  /// Sets the content of an asset in the assets library.
  public func set_asset_content(self : T.StableStore, caller : Principal, args : T.SetAssetContentArguments) : async* T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) await* AssetUtils.set_asset_content(self, { args with key = Utils.format_key(args.key) });
      case (#err(msg)) #err(msg);
    };
  };

  /// Unsets the content of an asset in the assets library.
  public func unset_asset_content(self : T.StableStore, caller : Principal, args : T.UnsetAssetContentArguments) : T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.unset_asset_content(self, { args with key = Utils.format_key(args.key) });
      case (#err(msg)) #err(msg);
    };
  };

  /// Deletes an asset from the assets library.
  public func delete_asset(self : T.StableStore, caller : Principal, args : T.DeleteAssetArguments) : T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.delete_asset(self, { args with key = Utils.format_key(args.key) });
      case (#err(msg)) #err(msg);
    };
  };

  /// Gets the properties of an asset in the assets library.
  public func get_asset_properties(self : T.StableStore, key : T.Key) : T.Result<T.AssetProperties, Text> {
    AssetUtils.get_asset_properties(self, Utils.format_key(key));
  };

  /// Sets the properties of an asset in the assets library.
  public func set_asset_properties(self : T.StableStore, caller : Principal, args : T.SetAssetPropertiesArguments) : T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.set_asset_properties(self, { args with key = Utils.format_key(args.key) });
      case (#err(msg)) #err(msg);
    };
  };

  /// Clears the assets library.
  public func clear(self : T.StableStore, caller : Principal, args : T.ClearArguments) : Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) #ok(AssetUtils.clear(self, args));
      case (#err(msg)) #err(msg);
    };
  };

  /// Creates a batch in the assets library.
  public func create_batch(self : T.StableStore, caller : Principal, _args : {}) : Result<(T.CreateBatchResponse), Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) Upload.create_batch(self.upload, Time.now(), Const.BATCH_EXPIRY_DURATION);
      case (#err(msg)) #err(msg);
    };
  };

  /// Creates a chunk in the assets library.
  public func create_chunk(self : T.StableStore, caller : Principal, args : T.CreateChunkArguments) : Result<(T.CreateChunkResponse), Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) Upload.create_chunk(self.upload, args, Time.now(), Const.BATCH_EXPIRY_DURATION);
      case (#err(msg)) #err(msg);
    };
  };

  /// Creates multiple chunks in the assets library.
  public func create_chunks(self : T.StableStore, caller : Principal, args : T.CreateChunksArguments) : async* Result<T.CreateChunksResponse, Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) await* AssetUtils.create_chunks(self, args);
      case (#err(msg)) #err(msg);
    };
  };

  /// Formats the keys in the commit batch arguments.
  func format_commit_batch_arg_keys(args : T.CommitBatchArguments) : T.CommitBatchArguments {

    func format_key(op : T.BatchOperationKind) : T.BatchOperationKind {
      switch (op) {
        case (#CreateAsset(args)) #CreateAsset({
          args with key = Utils.format_key(args.key)
        });
        case (#SetAssetContent(args)) #SetAssetContent({
          args with key = Utils.format_key(args.key)
        });
        case (#UnsetAssetContent(args)) #UnsetAssetContent({
          args with key = Utils.format_key(args.key)
        });
        case (#DeleteAsset(args)) #DeleteAsset({
          args with key = Utils.format_key(args.key)
        });
        case (#SetAssetProperties(args)) #SetAssetProperties({
          args with key = Utils.format_key(args.key)
        });
        case (#Clear(args)) #Clear(args);
      };

    };

    {
      args with operations = Array.map(args.operations, format_key);
    };
  };

  /// Commits a batch in the assets library.
  public func commit_batch(self : T.StableStore, caller : Principal, args : T.CommitBatchArguments) : async* Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) await* AssetUtils.commit_batch(self, format_commit_batch_arg_keys(args));
      case (#err(msg)) #err(msg);
    };
  };

  /// Proposes a commit batch in the assets library.
  public func propose_commit_batch(self : T.StableStore, caller : Principal, args : T.CommitBatchArguments) : Result<(), Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.propose_commit_batch(self, format_commit_batch_arg_keys(args));
      case (#err(msg)) #err(msg);
    };
  };

  /// Commits a proposed batch in the assets library.
  public func commit_proposed_batch(self : T.StableStore, caller : Principal, args : T.CommitProposedBatchArguments) : async* Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) await* AssetUtils.commit_proposed_batch(self, args);
      case (#err(msg)) #err(msg);
    };
  };

  /// Computes evidence for a batch in the assets library.
  public func compute_evidence(self : T.StableStore, caller : Principal, args : T.ComputeEvidenceArguments) : async* Result<(?Blob), Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) await* Upload.compute_evidence(self.upload, args);
      case (#err(msg)) #err(msg);
    };
  };

  /// Deletes a batch from the assets library.
  public func delete_batch(self : T.StableStore, caller : Principal, args : T.DeleteBatchArguments) : Result<(), Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) AssetUtils.delete_batch(self, args);
      case (#err(msg)) return #err(msg);
    };

  };

  /// Authorizes a principal in the assets library.
  public func authorize(self : T.StableStore, caller : Principal, principal : Principal) : async* Result<(), Text> {
    switch (await* Permissions.is_manager_or_controller(self.permissions, self.canister_id, caller)) {
      case (#ok(_)) Permissions.grant_permission(self.permissions, principal, #Commit);
      case (#err(msg)) return #err(msg);
    };

    #ok

  };

  /// Deauthorizes a principal in the assets library.
  public func deauthorize(self : T.StableStore, caller : Principal, principal : Principal) : async* Result<(), Text> {

    var has_permission = if (principal == caller) {
      Permissions.has_permission(self.permissions, principal, #Commit);
    } else false;

    if (not has_permission) {
      switch (await* Permissions.is_controller(self.permissions, self.canister_id, caller)) {
        case (#ok(_)) {};
        case (#err(msg)) return #err(msg);
      };
    };

    Permissions.revoke_permission(self.permissions, principal, #Commit);

    #ok();
  };

  /// Lists all authorized principals in the assets library.
  public func list_authorized(self : T.StableStore) : [Principal] {
    Permissions.get_permission_list(self.permissions, #Commit);
  };

  /// Grants permission to a principal in the assets library.
  public func grant_permission(self : T.StableStore, caller : Principal, args : T.GrantPermission) : async* Result<(), Text> {
    switch (await* Permissions.is_manager_or_controller(self.permissions, self.canister_id, caller)) {
      case (#ok(_)) Permissions.grant_permission(self.permissions, args.to_principal, args.permission);
      case (#err(msg)) return #err(msg);
    };

    #ok;
  };

  /// Revokes permission from a principal in the assets library.
  public func revoke_permission(self : T.StableStore, caller : Principal, args : T.RevokePermission) : async* Result<(), Text> {
    let is_caller_trying_to_revoke_their_own_permission = args.of_principal == caller;

    if (is_caller_trying_to_revoke_their_own_permission) {
      // caller does not have said permission
      if (not Permissions.has_permission(self.permissions, args.of_principal, args.permission)) {
        return #ok();
      };
    };

    if (not is_caller_trying_to_revoke_their_own_permission) {
      // check if caller has manager or controller permissions
      switch (await* Permissions.is_manager_or_controller(self.permissions, self.canister_id, caller)) {
        case (#ok(_)) {};
        case (#err(msg)) return #err(msg);
      };
    };

    Permissions.revoke_permission(self.permissions, args.of_principal, args.permission);

    #ok();
  };

  /// Lists all principals permitted for a specific permission in the assets library.
  public func list_permitted(self : T.StableStore, { permission } : T.ListPermitted) : [Principal] {
    Permissions.get_permission_list(self.permissions, permission);
  };

  /// Takes ownership of the assets library.
  public func take_ownership(self : T.StableStore, caller : Principal) : async* Result<(), Text> {
    switch (await* Permissions.is_controller(self.permissions, self.canister_id, caller)) {
      case (#ok(_)) {};
      case (#err(msg)) return #err(msg);
    };

    Permissions.clear_all(self.permissions);
    Permissions.grant_permission(self.permissions, caller, #Commit);

    #ok;
  };

  /// Gets the configuration of the assets library.
  public func get_configuration(self : T.StableStore, caller : Principal) : Result<T.ConfigurationResponse, Text> {
    switch (Permissions.can_prepare(self.permissions, caller)) {
      case (#ok(_)) {};
      case (#err(msg)) return #err(msg);
    };

    let config : T.ConfigurationResponse = {
      max_batches = self.upload.configuration.max_batches;
      max_chunks = self.upload.configuration.max_chunks;
      max_bytes = self.upload.configuration.max_bytes;
    };

    #ok(config);
  };

  /// Recertifies an asset in the assets library.
  public func recertify(self : T.StableStore, caller : Principal, key : Text) : T.Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) {};
      case (#err(msg)) return #err(msg);
    };

    switch (AssetUtils.exists(self, Utils.format_key(key))) {
      case (false) return #err("Asset does not exist");
      case (true) {};
    };

    AssetUtils.recertify(self, Utils.format_key(key));

    #ok();
  };

  /// Configures the assets library.
  public func configure(self : T.StableStore, caller : Principal, args : T.ConfigureArguments) : Result<(), Text> {
    switch (Permissions.can_commit(self.permissions, caller)) {
      case (#ok(_)) {};
      case (#err(msg)) return #err(msg);
    };

    Upload.set_max_batches(self.upload, args.max_batches);
    Upload.set_max_chunks(self.upload, args.max_chunks);
    Upload.set_max_bytes(self.upload, args.max_bytes);

    #ok();
  };

  /// Validates the grant permission arguments.
  public func validate_grant_permission(self : T.StableStore, args : T.GrantPermission) : Result<Text, Text> {
    #ok(
      "grant " # debug_show args.permission # " permission to principal " # debug_show args.to_principal
    );
  };

  /// Validates the revoke permission arguments.
  public func validate_revoke_permission(self : T.StableStore, args : T.RevokePermission) : Result<Text, Text> {
    #ok("revoke " # debug_show args.permission # " permission from principal " # debug_show args.of_principal);
  };

  /// Validates the take ownership arguments.
  public func validate_take_ownership() : Result<Text, Text> {
    #ok("revoke all permissions, then gives the caller Commit permissions");
  };

  /// Validates the commit proposed batch arguments.
  public func validate_commit_proposed_batch(self : T.StableStore, args : T.CommitProposedBatchArguments) : Result<Text, Text> {
    #ok("commit proposed batch " # debug_show args.batch_id # "with evidence " # debug_show args.evidence);
  };

  /// Validates the configure arguments.
  public func validate_configure(self : T.StableStore, args : T.ConfigureArguments) : Result<Text, Text> {
    #ok("configure: " # debug_show args);
  };

  /// Handles HTTP requests.
  public func http_request(self : T.StableStore, req : T.HttpRequest) : Result<T.HttpResponse, Text> {
    Http.process_http_request(self, req);
  };
};
