import Debug "mo:base/Debug";
import Result "mo:base/Result";
import Array "mo:base/Array";
import Text "mo:base/Text";
import Time "mo:base/Time";
import Iter "mo:base/Iter";

import { URL; Headers } "mo:http-parser";
import Map "mo:map/Map";
import CertifiedAssets "mo:certified-assets/Stable";

import T "Types";
import BaseAssets "BaseAssets";
import Migrations "Migrations";
import AssetUtils "AssetUtils";

module {
    let { thash } = Map;
    type Result<T, E> = Result.Result<T, E>;

    public type Key = T.Key;
    public type Path = T.Path;
    public type BatchId = T.BatchId;
    public type ChunkId = T.ChunkId;
    public type Time = Time.Time;

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
    public type CreateChunksArguments = T.CreateChunksArguments;
    public type CreateChunksResponse = T.CreateChunksResponse;
    public type EndpointRecord = T.EndpointRecord;

    public type VersionedStableStore = T.VersionedStableStore;
    type StableStore = T.StableStore;

    public let MAX_CHUNK_SIZE = BaseAssets.MAX_CHUNK_SIZE;

    public func from_version(versions : T.VersionedStableStore) : StableStore {
        let upgraded_store = Migrations.upgrade(versions);
        Migrations.get_current_state(upgraded_store);
    };

    public func share_version(self : StableStore) : T.VersionedStableStore {
        Migrations.share_version(self);
    };

    public func init_stable_store(canister_id : Principal, owner : Principal) : VersionedStableStore {
        let stable_store_v0 = BaseAssets.init_stable_store(canister_id, owner);
        share_version(stable_store_v0);
    };

    public func upgrade(asset_versions : VersionedStableStore) : VersionedStableStore {
        Migrations.upgrade(asset_versions);
    };

    public func div_ceiling(a : Nat, b : Nat) : Nat {
        (a + (b - 1)) / b;
    };

    public func num_of_chunks(bytes : Nat) : Nat {
        div_ceiling(bytes, MAX_CHUNK_SIZE);
    };

    //! todo: implement
    public func split_into_chunks(blob : Blob) : [Blob] { [] };

    public func num_chunks(total_length : Nat) : Nat {
        div_ceiling(total_length, MAX_CHUNK_SIZE);
    };

    public func redirect_to(assets : Assets, prev_http_request : T.HttpRequest, path : Text, headers : [(Text, Text)]) : T.HttpResponse {
        let canister_id = assets.get_canister_id();

        let url = URL(prev_http_request.url, Headers(prev_http_request.headers));

        let new_location = url.protocol # "://" # url.host.original # ":" # Text.replace(debug_show url.port, #text("_"), "") # path # "?" # url.queryObj.original;
        Debug.print("Redirecting to: " # new_location);

        {
            status_code = 308; // Permanent Redirect
            headers = Array.append([("Location", new_location)], headers);
            body = "";
            upgrade = null;
            streaming_strategy = null;
        };

    };

    public func hash_chunks(chunks : [Blob]) : async* Blob {
        await* AssetUtils.hash_blob_chunks(chunks, null);
    };

    public class Assets(sstore : VersionedStableStore) {
        let state = Migrations.get_current_state(sstore);

        public func api_version() : Nat16 = BaseAssets.api_version();

        public func set_canister_id(id : Principal) : () {
            BaseAssets.set_canister_id(state, id);
        };

        public func get_canister_id() : Principal {
            BaseAssets.get_canister_id(state);
        };

        public func exists(key : T.Key) : Bool {
            BaseAssets.exists(state, key);
        };

        /// This method is needed to support streaming large assets from the canister.
        /// To use this method, you must create a public update function in your canister that calls this method.
        /// After the update function is created, you have to store the function in the asset library by calling [set_streaming_callback()](#method-set_streaming_callback).
        ///
        /// #### Example
        ///
        /// ```motoko
        ///
        /// public func ic_assets_streaming_callback(token : Assets.StreamingToken) : Assets.StreamingCallbackResponse {
        ///     assets.http_request_streaming_callback(token);
        /// };
        ///
        /// assets.set_streaming_callback(ic_assets_streaming_callback);
        ///
        public func http_request_streaming_callback(token : StreamingToken) : StreamingCallbackResponse {
            BaseAssets.http_request_streaming_callback(state, token);
        };

        /// This method points to the streaming callback function defined in the canister.
        public func set_streaming_callback(callback : StreamingCallback) {
            BaseAssets.set_streaming_callback(state, callback);
        };

        /// Returns the certified tree for all the assets.
        ///
        /// **Note**: This method can only be executed in a query call. It will return an error if executed in an update call.
        public func certified_tree() : Result<T.CertifiedTree, Text> {
            BaseAssets.certified_tree(state);
        };

        /// This method looks up the asset with the given key, using [aliasing](#aliasing) rules if the key is not found.
        ///
        /// Then, it searches the asset's [content encodings](#content-encodings) in the order specified in `accept_encodings`.  If none are found, it returns an error.  A typical value for `accept_encodings` would be `["gzip", "identity"]`.
        ///
        /// Finally, it returns the first chunk of the content encoding.
        ///
        /// If `total_length` exceeds the length of the returned `content` blob, this means that there is more than one chunk.  The caller can then call [get_chunk()](#method-get_chunk) to retrieve the remaining chunks.  Note that since all chunks except the last have the same length as the first chunk, the caller can determine the number of chunks by dividing `total_length` by the length of the first chunk.
        ///
        /// The `sha256` field is `opt` only because it was added after the initial release of the asset canister.  It will always be present in the response.
        ///
        public func get(args : T.GetArgs) : Result<T.EncodedAsset, Text> {
            BaseAssets.get(state, args);
        };

        /// This method looks up the asset with the given key, using [aliasing](#aliasing) rules if the key is not found.
        ///
        /// Then, it searches the asset's [content encodings](#content-encodings) in the order specified in `accept_encodings`.  If none are found, it returns an error.  A typical value for `accept_encodings` would be `["gzip", "identity"]`.
        ///
        /// Finally, it returns the first chunk of the content encoding.
        ///
        /// If `total_length` exceeds the length of the returned `content` blob, this means that there is more than one chunk.  The caller can then call [get_chunk()](#method-get_chunk) to retrieve the remaining chunks.  Note that since all chunks except the last have the same length as the first chunk, the caller can determine the number of chunks by dividing `total_length` by the length of the first chunk.
        ///
        /// The `sha256` field is `opt` only because it was added after the initial release of the asset canister.  It must always be present in the response.
        public func get_chunk(args : T.GetChunkArgs) : Result<T.ChunkContent, Text> {
            BaseAssets.get_chunk(state, args);
        };

        /// This method returns a list of all assets.

        /// > The `sha256` field is `opt` only because it was added after the initial release of the asset canister.  It must always be present in the response.
        public func list(args : {}) : [T.AssetDetails] {
            BaseAssets.list(state, args);
        };

        /// Create an asset with data for a single content encoding that fits within the message ingress limit (2MB).
        ///
        /// For larger assets, create a batch request with the #CreateAsset and #SetAssetContent operations.
        ///
        /// Required Commit: [Commit](#permission-commit)
        public func store(caller : Principal, args : StoreArgs) : Result<(), Text> {
            BaseAssets.store(state, caller, args);
        };

        /// This method creates a new asset. It cannot create an asset with the same key as an existing asset.
        ///
        /// Required Permission: [Commit](#permission-commit)
        public func create_asset(caller : Principal, args : CreateAssetArguments) : Result<(), Text> {
            BaseAssets.create_asset(state, caller, args);
        };

        /// This method overwrites a single content encoding for an asset to the provided chunks. It also updates the modification time of the content encoding.
        ///
        /// If `sha256` is not passed, the asset canister will compute the hash of the content.
        ///
        public func set_asset_content(caller : Principal, args : SetAssetContentArguments) : async* Result<(), Text> {
            await* BaseAssets.set_asset_content(state, caller, args);
        };

        /// This method removes a single content encoding for an asset.
        public func unset_asset_content(caller : Principal, args : T.UnsetAssetContentArguments) : Result<(), Text> {
            BaseAssets.unset_asset_content(state, caller, args);
        };

        /// This method deletes a single asset.
        public func delete_asset(caller : Principal, args : DeleteAssetArguments) : Result<(), Text> {
            BaseAssets.delete_asset(state, caller, args);
        };

        /// This method returns the properties of the asset with the given key.
        public func get_asset_properties(key : T.Key) : Result<T.AssetProperties, Text> {
            BaseAssets.get_asset_properties(state, key);
        };

        /// This operation sets some or all properties of an asset.
        ///
        /// Callable by:
        public func set_asset_properties(caller : Principal, args : SetAssetPropertiesArguments) : Result<(), Text> {
            BaseAssets.set_asset_properties(state, caller, args);
        };

        /// This operation deletes all assets.
        ///
        /// This functionality is also available as an operation via #Clear in the `commit_batch` method.
        ///
        public func clear(caller : Principal, args : ClearArguments) : Result<(), Text> {
            BaseAssets.clear(state, caller, args);
        };

        /// This method creates a new [batch](#batch) and returns its ID.
        ///
        /// The batch is only created if the following conditions are met:
        ///
        /// - No batch exists for which [propose_commit_batch()](#method-propose_commit_batch) has been called.
        /// - Creation of a new batch would not exceed batch creation limits.
        ///
        /// > **Required Permission**: [#Prepare](#permission-prepare)
        public func create_batch(caller : Principal, args : {}) : Result<CreateBatchResponse, Text> {
            BaseAssets.create_batch(state, caller, args);
        };

        /// This method stores a content chunk and extends the batch expiry time.
        ///
        /// When creating chunks for a given content encoding, the size of each chunk except the last must be the same.
        ///
        /// The asset canister must retain all data related to a batch for at least the [Minimum Batch Retention Duration](#constant-minimum-batch-retention-duration) after creating a chunk in a batch.
        ///
        /// Preconditions:
        ///
        /// - The batch exists.
        /// - Creation of the chunk would not exceed chunk creation limits.
        ///
        /// Required Permission: [Prepare](#permission-prepare)
        /// todo - indicate somewhere that the chunk ids have to be listed in order when calling set_asset_content or using the #SetAssetContent operation

        public func create_chunk(caller : Principal, args : T.Chunk) : Result<T.CreateChunkResponse, Text> {
            BaseAssets.create_chunk(state, caller, args);
        };

        /// This method stores a number of chunks and extends the batch expiry time.
        ///
        /// When creating chunks for a given content encoding, the size of each chunk except the last must be the same.
        ///
        /// The asset canister must retain all data related to a batch for at least the [Minimum Batch Retention Duration](#constant-minimum-batch-retention-duration) after creating a chunk in a batch.
        ///
        /// Preconditions:
        ///
        /// - The batch exists.
        /// - Creation of the chunk would not exceed chunk creation limits.
        ///
        /// Required Permission: [Prepare](#permission-prepare)

        public func create_chunks(caller : Principal, args : T.CreateChunksArguments) : async* Result<T.CreateChunksResponse, Text> {
            await* BaseAssets.create_chunks(state, caller, args);
        };

        ///
        /// The `commit_batch` method executes the specified batch operations in the order listed. The method traps if there is an error executing any operation, so either all or none of the operations will be applied.
        ///
        ///! need to re-implement so the above statement is true
        ///! - can implement it in two ways:
        ///!   - 1. execute a dry run, only proceed if all operations are successful
        ///!   - 2. execute all operations in parallel, but if one fails, revert all operations
        /// After executing the operations, this method deletes the batch associated with `batch_id`. It is valid to pass `0` for batch_id, in which case this method does not delete any batch. This allows multiple calls to `commit_batch` to execute operations from a large batch, such that no call to `commit_batch` exceeds per-call computation limits. The final call to `commit_batch` should include the batch ID, in order to delete the batch.
        /// todo - replace table with a list as it doesn't render in mops
        /// | Operation                                           | Description                           |
        /// | --------------------------------------------------- | ------------------------------------- |
        /// | [CreateAsset](#operation-createasset)               | Creates a new asset.                  |
        /// | [SetAssetContent](#operation-setassetcontent)       | Adds or changes content for an asset. |
        /// | [SetAssetProperties](#operation-setassetproperties) | Changes properties for an asset.      |
        /// | [UnsetAssetContent](#operation-unsetassetcontent)   | Removes content for an asset.         |
        /// | [DeleteAsset](#operation-deleteasset)               | Deletes an asset.                     |
        /// | [Clear](#operation-clear)                           | Deletes all assets.                   |
        ///
        /// Required Permission: [Commit](#permission-commit)
        public func commit_batch(caller : Principal, args : CommitBatchArguments) : async* Result<(), Text> {
            await* BaseAssets.commit_batch(state, caller, args);
        };

        /// This method takes the same arguments as `commit_batch`, but does not execute the operations. Instead, it stores the operations in a "proposed batch" for later execution by the `commit_proposed_batch` method.

        /// Required permission: [Prepare](#permission-prepare)
        public func propose_commit_batch(caller : Principal, args : CommitBatchArguments) : Result<(), Text> {
            BaseAssets.propose_commit_batch(state, caller, args);
        };

        /// The `compute_evidence` method computes a hash over the proposed commit batch arguments.
        /// todo - replace inline doc to reflect the new implementation that uses multiple parralel calls to compute the hash
        ///
        /// Since calculation of this hash may exceed per-message computation limits, this method computes the hash iteratively, saving its work as it goes. Once it completes the computation, it saves the hash as `evidence` to be checked later.
        ///
        /// The method will return `None` if the hash computation has not yet completed, or `Some(evidence)` if the hash computation has been completed.
        ///
        /// The returned `evidence` value must be passed to the `commit_proposed_batch` method.
        ///
        /// After the hash computation has completed, the batch will no longer expire. The batch will remain until one of the following occurs:
        ///
        /// - a call to [commit_proposed_batch()]
        /// - a call to [delete_batch()]
        /// - the canister is upgraded
        ///
        /// Required permission: [Prepare](#permission-prepare)

        public func commit_proposed_batch(caller : Principal, args : CommitProposedBatchArguments) : async* Result<(), Text> {
            await* BaseAssets.commit_proposed_batch(state, caller, args);
        };

        public func compute_evidence(caller : Principal, args : ComputeEvidenceArguments) : async* Result<(?Blob), Text> {
            await* BaseAssets.compute_evidence(state, caller, args);
        };

        /// The `delete_batch` method deletes a single batch and any related chunks.
        ///
        /// Required Permission: [Prepare](#permission-prepare)
        public func delete_batch(caller : Principal, args : DeleteBatchArguments) : Result<(), Text> {
            BaseAssets.delete_batch(state, caller, args);
        };

        /// @deprecated Use [grant_permission()](#method-grant_permission) instead.
        public func authorize(caller : Principal, principal : Principal) : async* Result<(), Text> {
            await* BaseAssets.authorize(state, caller, principal);
        };

        /// @deprecated Use [revoke_permission()](#method-revoke_permission) instead.
        public func deauthorize(caller : Principal, principal : Principal) : async* Result<(), Text> {
            await* BaseAssets.deauthorize(state, caller, principal);
        };

        /// @deprecated Use [list_permitted()](#method-list_permitted) instead.
        public func list_authorized() : [Principal] {
            BaseAssets.list_authorized(state);
        };

        /// This method grants a permission to a principal.
        ///
        /// Callable by: Principals with [ManagePermissions](#permission-managepermissions) permission, and canister controllers.
        public func grant_permission(caller : Principal, args : T.GrantPermission) : async* Result<(), Text> {
            await* BaseAssets.grant_permission(state, caller, args);
        };

        /// This method revokes a permission from a principal.
        ///
        /// Callable by: Principals with [ManagePermissions](#permission-managepermissions) permission, and canister controllers. Also, any principal can revoke any of its own permissions.
        public func revoke_permission(caller : Principal, args : RevokePermission) : async* Result<(), Text> {
            await* BaseAssets.revoke_permission(state, caller, args);
        };

        /// This method returns a list of principals that have the given permission.
        ///
        /// Callable by any principal.
        public func list_permitted(permitted : ListPermitted) : [Principal] {
            BaseAssets.list_permitted(state, permitted);
        };

        /// Revokes the permissions for every other principal and grants the caller the [Commit](#permission-commit) permission.
        ///
        /// Callable by: Canister controllers.
        public func take_ownership(caller : Principal) : async* Result<(), Text> {
            await* BaseAssets.take_ownership(state, caller);
        };

        /// This method returns the current configuration of the asset canister.
        public func get_configuration(caller : Principal) : Result<T.ConfigurationResponse, Text> {
            BaseAssets.get_configuration(state, caller);
        };

        public func recertify(caller : Principal, key : Text) : Result<(), Text> {
            BaseAssets.recertify(state, caller, key);
        };

        /// This method configures the `max_batches`, `max_chunks` and `max_bytes` limits for the assets.
        ///
        /// Callable by: Principals with [Commit](#permission-commit) permissions.
        public func configure(caller : Principal, args : T.ConfigureArguments) : Result<(), Text> {
            BaseAssets.configure(state, caller, args);
        };

        public func validate_grant_permission(args : GrantPermission) : Result<Text, Text> {
            BaseAssets.validate_grant_permission(state, args);
        };

        public func validate_revoke_permission(args : RevokePermission) : Result<Text, Text> {
            BaseAssets.validate_revoke_permission(state, args);
        };

        public func validate_take_ownership() : Result<Text, Text> {
            BaseAssets.validate_take_ownership();
        };

        public func validate_commit_proposed_batch(args : CommitProposedBatchArguments) : Result<Text, Text> {
            BaseAssets.validate_commit_proposed_batch(state, args);
        };

        public func validate_configure(args : T.ConfigureArguments) : Result<Text, Text> {
            BaseAssets.validate_configure(state, args);
        };

        public func http_request(req : T.HttpRequest) : Result<T.HttpResponse, Text> {
            BaseAssets.http_request(state, req);
        };

        public func get_certified_endpoints() : [T.EndpointRecord] {
            Iter.toArray(CertifiedAssets.endpoints(state.certificate_store));
        };

    };

};
