import Order "mo:base@0/Order";
import Result "mo:base@0/Result";
import Time "mo:base@0/Time";
import Nat64 "mo:base@0/Nat64";
import Iter "mo:base@0/Iter";
import Buffer "mo:base@0/Buffer";

import Set "mo:map@9/Set";
import Map "mo:map@9/Map";
import CertifiedAssets "mo:certified-assets@0/Stable";
import SHA256 "mo:sha2@0/Sha256";
import Vector "mo:vector@0";
import MemoryRegion "mo:memory-region@1/MemoryRegion";
import HttpParser "mo:http-parser@0";

module T {

  public type Map<K, V> = Map.Map<K, V>;
  public type Iter<T> = Iter.Iter<T>;
  public type Set<V> = Set.Set<V>;
  public let { thash; bhash; nhash; phash; ihash } = Map;

  public type Result<T, E> = Result.Result<T, E>;
  public type Time = Time.Time;
  public type Order = Order.Order;
  public type Buffer<V> = Buffer.Buffer<V>;
  public type MemoryRegion = MemoryRegion.MemoryRegion;

  public type EndpointRecord = CertifiedAssets.EndpointRecord;
  public type Endpoint = CertifiedAssets.Endpoint;
  public type CertifiedAssets = CertifiedAssets.StableStore;

  public type URL = HttpParser.URL;
  public type Headers = HttpParser.Headers;

  public type Configuration = {
    var max_batches : ?Nat64;
    var max_chunks : ?Nat64;
    var max_bytes : ?Nat64;
  };

  public type AssetEncoding = {
    var modified : Time;
    var content_pointer : SizedPointer;

    var total_length : Nat;
    var certified : Bool;
    var sha256 : Blob;
  };

  public type Asset = {
    var content_type : Text;
    encodings : Map<Text, AssetEncoding>;
    headers : Map<Text, Text>;
    var is_aliased : ?Bool;
    var max_age : ?Nat64;
    var allow_raw_access : ?Bool;
    var last_certified_encoding : ?Text;
  };

  public type BatchOperation = {
    #CreateAssetArguments;
    #SetAssetContentArguments;
    #UnsetAssetContentArguments;
    #DeleteAssetArguments;
    #ClearArguments;
    #SetAssetPropertiesArguments;
  };

  public type NextOperation = {
    operation_index : Nat;
    hasher_state : SHA256.StaticSha256;
  };

  public type NextChunkIndex = {
    operation_index : Nat;
    chunk_index : Nat;
    hasher_state : SHA256.StaticSha256;
  };

  public type EvidenceComputation = {
    #NextOperation : NextOperation;
    #NextChunkIndex : NextChunkIndex;
    #Computed : Blob;
  };
  public type Batch = {
    var expires_at : Time;
    var commit_batch_arguments : ?CommitBatchArguments;
    var evidence_computation : ?EvidenceComputation;
    var total_bytes : Nat;
    chunk_ids : Vector.Vector<ChunkId>;
  };

  public type Key = Text;
  public type Path = Text;
  public type BatchId = Nat;
  public type ChunkId = Nat;

  public type SizedPointer = (Nat, Nat);

  public type HierarchicalAssets = {
    #Asset : Asset;
    #Directory : Map<Text, HierarchicalAssets>;
  };

  public type Directory = Map<Text, HierarchicalAssets>;

  public type DirectoryContent = {
    name : Text;
    is_directory : Bool;
  };

  public type Upload = {
    batches : T.Map<T.BatchId, T.Batch>;
    var next_batch_id : T.BatchId;

    chunks : T.Map<T.ChunkId, T.StoredChunk>; // replaced by tmp_chunks associated with a batch
    region : T.MemoryRegion;
    var next_chunk_id : T.ChunkId;

    configuration : Configuration;
  };

  public type Permissions = {
    commit_principals : T.Set<Principal>;
    prepare_principals : T.Set<Principal>;
    manage_permissions_principals : T.Set<Principal>;
  };

  public type FileSystem = {
    root : Directory;
    region : T.MemoryRegion;
    certs : T.CertifiedAssets;
  };

  public type StableStore = {

    var canister_id : Principal;
    var streaming_callback : ?StreamingCallback;

    /// region shared with FileSystem and Upload modules
    shared_region : MemoryRegion;

    fs : FileSystem;
    upload : Upload;
    permissions : Permissions;

  };

  public type CreateAssetArguments = {
    key : Key;
    content_type : Text;
    max_age : ?Nat64;
    headers : ?[Header];
    enable_aliasing : ?Bool;
    allow_raw_access : ?Bool;
  };

  /// Add or change content for an asset, by content encoding
  public type SetAssetContentArguments = {
    key : Key;
    sha256 : ?Blob;
    chunk_ids : [ChunkId];
    content_encoding : Text;
  };

  /// Remove content for an asset, by content encoding
  public type UnsetAssetContentArguments = {
    key : Key;
    content_encoding : Text;
  };

  /// Delete an asset
  public type DeleteAssetArguments = { key : Key };

  /// Reset everything
  public type ClearArguments = {};

  public type SetAssetPropertiesArguments = {
    key : Key;
    max_age : ??Nat64;
    headers : ??[Header];
    allow_raw_access : ??Bool;
    is_aliased : ??Bool;
  };

  public type BatchOperationKind = {
    #CreateAsset : CreateAssetArguments;
    #SetAssetContent : SetAssetContentArguments;
    #SetAssetProperties : SetAssetPropertiesArguments;
    #UnsetAssetContent : UnsetAssetContentArguments;
    #DeleteAsset : DeleteAssetArguments;
    #Clear : ClearArguments;
  };

  public type AssetDetails = {
    key : Key;
    encodings : [AssetEncodingDetails];
    content_type : Text;
  };

  public type AssetEncodingDetails = {
    modified : Time;
    content_encoding : Text;
    /// sha256 of entire asset encoding, calculated by dfx and passed in SetAssetContentArguments
    sha256 : ?Blob;
    /// Size of this encoding's blob. Calculated when uploading assets.
    length : Nat;
  };

  public type CommitBatchArguments = {
    batch_id : BatchId;
    operations : [BatchOperationKind];
  };

  public type CommitProposedBatchArguments = {
    batch_id : BatchId;
    evidence : Blob;
  };

  public type ComputeEvidenceArguments = {
    batch_id : BatchId;
    max_iterations : ?Nat16;
  };

  public type DeleteBatchArguments = {
    batch_id : BatchId;
  };

  public type StreamingCallbackToken = {
    key : Key;
    content_encoding : Text;
    index : Nat;
    sha256 : ?Blob;
  };

  public type ConfigurationResponse = {
    max_batches : ?Nat64;
    max_chunks : ?Nat64;
    max_bytes : ?Nat64;
  };

  public type ConfigureArguments = {
    max_batches : ??Nat64;
    max_chunks : ??Nat64;
    max_bytes : ??Nat64;
  };

  public type Permission = {
    #Commit;
    #Manage;
    #Prepare;
  };

  public type GrantPermission = {
    to_principal : Principal;
    permission : Permission;
  };

  public type RevokePermission = {
    of_principal : Principal;
    permission : Permission;
  };

  public type ListPermitted = { permission : Permission };

  public type CanisterArgs = {
    #Init : InitArgs;
    #Upgrade : UpgradeArgs;
  };

  public type InitArgs = {};

  public type UpgradeArgs = {
    set_permissions : ?SetPermissions;
  };

  /// Sets the list of principals granted each permission.
  public type SetPermissions = {
    prepare : [Principal];
    commit : [Principal];
    manage_permissions : [Principal];
  };

  public type GetArgs = {
    key : Key;
    accept_encodings : [Text];
  };

  public type EncodedAsset = {
    content_type : Text;
    content_encoding : Text;

    /// may be the entirety of the content, or just chunk index 0
    content : Blob;

    /// sha256 of entire asset encoding, calculated by dfx and passed in SetAssetContentArguments
    sha256 : ?Blob;

    /// all chunks except last have size == content.size()
    total_length : Nat;
  };

  /// The `sha256` field is the hash of the entire asset encoding, calculated by dfx and passed in SetAssetContentArguments
  public type GetChunkArgs = StreamingCallbackToken;

  public type ChunkContent = {
    content : Blob;
  };

  public type Chunk = ChunkContent and {
    batch_id : BatchId;
  };

  public type StoredChunk = {
    pointer : SizedPointer;
    batch_id : BatchId;
  };

  public type Chunks = {
    content : [Blob];
    batch_id : BatchId;
  };

  public type ListArgs = {};

  public type CertifiedTree = {
    certificate : Blob;
    tree : Blob;
  };

  public type AssetProperties = {
    max_age : ?Nat64;
    headers : ?[Header];
    allow_raw_access : ?Bool;
    is_aliased : ?Bool;
  };

  public type StoreArgs = {
    key : Key;
    content : Blob;
    sha256 : ?Blob;
    content_type : Text;
    content_encoding : Text;
    is_aliased : ?Bool;
  };

  public type ValidationResult = Result<Text, Text>;

  public type Contents = Blob;

  public type Header = (Text, Text);

  public type HttpResponse = {
    status_code : Nat16;
    headers : [Header];
    body : Blob;
    streaming_strategy : ?StreamingStrategy;
    upgrade : ?Bool;
  };

  public type HttpRequest = {
    url : Text;
    method : Text;
    headers : [Header];
    body : Blob;
    certificate_version : ?Nat16;
  };

  public type StreamingStrategy = {
    #Callback : {
      callback : StreamingCallback;
      token : StreamingToken;
    };
  };

  public type StreamingCallback = shared query (StreamingToken) -> async StreamingCallbackResponse;
  public type StreamingToken = {
    key : Key;
    sha256 : ?Blob;
    content_encoding : Text;
    index : Nat;
  };
  public type CustomStreamingToken = StreamingToken;
  public type StreamingCallbackResponse = {
    body : Blob;
    token : ?StreamingToken;
  };

  public type StreamingCallbackResponseAny = {
    body : Blob;
    token : ?Any;
  };

  public type CreateBatchResponse = {
    batch_id : BatchId;
  };

  public type CreateChunkArguments = Chunk;

  public type CreateChunksArguments = {
    batch_id : BatchId;
    content : [Blob];
  };

  public type CreateChunkResponse = {
    chunk_id : Nat;
  };

  public type CreateChunksResponse = {
    chunk_ids : [ChunkId];
  };

  public type CreateBatchArguments = {};

  public type AssetsInterface = actor {
    // init : shared () -> async ();
    api_version : shared query () -> async (Nat16);

    get : shared query (GetArgs) -> async (EncodedAsset);

    /// if get() returned chunks > 1, call this to retrieve them.
    /// chunks may or may not be split up at the same boundaries as presented to create_chunk().
    get_chunk : shared query (StreamingCallbackToken) -> async ChunkContent;

    list : shared query ({}) -> async ([AssetDetails]);

    clear : shared (ClearArguments) -> async ();

    create_asset : shared (CreateAssetArguments) -> async ();
    set_asset_content : shared (SetAssetContentArguments) -> async ();
    unset_asset_content : shared (UnsetAssetContentArguments) -> async ();
    delete_asset : shared (DeleteAssetArguments) -> async ();

    get_asset_properties : shared query (key : Key) -> async (AssetProperties);
    set_asset_properties : (SetAssetPropertiesArguments) -> async ();

    /// Single call to create an asset with content for a single content encoding that
    /// fits within the message ingress limit.
    store : shared (StoreArgs) -> async ();

    // certified_tree : shared query ({}) -> async (CertifiedTree);
    create_batch : shared ({}) -> async (CreateBatchResponse);
    create_chunk : shared (CreateChunkArguments) -> async (CreateChunkResponse);
    create_chunks : shared (CreateChunksArguments) -> async (CreateChunksResponse);

    /// Perform all operations successfully, or reject the entire batch.
    commit_batch : shared CommitBatchArguments -> async ();

    /// Save the batch operations for later commit
    propose_commit_batch : shared (CommitBatchArguments) -> async ();

    /// Given a batch already proposed, perform all operations successfully, or reject
    commit_proposed_batch : shared (CommitProposedBatchArguments) -> async ();

    /// Delete a batch that has been created, or proposed for commit, but not yet committed
    delete_batch : shared (DeleteBatchArguments) -> async ();

    authorize : shared (Principal) -> async ();
    deauthorize : shared (Principal) -> async ();
    list_authorized : shared () -> async ([Principal]);
    grant_permission : shared (GrantPermission) -> async ();
    revoke_permission : shared (RevokePermission) -> async ();
    list_permitted : shared (ListPermitted) -> async ([Principal]);
    take_ownership : shared () -> async ();

    get_configuration : () -> async (ConfigurationResponse);
    configure : (ConfigureArguments) -> async ();

    validate_grant_permission : (GrantPermission) -> async (Result<Text, Text>);
    validate_revoke_permission : (RevokePermission) -> async (Result<Text, Text>);
    validate_take_ownership : () -> async (Result<Text, Text>);
    validate_commit_proposed_batch : (CommitProposedBatchArguments) -> async (Result<Text, Text>);
    validate_configure : (ConfigureArguments) -> async (Result<Text, Text>);

    // /// Compute a hash over the CommitBatchArguments.  Call until it returns Some(evidence).
    compute_evidence : shared (ComputeEvidenceArguments) -> async (?Blob);

    http_request : (HttpRequest) -> async (HttpResponse);
    http_request_streaming_callback : StreamingCallback;

  };

  public type Service = (args : CanisterArgs) -> async AssetsInterface;
};
