import Array "mo:base@0/Array";
import Option "mo:base@0/Option";
import Principal "mo:base@0/Principal";
import Result "mo:base@0/Result";
import Iter "mo:base@0/Iter";
import Blob "mo:base@0/Blob";
import Time "mo:base@0/Time";
import Nat64 "mo:base@0/Nat64";
import Nat16 "mo:base@0/Nat16";
import Order "mo:base@0/Order";
import Text "mo:base@0/Text";
import Debug "mo:base@0/Debug";
import Error "mo:base@0/Error";
import Buffer "mo:base@0/Buffer";
import Nat "mo:base@0/Nat";

import Set "mo:map@9/Set";
import Map "mo:map@9/Map";
import IC "mo:ic@3";
import Sha256 "mo:sha2@0/Sha256";
import MemoryRegion "mo:memory-region@1/MemoryRegion";
import Itertools "mo:itertools@0/Iter";

import Utils "Utils";
import T "Types";
import FileSystem "FileSystem/lib";
import ErrorMessages "ErrorMessages";
import Certs "FileSystem/Certs";
import Encoding "FileSystem/Encoding";
import Asset "FileSystem/Asset";
import Upload "Upload/lib";
import Const "Const";

module {

  type Map<K, V> = Map.Map<K, V>;
  type Set<V> = Set.Set<V>;
  type Buffer<V> = Buffer.Buffer<V>;
  type Result<T, E> = Result.Result<T, E>;
  type Time = Time.Time;
  type Asset = T.Asset;
  type AssetEncoding = T.AssetEncoding;
  type Key = T.Key;
  type StableStore = T.StableStore;

  let { ic } = IC;
  let { thash; nhash } = Map;

  public func store(self : T.StableStore, args : T.StoreArgs) : T.Result<(), Text> {
    let key = Utils.format_key(args.key);
    let hash = Sha256.fromBlob(#sha256, args.content);

    switch (args.sha256) {
      case (?provided_hash) {
        if (hash != provided_hash) {
          return #err(ErrorMessages.sha256_hash_mismatch(provided_hash, hash));
        };
      };
      case (_) ();
    };

    let asset = switch (FileSystem.get_asset_using_aliases(self.fs, key, false)) {
      case (#ok(?asset)) asset;
      case (#ok(null)) switch (FileSystem.create_asset(self.fs, key)) {
        case (#ok(new_asset)) new_asset;
        case (#err(msg)) return #err(msg);
      };
      case (#err(msg)) return #err(msg);
    };

    Certs.remove_asset_certificates(self.fs, key, asset, false);

    let encoding = Utils.map_get_or_put(asset.encodings, thash, args.content_encoding, func() : T.AssetEncoding = Encoding.new());

    Encoding.replace_content(self.fs, encoding, args.content, hash);
    encoding.certified := false;
    asset.content_type := args.content_type;
    asset.is_aliased := args.is_aliased;

    Certs.certify_asset(self.fs, key, asset, null);

    #ok();
  };

  public func exists(self : T.StableStore, key : T.Key) : Bool {
    let #ok(?(_)) = FileSystem.get_asset_using_aliases(self.fs, key, false) else return false;
    true;
  };

  public func get(self : T.StableStore, args : T.GetArgs) : T.Result<T.EncodedAsset, Text> {
    let key = Utils.format_key(args.key);

    let asset = switch (FileSystem.get_asset_using_aliases(self.fs, key, true)) {
      case (#ok(?asset)) asset;
      case (#ok(null)) return #err(ErrorMessages.asset_not_found(key));
      case (#err(msg)) return #err(msg);
    };

    label for_loop for (encoding_key in args.accept_encodings.vals()) {
      let encoding = switch (Map.get(asset.encodings, thash, encoding_key)) {
        case (?encoding) encoding;
        case (_) continue for_loop;
      };

      return #ok({
        content_type = asset.content_type;
        content = switch (Encoding.get_chunk(self.fs, encoding, 0)) {
          case (null) "";
          case (?content) content;
        };
        content_encoding = encoding_key;
        total_length = encoding.total_length;
        sha256 = ?encoding.sha256;
      });
    };

    #err(
      "No matching encoding found for " # debug_show args.accept_encodings # " in " # key #
      ". Encodings available: " # debug_show Iter.toArray(Map.keys(asset.encodings))
    );
  };

  public func get_asset_properties(self : T.StableStore, _key : T.Key) : T.Result<T.AssetProperties, Text> {
    let key = Utils.format_key(_key);

    let asset = switch (get_asset_using_aliases(self, key, false)) {
      case (#ok(asset)) asset;
      case (#err(msg)) return #err(msg);
    };

    #ok(Asset.get_properties(asset));
  };

  public func list(self : T.StableStore, args : T.ListArgs) : [T.AssetDetails] {

    let assets = FileSystem.to_array(self.fs);

    Array.map<(T.Key, T.Asset), T.AssetDetails>(
      assets,
      func(key : T.Key, asset : T.Asset) : T.AssetDetails {
        Asset.get_details(key, asset);
      },
    );

  };

  public func get_asset_using_aliases(self : T.StableStore, key : Text, only_aliases : Bool) : T.Result<T.Asset, Text> {
    switch (FileSystem.get_asset_using_aliases(self.fs, key, only_aliases)) {
      case (#ok(?asset)) #ok(asset);
      case (#ok(null)) #err(ErrorMessages.asset_not_found(key));
      case (#err(msg)) #err(msg);
    };
  };

  public func get_encoding(self : T.StableStore, asset_key : Text, asset : T.Asset, encoding_name : Text) : T.Result<T.AssetEncoding, Text> {
    let ?encoding = Map.get(asset.encodings, thash, encoding_name) else return #err(ErrorMessages.encoding_not_found(asset_key, encoding_name));
    #ok(encoding);
  };

  public func get_chunk(self : T.StableStore, args : T.GetChunkArgs) : T.Result<T.ChunkContent, Text> {
    let key = Utils.format_key(args.key);
    let asset = switch (get_asset_using_aliases(self, key, true)) {
      case (#ok(asset)) asset;
      case (#err(msg)) return #err(msg);
    };

    let encoding = switch (get_encoding(self, key, asset, args.content_encoding)) {
      case (#ok(encoding)) encoding;
      case (#err(msg)) return #err(msg);
    };

    switch (args.sha256) {
      case (?provided_hash) {
        if (encoding.sha256 != provided_hash) {
          return #err(ErrorMessages.sha256_hash_mismatch(provided_hash, encoding.sha256));
        };
      };
      case (null) {};
    };

    let num_chunks = Encoding.get_chunks_size(encoding);
    if (args.index >= num_chunks) return #err("Chunk index out of bounds.");

    switch (Encoding.get_chunk(self.fs, encoding, args.index)) {
      case (?content) #ok({ content });
      case (null) #err(ErrorMessages.chunk_not_found(args.index));
    };

  };

  public func create_asset(self : T.StableStore, args : T.CreateAssetArguments) : T.Result<(), Text> {
    let key = Utils.format_key(args.key);

    switch (FileSystem.create_asset(self.fs, key)) {
      case (#ok(asset)) {
        asset.content_type := args.content_type;
        asset.is_aliased := args.enable_aliasing;
        asset.max_age := args.max_age;
        asset.allow_raw_access := args.allow_raw_access;

        switch (args.headers) {
          case (?headers) {
            for ((field, value) in headers.vals()) {
              ignore Map.put(asset.headers, thash, field, value);
            };
          };
          case (_) {};
        };

        #ok();
      };
      case (#err(msg)) return #err(msg);
    };
  };

  public func set_asset_content(self : T.StableStore, args : T.SetAssetContentArguments) : async* T.Result<(), Text> {
    let key = Utils.format_key(args.key);
    let asset = switch (get_asset_using_aliases(self, key, false)) {
      case (#ok(asset)) asset;
      case (#err(msg)) return #err(msg);
    };
    let encoding = Utils.map_get_or_put(asset.encodings, thash, args.content_encoding, func() : T.AssetEncoding = Encoding.new());

    var total_length = 0;

    var error_msg : ?Text = null;

    let chunk_pointers = Array.map<Nat, (Nat, Nat)>(
      args.chunk_ids,
      func(chunk_id : Nat) : (Nat, Nat) {
        let chunk_pointer = switch (Upload.get_chunk_pointer(self.upload, chunk_id)) {
          case (?pointer) pointer;
          case (null) {
            error_msg := ?("Chunk with id " # debug_show chunk_id # " not found.");
            (0, 0);
          };
        };

        total_length += chunk_pointer.1;

        chunk_pointer;
      },
    );

    switch (error_msg) {
      case (?msg) return #err(msg);
      case (null) {};
    };

    let hash = switch (await* async_hash_chunks_via_pointers(self, chunk_pointers)) {
      case (#ok(hash)) hash;
      case (#err(msg)) return #err("Failed to hash chunks: " # msg);
    };

    switch (args.sha256) {
      case (?provided_hash) if (hash != provided_hash) {
        return #err(ErrorMessages.sha256_hash_mismatch(provided_hash, hash));
      };
      case (_) {};
    };

    Certs.remove_encoding_certificate(self.fs, key, asset, args.content_encoding, encoding, false);
    Encoding.deallocate_content(self.fs, encoding);

    let chunks = Iter.map<(Nat, Nat), Blob>(
      chunk_pointers.vals(),
      func(address : Nat, size : Nat) : Blob {
        MemoryRegion.loadBlob(self.fs.region, address, size);
      },
    );

    Encoding.replace_content_via_chunks(self.fs, encoding, chunks, total_length, hash);
    Certs.certify_asset(self.fs, key, asset, ?args.content_encoding);

    #ok;
  };

  public func unset_asset_content(self : T.StableStore, args : T.UnsetAssetContentArguments) : T.Result<(), Text> {
    let key = Utils.format_key(args.key);

    let asset = switch (get_asset_using_aliases(self, key, false)) {
      case (#ok(asset)) asset;
      case (#err(msg)) return #err(msg);
    };

    switch (Asset.remove_encoding(self.fs, key, asset, args.content_encoding)) {
      case (#ok(_)) {};
      case (#err(msg)) return #err(msg);
    };

    #ok();

  };

  public func delete_asset(self : T.StableStore, args : T.DeleteAssetArguments) : T.Result<(), Text> {
    let key = Utils.format_key(args.key);

    switch (FileSystem.remove_asset(self.fs, key)) {
      case (#ok(_)) #ok();
      case (#err(msg)) #err(msg);
    };
  };

  public func clear(self : T.StableStore, _args : T.ClearArguments) {
    Upload.clear(self.upload);
    FileSystem.clear(self.fs);
  };

  public func propose_commit_batch(self : T.StableStore, args : T.CommitBatchArguments) : T.Result<(), Text> {
    Upload.propose_commit_batch(self.upload, args);
  };

  public func recertify(self : T.StableStore, key : Text) {
    let asset = switch (get_asset_using_aliases(self, key, true)) {
      case (#ok(asset)) asset;
      case (#err(msg)) return;
    };

    Certs.remove_asset_certificates(self.fs, key, asset, false);
    Certs.certify_asset(self.fs, key, asset, null);
  };

  public func commit_proposed_batch(self : T.StableStore, args : T.CommitProposedBatchArguments) : async* T.Result<(), Text> {
    let validate_result = validate_commit_proposed_batch_args(self, args);
    let #ok(_) = validate_result else return Utils.send_error(validate_result);

    let ?batch = Map.get(self.upload.batches, nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));
    let ?commit_batch_arguments = batch.commit_batch_arguments else return #err("Batch does not have proposed CommitBatchArguments");

    switch (await* commit_batch(self, commit_batch_arguments)) {
      case (#ok(_)) {};
      case (#err(error)) return #err(error);
    };

    batch.commit_batch_arguments := null;

    #ok();
  };

  public func delete_batch(self : T.StableStore, args : T.DeleteBatchArguments) : T.Result<(), Text> {
    switch (Upload.remove_batch(self.upload, args.batch_id)) {
      case (?_) #ok();
      case (null) #err(ErrorMessages.batch_not_found(args.batch_id));
    };
  };

  public func async_create_chunk(self : T.StableStore, args : T.CreateChunkArguments) : async T.Result<T.CreateChunkResponse, Text> {
    Upload.create_chunk(self.upload, args, Time.now(), Const.BATCH_EXPIRY_DURATION);
  };

  public func create_chunks(self : T.StableStore, args : T.CreateChunksArguments) : async* T.Result<T.CreateChunksResponse, Text> {
    let parallel = Buffer.Buffer<(async T.Result<T.CreateChunkResponse, Text>)>(args.content.size());
    let chunk_ids = Buffer.Buffer<Nat>(args.content.size());

    for (chunk in args.content.vals()) {
      let async_call = async_create_chunk(self, { batch_id = args.batch_id; content = chunk });
      parallel.add(async_call);
    };

    for (async_call in parallel.vals()) {
      let res = await async_call;
      let #ok({ chunk_id }) = res else return Utils.send_error(res);
      chunk_ids.add(chunk_id);
    };

    #ok({ chunk_ids = Buffer.toArray<Nat>(chunk_ids) });
  };

  public func commit_batch(self : T.StableStore, args : T.CommitBatchArguments) : async* T.Result<(), Text> {
    let asset_groups = Map.new<T.Key, Buffer<T.BatchOperationKind>>();

    label grouping_by_key for (op in args.operations.vals()) {
      if (op == #Clear({})) {
        clear(self, {});
        Map.clear(asset_groups);
        continue grouping_by_key;
      };

      let #CreateAsset({ key }) or #SetAssetContent({ key }) or #UnsetAssetContent({
        key;
      }) or #DeleteAsset({ key }) or #SetAssetProperties({ key }) = op else Debug.trap("unreachable");

      let group = Utils.map_get_or_put(asset_groups, thash, key, func() : Buffer<T.BatchOperationKind> = Buffer.Buffer(8));
      group.add(op);
    };

    // The temporary file system stores changes in the shared region
    // However, it manages its assets and encodings separately,
    // keeping track of its allocated memory blocks and deleting them
    // if the batch fails
    let tmp_fs = FileSystem.new(self.fs.region);

    label cloning_fs for (key in Map.keys(asset_groups)) {
      let prev = switch (FileSystem.get_asset_using_aliases(self.fs, key, false)) {
        case (#ok(?asset)) asset;
        case (#ok(null)) continue cloning_fs;
        case (#err(msg)) return #err("commit_batch(): " # msg);
      };

      let asset = Asset.copy(prev);

      // nullify the pointers to avoid changing the content in the main file system
      // during the execution as we only want to commit the changes if the batch is successful
      for (encoding in Map.vals(asset.encodings)) {
        encoding.content_pointer := (0, 0);
      };

      switch (FileSystem.insert_asset(tmp_fs, key, asset)) {
        case (#ok(_)) {};
        case (#err(msg)) return #err("commit_batch(): Error creating temporary file system \n\t" # msg);
      };

    };

    // replace fs with tmp_fs for the batch operations
    let other : T.StableStore = {
      self with
      fs = tmp_fs;
      var canister_id = self.canister_id;
      var streaming_callback = self.streaming_callback;
    };

    let parallel = Buffer.Buffer<(async* T.Result<(), Text>)>(asset_groups.size());
    for (group in Map.vals(asset_groups)) {
      let res = execute_batch_operations_with_same_key_sequentially(other, Buffer.toArray(group));
      parallel.add(res);
    };

    var opt_error_msg : ?Text = null;

    label waiting_for_parallel_calls for (async_call in parallel.vals()) {
      switch (await* async_call) {
        case (#ok(_)) {};
        case (#err(error)) {
          opt_error_msg := ?error;
          break waiting_for_parallel_calls;
        };
      };
    };

    switch (opt_error_msg) {
      case (null) {
        // batch was successful - merge changes
        FileSystem.merge(self.fs, tmp_fs);
      };
      case (?error_msg) {
        // batch failed - revert changes
        FileSystem.clear(tmp_fs); // -> deallocates memory blocks
        return #err(error_msg);
      };

    };

    ignore Upload.remove_batch(self.upload, args.batch_id);

    #ok();
  };

  public func execute_batch_operations_with_same_key_sequentially(self : T.StableStore, operations : [T.BatchOperationKind]) : async* T.Result<(), Text> {
    for (operation in operations.vals()) {
      let res = await* execute_batch_operation(self, operation);
      let #ok(_) = res else return Utils.send_error(res);
    };

    #ok();
  };

  public func execute_batch_operation(self : T.StableStore, operation : T.BatchOperationKind) : async* T.Result<(), Text> {
    let res : T.Result<(), Text> = switch (operation) {
      case (#Clear(args)) #ok(clear(self, args));
      case (#CreateAsset(args)) create_asset(self, args);
      case (#SetAssetContent(args)) await* set_asset_content(self, args);
      case (#UnsetAssetContent(args)) unset_asset_content(self, args);
      case (#DeleteAsset(args)) delete_asset(self, args);
      case (#SetAssetProperties(args)) set_asset_properties(self, args);
    };

    let #ok(_) = res else return Utils.send_error(res);

    #ok();
  };

  public func set_asset_properties(self : T.StableStore, args : T.SetAssetPropertiesArguments) : T.Result<(), Text> {
    let key = Utils.format_key(args.key);
    let asset = switch (FileSystem.get_asset_using_aliases(self.fs, key, false)) {
      case (#ok(?asset)) asset;
      case (#ok(null)) return #err(ErrorMessages.asset_not_found(key));
      case (#err(msg)) return #err(msg);
    };

    switch (asset.is_aliased, args.is_aliased) {
      case (?true, ??false or ?null) {
        // remove only aliases
        Certs.remove_asset_certificates(self.fs, key, asset, true);

        // only revoke support for aliases after the certificates have been removed
        asset.is_aliased := Option.get(args.is_aliased, ?false);
      };
      case (?false or null, ??true) {
        // needs to be updated first so certify_asset knows to certify for the aliases
        asset.is_aliased := ?true;
        Certs.certify_asset(self.fs, key, asset, null);
      };
      case (_) {};
    };

    Asset.set_max_age(asset, args.max_age);
    Asset.set_allow_raw_access(asset, args.allow_raw_access);
    Asset.set_headers(asset, args.headers);

    #ok();
  };

  public func delete_asset_contents(self : T.StableStore, asset : T.Asset) {
    for (encoding in Map.vals(asset.encodings)) {
      Encoding.deallocate_content(self.fs, encoding);
    };
  };

  public func validate_commit_proposed_batch_args(self : T.StableStore, args : T.CommitProposedBatchArguments) : T.Result<(), Text> {
    let ?batch = Upload.get_batch(self.upload, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));
    let ?commit_batch_arguments = batch.commit_batch_arguments else return #err("Batch does not have proposed CommitBatchArguments");

    if (commit_batch_arguments.operations.size() == 0) return #err("Batch has no operations");

    #ok();
  };

  public func certify_asset(self : T.StableStore, key : Text, asset : T.Asset, opt_encoding : ?Text) {
    Certs.remove_asset_certificates(self.fs, key, asset, false);
    Certs.certify_asset(self.fs, key, asset, opt_encoding);
  };

  public func hash_blob_chunks(content_chunks : [Blob], opt_prefix_sum_array_of_chunk_sizes : ?[Nat]) : async* Blob {
    await* hash_chunks(content_chunks, opt_prefix_sum_array_of_chunk_sizes);
  };

  func hash_bytes(sha256 : Sha256.Digest, chunks : Iter.Iter<Blob>) : async () {
    for (content in chunks) {
      sha256.writeBlob(content);
    };
  };

  func hash_chunks_section(
    self : T.StableStore,
    sha256 : Sha256.Digest,
    chunk_pointers : [(Nat, Nat)],
  ) : async T.Result<(), Text> {
    for ((chunk_address, chunk_size) in chunk_pointers.vals()) {
      let chunk = MemoryRegion.loadBlob(self.upload.region, chunk_address, chunk_size);

      sha256.writeBlob(chunk);
    };

    #ok();
  };

  public func async_hash_chunks_via_pointers(self : T.StableStore, chunk_pointers : [(Nat, Nat)]) : async* T.Result<Blob, Text> {
    // need to make multiple async calls to hash the content
    // to bypass the 40B instruction limit

    // From the Sha256 benchmarks we know that hashing 1MB of data uses about 320M instructions
    // So we can safely hash about 60MB of data before we hit the 40B instruction limit
    // Assuming each chunk is less than 2MB (the suggested transfer limit for the IC), we can hash
    // 60 in a single call

    let buffer = Buffer.Buffer<(Nat, Nat)>(chunk_pointers.size());
    let hash_sections = Buffer.Buffer<[(Nat, Nat)]>(chunk_pointers.size());

    var accumulated_size = 0;
    var i = 0;

    for (chunk_pointer in chunk_pointers.vals()) {

      buffer.add(chunk_pointer);
      accumulated_size += chunk_pointer.1;
      i += 1;

      if (accumulated_size > Const.MAX_HASHING_BYTES_PER_CALL) {
        accumulated_size := chunk_pointer.1;
        hash_sections.add(Buffer.toArray(buffer));
        buffer.clear();
      };

      if (i == chunk_pointers.size()) {
        hash_sections.add(Buffer.toArray(buffer));
        buffer.clear();
      };

    };

    let sha256 = Sha256.Digest(#sha256);

    for (hash_section in hash_sections.vals()) {
      switch (await hash_chunks_section(self, sha256, hash_section)) {
        case (#ok()) {};
        case (#err(msg)) return #err(msg);
      };
    };

    #ok(sha256.sum());

  };

  public func hash_chunks(content_chunks : [Blob], opt_prefix_sum_array_of_chunk_sizes : ?[Nat]) : async* Blob {
    // need to make multiple async calls to hash the content
    // to bypass the 40B instruction limit

    // From the Sha256 benchmarks we know that hashing 1MB of data uses about 320M instructions
    // So we can safely hash about 60MB of data before we hit the 40B instruction limit
    // Assuming each chunk is less than 2MB (the suggested transfer limit for the IC), we can hash
    // 60 in a single call

    let buffer = Buffer.Buffer<Nat>(content_chunks.size());

    var prev_accumulated_size = 0;
    let content_chunks_prefix_sum = switch (opt_prefix_sum_array_of_chunk_sizes) {
      case (?prefix_sum_array_of_chunk_sizes) prefix_sum_array_of_chunk_sizes;
      case (null) Array.tabulate(
        content_chunks.size(),
        func(i : Nat) : Nat {
          let curr = prev_accumulated_size + content_chunks[i].size();
          prev_accumulated_size := curr;
          curr;
        },
      );
    };

    assert content_chunks_prefix_sum.size() == content_chunks.size();
    prev_accumulated_size := 0;
    for ((i, accumulated_size) in Itertools.enumerate(content_chunks_prefix_sum.vals())) {
      assert accumulated_size >= prev_accumulated_size;

      let is_exceeding_limit = accumulated_size - prev_accumulated_size > Const.MAX_HASHING_BYTES_PER_CALL;
      let is_last_chunk = i == content_chunks.size() - 1;

      if (is_exceeding_limit) {
        buffer.add(i);
        prev_accumulated_size := accumulated_size;
      };

      if (is_last_chunk) {
        buffer.add(i + 1);
      };
    };

    var prev_chunk_index = 0;
    let hashable_chunks_per_call = Iter.map(
      buffer.vals(),
      func(end_index : Nat) : Iter.Iter<Blob> {
        let slice = Itertools.fromArraySlice(content_chunks, prev_chunk_index, end_index);
        prev_chunk_index := end_index;
        slice;
      },
    );

    let sha256 = Sha256.Digest(#sha256);

    for (chunked_contents in hashable_chunks_per_call) {
      await hash_bytes(sha256, chunked_contents);
    };

    sha256.sum();

  };

};
