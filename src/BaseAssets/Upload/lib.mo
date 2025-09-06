/// The upload module is responsible for managing data that is uploaded but not yet committed to the canister.

import Option "mo:base@0/Option";
import Debug "mo:base@0/Debug";
import Iter "mo:base@0/Iter";
import Text "mo:base@0/Text";
import Time "mo:base@0/Time";
import Nat64 "mo:base@0/Nat64";
import Nat16 "mo:base@0/Nat16";

import Map "mo:map@9/Map";
import Vector "mo:vector@0";
import MemoryRegion "mo:memory-region@1/MemoryRegion";
import Sha256 "mo:sha2@0/Sha256";

import T "../Types";
import ComputeEvidence "ComputeEvidence";
import ErrorMessages "../ErrorMessages";

module {

  public type Upload = T.Upload;

  public func new(region : T.MemoryRegion) : Upload {
    {
      batches = Map.new();
      var next_batch_id = 0;

      chunks = Map.new();
      region;
      var next_chunk_id = 0;

      configuration = {
        var max_batches = null;
        var max_chunks = null;
        var max_bytes = null;
      };
    };
  };

  public func clear(self : Upload) {

    // !don't clear entire region (MemoryRegion.clear(region)) because it's shared with other modules

    self.next_batch_id := 0;
    self.next_chunk_id := 0;

    // clearing batches releases the memory allocated for chunks
    for (batch_id in Map.keys(self.batches)) {
      ignore remove_batch(self, batch_id);
    };

    // clear configuration
    self.configuration.max_batches := null;
    self.configuration.max_chunks := null;
    self.configuration.max_bytes := null;

  };

  public func get_configuration(self : Upload) : T.ConfigurationResponse {
    let config : T.ConfigurationResponse = {
      max_batches = self.configuration.max_batches;
      max_chunks = self.configuration.max_chunks;
      max_bytes = self.configuration.max_bytes;
    };

    config;
  };

  public func set_max_batches(self : Upload, opt_max_batches : ??Nat64) {
    switch (opt_max_batches) {
      case (?max_batches) self.configuration.max_batches := max_batches;
      case (null) {}; // do nothing
    };
  };

  public func set_max_chunks(self : Upload, opt_max_chunks : ??Nat64) {
    switch (opt_max_chunks) {
      case (?max_chunks) self.configuration.max_chunks := max_chunks;
      case (null) {}; // do nothing
    };
  };

  public func set_max_bytes(self : Upload, opt_max_bytes : ??Nat64) {
    switch (opt_max_bytes) {
      case (?max_bytes) self.configuration.max_bytes := max_bytes;
      case (null) {};
    };
  };

  func deallocate_chunk(self : Upload, chunk_id : T.ChunkId) : ?T.StoredChunk {
    let ?chunk = Map.remove(self.chunks, T.nhash, chunk_id) else return null;
    MemoryRegion.deallocate(self.region, chunk.pointer.0, chunk.pointer.1);
    ?chunk;
  };

  func deallocate_batch_chunks(self : Upload, batch : T.Batch) {
    for (chunk_id in Vector.vals(batch.chunk_ids)) {
      let ?chunk = deallocate_chunk(self, chunk_id);
    };
  };

  public func remove_batch(self : Upload, batch_id : Nat) : ?T.Batch {
    let ?batch = Map.remove(self.batches, T.nhash, batch_id) else return null;
    deallocate_batch_chunks(self, batch);

    ?batch;
  };

  public func get_batch(self : Upload, batch_id : Nat) : ?T.Batch {
    Map.get(self.batches, T.nhash, batch_id);
  };

  public func create_batch(self : Upload, curr_time : T.Time, expiry_duration : Nat) : T.Result<T.CreateBatchResponse, Text> {

    var batch_with_commit_args_exists : ?(T.BatchId, ?T.EvidenceComputation) = null;

    for ((batch_id, batch) in Map.entries(self.batches)) {
      // remove expired batches only if the evidence has not been computed
      if (batch.expires_at < curr_time and Option.isNull(batch.evidence_computation)) {
        ignore remove_batch(self, batch_id);
      };

      // or evidence computation is done
      if (Option.isSome(batch.commit_batch_arguments) and Option.isNull(batch_with_commit_args_exists)) {
        batch_with_commit_args_exists := ?(batch_id, batch.evidence_computation);
      };
    };

    switch (batch_with_commit_args_exists) {
      case (?(batch_id, ?#Computed(_))) {
        return #err("Batch " # debug_show # batch_id # " is already proposed.  Delete or execute it to propose another.");
      };
      case (?(batch_id, _)) {
        return #err("Batch " # debug_show # batch_id # " has not completed evidence computation.  Wait for it to expire or delete it to propose another.");
      };
      case (_) {};
    };

    switch (self.configuration.max_batches) {
      case (?max_batches) {
        if (Nat64.fromNat(Map.size(self.batches)) >= max_batches) {
          return #err("Maximum number of batches reached.");
        };
      };
      case (_) {};
    };

    let batch_id = self.next_batch_id;
    self.next_batch_id += 1;

    let batch : T.Batch = {
      var expires_at = curr_time + expiry_duration;
      var commit_batch_arguments = null;
      var evidence_computation = null;
      var total_bytes = 0;
      chunk_ids = Vector.new();
    };

    ignore Map.put(self.batches, T.nhash, batch_id, batch);

    #ok({ batch_id });

  };

  public func create_chunk(self : Upload, args : T.Chunk, curr_time : T.Time, expiry_duration : Nat) : T.Result<T.CreateChunkResponse, Text> {

    switch (self.configuration.max_chunks) {
      case (?max_chunks) {
        if (Nat64.fromNat(Map.size(self.chunks)) >= max_chunks) {
          return #err("Maximum number of chunks reached.");
        };
      };
      case (_) {};
    };

    let batch = switch (Map.get(self.batches, T.nhash, args.batch_id)) {
      case (?batch) {
        if (Option.isSome(batch.commit_batch_arguments)) {
          return #err("Batch " # debug_show (args.batch_id) # " has already been proposed.");
        };

        batch;
      };
      case (_) return #err(ErrorMessages.batch_not_found(args.batch_id));
    };

    let total_bytes_plus_new_chunk = Nat64.fromNat(batch.total_bytes) + Nat64.fromNat(args.content.size());

    switch (self.configuration.max_bytes) {
      case (?max_bytes) if (total_bytes_plus_new_chunk > max_bytes) {
        return #err("Maximum number of bytes reached. Can only add " # debug_show (max_bytes - Nat64.fromNat(batch.total_bytes)) # " more bytes but trying to add " # debug_show args.content.size());
      };
      case (_) {};
    };

    let chunk_id = self.next_chunk_id;
    self.next_chunk_id += 1;

    let chunk_address = MemoryRegion.addBlob(self.region, args.content);

    let chunk = {
      batch_id = args.batch_id;
      pointer = (chunk_address, args.content.size());
    };

    ignore Map.put(self.chunks, T.nhash, chunk_id, chunk);
    batch.expires_at := curr_time + expiry_duration;
    batch.total_bytes += args.content.size();
    Vector.add(batch.chunk_ids, chunk_id);

    #ok({ chunk_id });
  };

  public func get_chunk(self : T.Upload, chunk_id : T.ChunkId) : ?T.StoredChunk {
    Map.get<Nat, T.StoredChunk>(self.chunks, T.nhash, chunk_id);
  };

  public func get_chunk_pointer(self : T.Upload, chunk_id : T.ChunkId) : ?(Nat, Nat) {
    let ?chunk = get_chunk(self, chunk_id) else return null;
    ?chunk.pointer;
  };

  public func propose_commit_batch(self : T.Upload, args : T.CommitBatchArguments) : T.Result<(), Text> {
    let ?batch = Map.get(self.batches, T.nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));

    if (Option.isSome(batch.commit_batch_arguments)) return #err("Batch already has proposed T.CommitBatchArguments");

    batch.commit_batch_arguments := ?args;

    #ok();
  };

  let DEFAULT_MAX_COMPUTE_EVIDENCE_ITERATIONS : Nat16 = 20;

  public func compute_evidence(self : Upload, args : T.ComputeEvidenceArguments) : async* T.Result<?Blob, Text> {
    let ?batch = Map.get(self.batches, T.nhash, args.batch_id) else return #err(ErrorMessages.batch_not_found(args.batch_id));
    let ?commit_batch_args = batch.commit_batch_arguments else return #err("Batch does not have CommitBatchArguments");

    let max_iterations = switch (args.max_iterations) {
      case (?max_iterations) max_iterations;
      case (_) DEFAULT_MAX_COMPUTE_EVIDENCE_ITERATIONS;
    };

    let _evidence_computation : T.EvidenceComputation = switch (batch.evidence_computation) {
      case (?evidence_computation) {
        batch.evidence_computation := null;
        evidence_computation;
      };
      case (_) {
        #NextOperation {
          operation_index = 0;
          hasher_state = do {
            let digest = Sha256.Digest(#sha256);
            digest.share();
          };
        };
      };
    };

    var evidence_computation = _evidence_computation;

    label for_loop for (_ in Iter.range(1, Nat16.toNat(max_iterations))) {
      evidence_computation := ComputeEvidence.advance(self, self.chunks, commit_batch_args, evidence_computation);

      switch (evidence_computation) {
        case (#Computed(_)) break for_loop;
        case (_) {};
      };
    };

    batch.evidence_computation := ?evidence_computation;

    switch (evidence_computation) {
      case (#Computed(evidence)) #ok(?evidence);
      case (_) #ok(null);
    };
  };

};
