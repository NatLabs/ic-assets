import Result "mo:base@0/Result";
import Time "mo:base@0/Time";

import Set "mo:map@9/Set";
import Map "mo:map@9/Map";
import CertifiedAssets "mo:certified-assets@0";
import SHA256 "mo:sha2@0/Sha256";
import Vector "mo:vector@0";

import HttpTypes "mo:http-types@1";

import V0_Types "types";

module V0 {

  type Map<K, V> = Map.Map<K, V>;
  type Set<V> = Set.Set<V>;
  type Result<T, E> = Result.Result<T, E>;
  type Time = Time.Time;
  type Vector<A> = Vector.Vector<A>;

  public func init_stable_store(owner : Principal) : V0_Types.StableStore {
    let state : V0_Types.StableStore = {
      var canister_id = null;
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
      var next_batch_id = 1;

      commit_principals = Set.new();
      prepare_principals = Set.new();
      manage_permissions_principals = Set.new();

    };

    state;
  };

};
