import Result "mo:base@0/Result";
import Array "mo:base@0/Array";
import Time "mo:base@0/Time";
import Blob "mo:base@0/Blob";
import Iter "mo:base@0/Iter";
import Option "mo:base@0/Option";
import Principal "mo:base@0/Principal";
import StableMemory "mo:base@0/ExperimentalStableMemory";

import Set "mo:map@9/Set";
import Map "mo:map@9/Map";
import CertifiedAssets "mo:certified-assets@0/Stable";
import SHA256 "mo:sha2@0/Sha256";
import Vector "mo:vector@0";

import V0_types "../V0/types";
import V0_1_0_types "../V0_1_0/types";

import V0_2_0_types "types";

module {
  type Vector<A> = Vector.Vector<A>;
  type Map<K, V> = Map.Map<K, V>;
  type Set<V> = Set.Set<V>;
  type Result<T, E> = Result.Result<T, E>;
  type Time = Time.Time;
  let { thash } = Map;

  public func init_stable_store(canister_id : Principal, owner : Principal) : V0_1_0_types.StableStore {
    let state : V0_1_0_types.StableStore = {
      var canister_id = canister_id;
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
      copy_on_write_batches = Map.new();
      var next_batch_id = 1;

      commit_principals = Set.new();
      prepare_principals = Set.new();
      manage_permissions_principals = Set.new();

    };

    state;
  };

  public func upgrade_from_v0_1_0(v0_1_0 : V0_1_0_types.StableStore) : V0_2_0_types.StableStore {

    let v0_2_0 : V0_2_0_types.StableStore = {
      var canister_id = v0_1_0.canister_id;
      var streaming_callback = v0_1_0.streaming_callback;

      assets = upgrade_assets_from_v0_1_0(v0_1_0.assets);
      certificate_store = v0_1_0.certificate_store;
      chunks = Map.new();
      batches = Map.new();
      copy_on_write_batches = Map.new();

      commit_principals = v0_1_0.commit_principals;
      prepare_principals = v0_1_0.prepare_principals;
      manage_permissions_principals = v0_1_0.manage_permissions_principals;

      configuration = v0_1_0.configuration;

      var next_chunk_id = v0_1_0.next_chunk_id;
      var next_batch_id = v0_1_0.next_batch_id;

    };

  };

  public func upgrade_from_v0(v0 : V0_types.StableStore) : V0_2_0_types.StableStore {

    let v0_2_0 : V0_2_0_types.StableStore = {
      var canister_id = Option.get(v0.canister_id, Principal.fromText("aaaaa-aa"));
      var streaming_callback = null;

      assets = upgrade_assets(v0.assets);
      certificate_store = v0.certificate_store;
      chunks = Map.new();
      batches = Map.new();
      copy_on_write_batches = Map.new();

      commit_principals = v0.commit_principals;
      prepare_principals = v0.prepare_principals;
      manage_permissions_principals = v0.manage_permissions_principals;

      configuration = v0.configuration;

      var next_chunk_id = v0.next_chunk_id;
      var next_batch_id = v0.next_batch_id;

    };

  };

  func upgrade_assets_from_v0_1_0(assets : Map<Text, V0_1_0_types.Asset>) : Map<Text, V0_2_0_types.Asset> {

    Map.fromIter<Text, V0_2_0_types.Asset>(
      Iter.map<(Text, V0_1_0_types.Asset), (Text, V0_2_0_types.Asset)>(
        Map.entries(assets),
        func((asset_id, asset) : (Text, V0_1_0_types.Asset)) : (Text, V0_2_0_types.Asset) {
          let new_asset = {
            var content_type = asset.content_type;
            headers = asset.headers;
            var is_aliased = asset.is_aliased;
            var max_age = asset.max_age;
            var allow_raw_access = asset.allow_raw_access;

            var last_certified_encoding = Map.keys(asset.encodings).next();
            encodings = upgrade_asset_encodings_from_v0_1_0(asset.encodings);
          };

          (asset_id, new_asset);
        },
      ),
      thash,
    );

  };

  func upgrade_asset_encodings_from_v0_1_0(encodings : Map<Text, V0_1_0_types.AssetEncoding>) : Map<Text, V0_2_0_types.AssetEncoding> {

    Map.fromIter<Text, V0_2_0_types.AssetEncoding>(
      Iter.map<(Text, V0_1_0_types.AssetEncoding), (Text, V0_2_0_types.AssetEncoding)>(
        Map.entries(encodings),
        func((encoding_id, encoding) : (Text, V0_1_0_types.AssetEncoding)) : (Text, V0_2_0_types.AssetEncoding) {
          let new_encoding : V0_2_0_types.AssetEncoding = {
            var modified = encoding.modified;
            var total_length = encoding.total_length;
            var certified = encoding.certified;
            var sha256 = encoding.sha256;

            var content_chunks : [Blob] = Array.map<[Nat8], Blob>(
              encoding.content_chunks,
              Blob.fromArray,
            );

            var content_chunks_prefix_sum : [Nat] = encoding.content_chunks_prefix_sum;
          };

          (encoding_id, new_encoding);
        },
      ),
      thash,
    );

  };

  func upgrade_content_chunks(content_chunks_vec : Vector<Blob>) : [Blob] {
    Array.tabulate(
      Vector.size(content_chunks_vec),
      func(i : Nat) : Blob {
        let content_chunks = Vector.get(content_chunks_vec, i);
        content_chunks;
      },
    );
  };

  func get_prefix_sum(array : [Blob]) : [Nat] {
    var sum = 0;

    Array.tabulate(
      array.size(),
      func(i : Nat) : Nat {
        sum += array.get(i).size();
        sum;
      },
    );
  };

  func upgrade_asset_encodings(encodings : Map<Text, V0_types.AssetEncoding>) : Map<Text, V0_2_0_types.AssetEncoding> {

    Map.fromIter<Text, V0_2_0_types.AssetEncoding>(
      Iter.map<(Text, V0_types.AssetEncoding), (Text, V0_2_0_types.AssetEncoding)>(
        Map.entries(encodings),
        func((encoding_id, encoding) : (Text, V0_types.AssetEncoding)) : (Text, V0_2_0_types.AssetEncoding) {
          let new_encoding : V0_2_0_types.AssetEncoding = {
            var modified = encoding.modified;
            var total_length = encoding.total_length;
            var certified = encoding.certified;
            var sha256 = encoding.sha256;

            var content_chunks : [Blob] = upgrade_content_chunks(encoding.content_chunks);
            var content_chunks_prefix_sum : [Nat] = get_prefix_sum(Vector.toArray<Blob>(encoding.content_chunks));
          };

          (encoding_id, new_encoding);
        },
      ),
      thash,
    );

  };

  func upgrade_assets(assets : Map<Text, V0_types.Asset>) : Map<Text, V0_2_0_types.Asset> {

    Map.fromIter<Text, V0_2_0_types.Asset>(
      Iter.map<(Text, V0_types.Asset), (Text, V0_2_0_types.Asset)>(
        Map.entries(assets),
        func((asset_id, asset) : (Text, V0_types.Asset)) : (Text, V0_2_0_types.Asset) {
          let new_asset = {
            var content_type = asset.content_type;
            headers = asset.headers;
            var is_aliased = asset.is_aliased;
            var max_age = asset.max_age;
            var allow_raw_access = asset.allow_raw_access;

            var last_certified_encoding = Map.keys(asset.encodings).next();
            encodings = upgrade_asset_encodings(asset.encodings);
          };

          (asset_id, new_asset);
        },
      ),
      thash,
    );

  };

};
