import Debug "mo:base@0/Debug";
import Result "mo:base@0/Result";
import Array "mo:base@0/Array";
import Time "mo:base@0/Time";
import Blob "mo:base@0/Blob";
import Iter "mo:base@0/Iter";
import Option "mo:base@0/Option";
import Principal "mo:base@0/Principal";
import StableMemory "mo:base@0/ExperimentalStableMemory";
import Text "mo:base@0/Text";
import Buffer "mo:base@0/Buffer";
import Nat "mo:base@0/Nat";

import Itertools "mo:itertools@0/Iter";
import MemoryRegion "mo:memory-region@1/MemoryRegion";
import RevIter "mo:itertools@0/RevIter";
import Set "mo:map@9/Set";
import Map "mo:map@9/Map";
import CertifiedAssets "mo:certified-assets@0/Stable";
import SHA256 "mo:sha2@0/Sha256";
import Vector "mo:vector@0";
import BaseX "mo:base-x-encoder@2";
import HttpParser "mo:http-parser@0";

import V0_types "../V0/types";
import V0_1_0_types "../V0_1_0/types";
import V0_2_0_types "../V0_2_0/types";
import V0_3_0_types "types";

import V0_2_0_upgrade "../V0_2_0/upgrade";

module {
  let T = V0_3_0_types;

  type Vector<A> = Vector.Vector<A>;
  type Map<K, V> = Map.Map<K, V>;
  type Set<V> = Set.Set<V>;
  type Result<T, E> = Result.Result<T, E>;
  type Time = Time.Time;
  let { thash } = Map;

  public func upgrade_from_v0_1_0(v0_1_0 : V0_1_0_types.StableStore) : V0_3_0_types.StableStore {
    upgrade_from_v0_2_0(
      V0_2_0_upgrade.upgrade_from_v0_1_0(v0_1_0)
    );

  };

  public func upgrade_from_v0(v0 : V0_types.StableStore) : V0_3_0_types.StableStore {

    upgrade_from_v0_2_0(
      V0_2_0_upgrade.upgrade_from_v0(v0)
    );

  };

  public func upgrade_from_v0_2_0(v0_2_0 : V0_2_0_types.StableStore) : V0_3_0_types.StableStore {

    let shared_region = MemoryRegion.new();
    let fs = FileSystem.new(shared_region);
    let upload = Upload.new(shared_region);

    for ((key, prev_asset) in Map.entries(v0_2_0.assets)) {
      let new_asset = upgrade_asset_from_v0_2_0(fs, prev_asset);

      switch (FileSystem.insert_asset(fs, key, new_asset)) {
        case (#ok(_)) {};
        case (#err(msg)) Debug.trap("Failed to migrate ic-assets: " # msg);
      };

    };

    let new_store : V0_3_0_types.StableStore = {

      fs;
      upload;
      shared_region;

      var canister_id = v0_2_0.canister_id;
      var streaming_callback = v0_2_0.streaming_callback;

      permissions = {
        commit_principals = v0_2_0.commit_principals;
        prepare_principals = v0_2_0.prepare_principals;
        manage_permissions_principals = v0_2_0.manage_permissions_principals;
      };

      configuration = v0_2_0.configuration;
    };

    new_store;
  };

  type MemoryRegion = MemoryRegion.MemoryRegion;

  func upgrade_asset_encodings_from_v0_2_0(fs : T.FileSystem, encodings : Map<Text, V0_2_0_types.AssetEncoding>) : Map<Text, V0_3_0_types.AssetEncoding> {

    Map.fromIter<Text, V0_3_0_types.AssetEncoding>(
      Iter.map<(Text, V0_2_0_types.AssetEncoding), (Text, V0_3_0_types.AssetEncoding)>(
        Map.entries(encodings),
        func((encoding_id, prev_encoding) : (Text, V0_2_0_types.AssetEncoding)) : (Text, V0_3_0_types.AssetEncoding) {

          let new_encoding = Encoding.new();

          Encoding.replace_content_via_chunks(
            fs,
            new_encoding,
            prev_encoding.content_chunks.vals(),
            prev_encoding.total_length,
            prev_encoding.sha256,
          );

          new_encoding.certified := prev_encoding.certified;

          (encoding_id, new_encoding);
        },
      ),
      thash,
    );

  };

  func upgrade_asset_from_v0_2_0(fs : T.FileSystem, asset : V0_2_0_types.Asset) : (V0_3_0_types.Asset) {
    let new_asset = {
      var content_type = asset.content_type;
      headers = asset.headers;
      var is_aliased = asset.is_aliased;
      var max_age = asset.max_age;
      var allow_raw_access = asset.allow_raw_access;

      var last_certified_encoding = asset.last_certified_encoding;
      encodings = upgrade_asset_encodings_from_v0_2_0(fs, asset.encodings);
    };

    (new_asset);
  };

  module ErrorMessages {
    public func encoding_not_found(asset_key : T.Key, encoding_name : Text) : Text {
      "Encoding not found for asset " # debug_show asset_key # " with encoding " # encoding_name;
    };
  };

  module Encoding {

    public func new() : T.AssetEncoding {
      {
        var modified = Time.now();
        var content_pointer = (0, 0);
        var total_length = 0;
        var certified = false;
        var sha256 = "";
      };
    };

    public func deallocate_content(fs : T.FileSystem, encoding : T.AssetEncoding) {
      let (address, size) = encoding.content_pointer;
      MemoryRegion.deallocate(fs.region, address, size);
      encoding.content_pointer := (0, 0);
    };

    public func replace_content_via_chunks(fs : T.FileSystem, encoding : T.AssetEncoding, chunks_iter : T.Iter<Blob>, total_length : Nat, content_hash : Blob) {
      // allocating new space is not guaranteed so we need to ensure we have enough space
      // before deallocating the old content
      let new_content_address = MemoryRegion.allocate(fs.region, total_length);
      deallocate_content(fs, encoding);

      var offset = 0;
      for (chunk in chunks_iter) {
        MemoryRegion.storeBlob(fs.region, new_content_address + offset, chunk);
        offset += chunk.size();
      };

      encoding.content_pointer := (new_content_address, total_length);

      encoding.modified := Time.now();
      encoding.total_length := total_length;
      encoding.sha256 := content_hash;
    };

  };

  module Asset {

    /// Creates a new asset.
    public func new() : T.Asset = {
      encodings = Map.new();
      headers = Map.new();
      var content_type = "";
      var is_aliased = null;
      var max_age = null;
      var allow_raw_access = null;
      var last_certified_encoding = null;
    };

    /// Deallocates the content in each asset encoding
    public func deallocate(fs : T.FileSystem, self : T.Asset) {
      for ((_, encoding) in Map.entries(self.encodings)) {
        Encoding.deallocate_content(fs, encoding);
      };
    };

  };

  module Common {

    public func get_asset(fs : T.FileSystem, key : T.Key) : T.Result<?T.Asset, Text> {

      let paths = Iter.toArray(Text.split(key, #text("/")));

      var map = fs.root;

      label extracting_asset for ((i, path) in Itertools.enumerate(paths.vals())) {
        if (path == "") if (i == 0 or i + 1 == paths.size()) {
          continue extracting_asset;
        } else {
          return #err("Invalid path: " # key);
        };

        map := switch (Map.get<Text, T.HierarchicalAssets>(map, T.thash, path)) {
          case (?#Directory(map)) map;
          case (?#Asset(asset)) if (i + 1 == paths.size()) {
            return #ok(?asset);
          } else {
            let asset_path = Text.join("/", Itertools.take(paths.vals(), i + 1));
            return #err("An asset exists at the path: " # asset_path);
          };
          case (null) return #ok(null);
        };

      };

      #err("A directory exists at the path: " # key);

    };

    public func join_paths(paths : [Text]) : Text {
      Text.join(
        "/",
        Itertools.flatten(
          Iter.map<Text, T.Iter<Text>>(
            paths.vals(),
            func(path : Text) : T.Iter<Text> {
              // assumes paths are formatted correctly
              // so we don't need to worry about double slashes
              Text.tokens(path, #text("/"));
            },
          )
        ),
      );
    };

    public func get_html_file_aliases(fs : T.FileSystem, key : Text) : T.Iter<Text> {
      if (Text.endsWith(key, #text ".html")) return Itertools.empty();

      // todo: check if the asset file is an html file

      let aliases = [
        join_paths([key, "index.html"]),
        key # ".html",
      ];

      // an alias cannot overwrite an existing asset
      Iter.filter(
        aliases.vals(),
        func(alias : Text) : Bool {
          switch (get_asset(fs, alias)) {
            case (#ok(null)) true;
            case (#ok(?(_)) or #err(_)) false;
          };
        },
      )

    };

  };

  module Certs {

    func get_success_endpoint(key_or_alias : Text, encoding : T.AssetEncoding, headers_array : [(Text, Text)], is_aliased : Bool) : T.Endpoint {

      CertifiedAssets.Endpoint(key_or_alias, null)
      // request certification is not supported in this context
      .no_request_certification()
      // the content's hash is inserted directly instead of computing it from the content
      .hash(encoding.sha256)
      // certifies response headers
      .response_headers(headers_array)
      // certifies success status code
      .status(200)
      // if the asset is an alias, that ends with "/index.html", then it is a fallback path
      .is_fallback_path(is_aliased);
    };

    func get_cache_endpoint(key_or_alias : Text, encoding : T.AssetEncoding, headers_array : [(Text, Text)], is_aliased : Bool) : T.Endpoint {
      // update the status code to '304 Not Modified'
      get_success_endpoint(key_or_alias, encoding, headers_array, is_aliased).status(304);
    };

    public func build_headers(asset : T.Asset, encoding_name : Text, encoding_sha256 : Blob) : T.Map<Text, Text> {
      let headers = Map.new<Text, Text>();
      ignore Map.put(headers, T.thash, "content-type", asset.content_type);

      switch (asset.max_age) {
        case (?max_age) {
          ignore Map.put(headers, T.thash, "cache-control", "max-age=" # debug_show (max_age));
        };
        case (_) {};
      };

      ignore Map.put(headers, T.thash, "content-encoding", encoding_name);
      ignore Map.put(headers, T.thash, "vary", "accept-encoding");

      let hex = BaseX.toHex(encoding_sha256.vals(), { isUpper = false; prefix = #none });
      let etag_value = "\"" # hex # "\"";
      ignore Map.put(headers, T.thash, "etag", etag_value);

      for ((key, value) in Map.entries(asset.headers)) {
        ignore Map.put(headers, T.thash, key, value);
      };

      headers;
    };

    public func certify_encoding(fs : T.FileSystem, asset_key : Text, asset : T.Asset, encoding_name : Text) : T.Result<(), Text> {

      let ?encoding = Map.get(asset.encodings, T.thash, encoding_name) else return #err(ErrorMessages.encoding_not_found(asset_key, encoding_name));

      let aliases = if (asset.is_aliased == ?true) Common.get_html_file_aliases(fs, asset_key) else Itertools.empty();

      let key_and_aliases = Itertools.add(aliases, asset_key);

      let headers = build_headers(asset, encoding_name, encoding.sha256);
      let headers_array = Map.toArray(headers);

      for (key_or_alias in key_and_aliases) {

        let success_endpoint = get_success_endpoint(key_or_alias, encoding, headers_array, asset.is_aliased == ?true);
        let cache_endpoint = get_cache_endpoint(key_or_alias, encoding, headers_array, asset.is_aliased == ?true);

        CertifiedAssets.certify(fs.certs, success_endpoint);
        CertifiedAssets.certify(fs.certs, cache_endpoint);

      };

      asset.last_certified_encoding := ?encoding_name;

      #ok();
    };

    // certifies all encodings of an asset
    // > remember to delete all previous certifications associated with the asset's key
    // > before their data is modified by calling remove_asset_certificates

    public func certify_asset(fs : T.FileSystem, key : Text, asset : T.Asset, opt_encoding_name : ?Text) {

      let encodings = switch (opt_encoding_name) {
        case (?encoding_name) switch (Map.get(asset.encodings, T.thash, encoding_name)) {
          case (?encoding) [(encoding_name, encoding)].vals();
          case (_) Debug.trap("certify_asset(): Encoding not found.");
        };
        case (_) Map.entries(asset.encodings);
      };

      for ((encoding_name, encoding) in encodings) {
        ignore certify_encoding(fs, key, asset, encoding_name);
        encoding.certified := true;
      };
    };

    public func remove_asset_certificates(fs : T.FileSystem, key : Text, asset : T.Asset, only_aliases : Bool) {
      for ((encoding_name, encoding) in Map.entries(asset.encodings)) {
        remove_encoding_certificate(fs, key, asset, encoding_name, encoding, only_aliases);
      };

      if (not only_aliases) {
        CertifiedAssets.remove_all(fs.certs, key);
      };
    };

    public func remove_encoding_certificate(fs : T.FileSystem, asset_key : Text, asset : T.Asset, encoding_name : Text, encoding : T.AssetEncoding, only_aliases : Bool) {

      // verify that `only_aliases is set to true only if the asset is aliased
      // if not, then we are probably calling this function incorrectly
      if (only_aliases) { assert asset.is_aliased == ?true };

      let aliases = if (asset.is_aliased == ?true) Common.get_html_file_aliases(fs, asset_key) else Itertools.empty();
      let keys = if (only_aliases) aliases else Itertools.add(aliases, asset_key);

      for (key_or_alias in keys) {

        let headers = build_headers(asset, encoding_name, encoding.sha256);
        let headers_array = Map.toArray(headers);

        let success_endpoint = get_success_endpoint(key_or_alias, encoding, headers_array, asset.is_aliased == ?true);
        let cache_endpoint = get_cache_endpoint(key_or_alias, encoding, headers_array, asset.is_aliased == ?true);

        CertifiedAssets.remove(fs.certs, success_endpoint);
        CertifiedAssets.remove(fs.certs, cache_endpoint);
      };

      if (not only_aliases) {
        encoding.certified := false;

        // only used for certification v1
        asset.last_certified_encoding := null;

      };

    };

  };

  module FileSystem {

    public type HierarchicalAssets = T.HierarchicalAssets;
    public type Directory = T.Map<Text, HierarchicalAssets>;
    public type DirectoryContent = T.DirectoryContent;

    public type FileSystem = {
      root : Directory;
      region : T.MemoryRegion;
      certs : T.CertifiedAssets;
    };

    public func new(region : T.MemoryRegion) : FileSystem {
      {
        root = Map.new();
        region;
        certs = CertifiedAssets.init_stable_store();
      };
    };

    func basename(path : Text) : Text {
      let parts = Iter.toArray(Text.split(path, #text("/")));
      parts[parts.size() - 1];
    };

    func dirname(path : Text) : Text {
      let parts = Iter.toArray(Text.split(path, #text("/")));
      Text.join("/", Itertools.take(parts.vals(), parts.size() - 1 : Nat));
    };

    public func create_directory(fs : FileSystem, key : T.Key) : T.Result<T.Directory, Text> {

      let paths = Iter.toArray(Text.split(key, #text("/")));

      var map = fs.root;

      label creating_directory for ((i, path) in Itertools.enumerate(paths.vals())) {
        if (path == "") if (i == 0 or i + 1 == paths.size()) {
          continue creating_directory;
        } else {
          return #err("Invalid path: " # key);
        };

        map := switch (Map.get(map, T.thash, path)) {
          case (?#Asset(_)) return #err("Asset already exists with path: " # Text.join("/", Itertools.take(paths.vals(), i + 1)));
          case (?#Directory(directory_map)) directory_map;
          case (null) {
            let new_directory : T.Directory = Map.new();
            ignore Map.put(map, T.thash, path, #Directory(new_directory));
            new_directory;
          };
        };
      };

      #ok(map);

    };

    public func insert_asset(fs : FileSystem, key : T.Key, asset : T.Asset) : T.Result<T.Asset, Text> {
      let directory_name = dirname(key);
      let asset_name = basename(key);

      let directory_map = switch (create_directory(fs, directory_name)) {
        case (#ok(directory_map)) directory_map;
        case (#err(msg)) return #err(msg);
      };

      switch (Map.get(directory_map, T.thash, asset_name)) {
        case (?#Directory(_)) return #err("Directory already exists with path: " # key);
        case (?#Asset(prev)) {
          Certs.remove_asset_certificates(fs, key, prev, true);
          Asset.deallocate(fs, prev);
        };
        case (null) {};
      };

      Certs.certify_asset(fs, key, asset, null);
      ignore Map.put(directory_map, T.thash, asset_name, #Asset(asset));

      #ok(asset);
    };

  };

  module Upload {

    public func new(region : T.MemoryRegion) : T.Upload {
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
  };

};
