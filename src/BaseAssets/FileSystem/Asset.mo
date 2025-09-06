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

import Map "mo:map@9/Map";
import MemoryRegion "mo:memory-region@1/MemoryRegion";

import T "../Types";
import ErrorMessages "../ErrorMessages";
import Utils "../Utils";

import Certs "Certs";
import Encoding "Encoding";
import Common "Common";

/// Module for asset level operations.
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

  /// Returns a deep clone of the asset and its content.
  public func clone(fs : T.FileSystem, asset : T.Asset) : T.Asset {

    let new_asset = Asset.new();

    new_asset.content_type := asset.content_type;
    new_asset.is_aliased := asset.is_aliased;
    new_asset.max_age := asset.max_age;
    new_asset.allow_raw_access := asset.allow_raw_access;

    for ((key, value) in Map.entries(asset.headers)) {
      ignore Map.put(new_asset.headers, T.thash, key, value);
    };

    for ((key, encoding) in Map.entries(asset.encodings)) {
      let new_encoding = Encoding.new();

      new_encoding.modified := encoding.modified;
      new_encoding.total_length := encoding.total_length;
      new_encoding.sha256 := encoding.sha256;

      // clone encoding content
      let encoding_content = Encoding.get_content(fs, encoding);
      Encoding.replace_content(fs, new_encoding, encoding_content, encoding.sha256);

      ignore Map.put(new_asset.encodings, T.thash, key, new_encoding);
    };

    new_asset;

  };

  /// Returns a deep copy of the asset record with a reference to the existing content.
  public func copy(asset : T.Asset) : T.Asset {
    let new_asset = Asset.new();

    new_asset.content_type := asset.content_type;
    new_asset.is_aliased := asset.is_aliased;
    new_asset.max_age := asset.max_age;
    new_asset.allow_raw_access := asset.allow_raw_access;

    for ((key, value) in Map.entries(asset.headers)) {
      ignore Map.put(new_asset.headers, T.thash, key, value);
    };

    for ((key, encoding) in Map.entries(asset.encodings)) {
      let new_encoding = Encoding.new();

      new_encoding.modified := encoding.modified;
      new_encoding.total_length := encoding.total_length;
      new_encoding.sha256 := encoding.sha256;
      new_encoding.content_pointer := encoding.content_pointer;

      ignore Map.put(new_asset.encodings, T.thash, key, new_encoding);
    };

    new_asset;
  };

  /// Deallocates the content in each asset encoding
  public func deallocate(fs : T.FileSystem, self : T.Asset) {
    for ((_, encoding) in Map.entries(self.encodings)) {
      Encoding.deallocate_content(fs, encoding);
    };
  };

  public func get_details(key : Text, asset : T.Asset) : T.AssetDetails {
    let encodings = Array.map(
      Map.toArray(asset.encodings),
      func((encoding_name, encoding) : (Text, T.AssetEncoding)) : T.AssetEncodingDetails {

        let encoding_details : T.AssetEncodingDetails = {
          content_encoding = encoding_name;
          modified = encoding.modified;
          length = encoding.total_length;
          sha256 = ?encoding.sha256;
        };

        encoding_details;
      },
    );

    let asset_details : T.AssetDetails = {
      key;
      content_type = asset.content_type;
      encodings = encodings;
    };
  };

  public func create_encoding(
    fs : T.FileSystem,
    asset : T.Asset,
    encoding_name : Text,
    content : Blob,
    content_hash : Blob,
  ) : ?T.AssetEncoding {

    let encoding = Encoding.new();
    Encoding.replace_content(fs, encoding, content, content_hash);

    let opt_prev = Map.put(asset.encodings, T.thash, encoding_name, encoding);
    opt_prev;

  };

  public func get_encoding(fs : T.FileSystem, asset : T.Asset, encoding_name : Text) : ?T.AssetEncoding {
    Map.get(asset.encodings, T.thash, encoding_name);
  };

  public func remove_encoding(
    fs : T.FileSystem,
    asset_key : Text,
    asset : T.Asset,
    content_encoding : Text,
  ) : T.Result<T.AssetEncoding, Text> {
    let ?encoding = Map.get(asset.encodings, T.thash, content_encoding) else return #err(
      ErrorMessages.encoding_not_found(asset_key, content_encoding)
    );

    Certs.remove_encoding_certificate(fs, asset_key, asset, content_encoding, encoding, false);
    Encoding.deallocate_content(fs, encoding);
    ignore Map.remove(asset.encodings, T.thash, content_encoding);

    #ok(encoding);
  };

  public func get_properties(asset : T.Asset) : T.AssetProperties {
    {
      is_aliased = asset.is_aliased;
      max_age = asset.max_age;
      allow_raw_access = asset.allow_raw_access;
      headers = if (Map.size(asset.headers) == 0) { null } else {
        ?Iter.toArray(Map.entries(asset.headers));
      };
    };
  };

  public func set_max_age(asset : T.Asset, opt_max_age : ??Nat64) {
    switch (opt_max_age) {
      case (?max_age) asset.max_age := max_age;
      case (_) {};
    };
  };

  public func set_allow_raw_access(asset : T.Asset, allow_raw_access : ??Bool) {
    switch (allow_raw_access) {
      case (?allow_raw_access) asset.allow_raw_access := allow_raw_access;
      case (_) {};
    };
  };

  public func set_headers(asset : T.Asset, headers : ??[(Text, Text)]) {
    switch (headers) {
      case (??headers) {
        Map.clear(asset.headers);

        for ((key, value) in headers.vals()) {
          ignore Map.put(asset.headers, T.thash, key, value);
        };
      };
      case (?null) Map.clear(asset.headers);
      case (_) {};
    };
  };

  /// Merges two assets together, overwriting the properties of the first asset with the properties of the second asset.
  /// Encodings in the first asset that are not present in the second asset are preserved.
  public func merge(asset : T.Asset, to_merge : T.Asset) {
    asset.content_type := to_merge.content_type;
    asset.is_aliased := to_merge.is_aliased;
    asset.max_age := to_merge.max_age;
    asset.allow_raw_access := to_merge.allow_raw_access;

    for ((key, value) in Map.entries(to_merge.headers)) {
      ignore Map.put(asset.headers, T.thash, key, value);
    };
  };

};
