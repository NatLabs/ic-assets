import Debug "mo:base@0/Debug";
import Blob "mo:base@0/Blob";

import CertifiedAssets "mo:certified-assets@0/Stable";
import Map "mo:map@9/Map";
import Itertools "mo:itertools@0/Iter";
import BaseX "mo:base-x-encoder@2";

import T "../Types";
import ErrorMessages "../ErrorMessages";

import Common "Common";

/// Module for certifying assets and their encodings.
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
    // update the status code to '304 Not Modified' and remove the body since the content is not sent
    get_success_endpoint(key_or_alias, encoding, headers_array, is_aliased).status(304).body("");
  };

  /// Builds the headers for the asset, combining the user-defined headers with the necessary headers for the asset's content and encoding.
  ///
  /// #### Additional headers:
  /// - `etag`: The encoding's sha256 hash, used to check if the asset has been modified since the last request.
  /// If the asset has not been modified, the server responds with a 304 status code.
  /// - `content-type`: The asset's content type.
  /// - `cache-control`: The asset's max age, if set.
  /// - `content-encoding`: The asset's encoding, could be `gzip`, `br`, `identity`, `compress` or `deflate`.
  ///
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

  /// Certifies an asset's encoding.
  ///
  /// This function certifies the asset's encoding by creating two endpoints:
  /// - A success endpoint, which creates a certificate for the 200 status code.
  /// - A cache endpoint, which creates a certificate for the 304 status code.
  ///
  /// Additionally, if the asset is aliased, the function also certifies the asset's encoding for each alias.
  public func certify_encoding(fs : T.FileSystem, asset_key : Text, asset : T.Asset, encoding_name : Text) : T.Result<(), Text> {

    let ?encoding = Map.get(asset.encodings, T.thash, encoding_name) else return #err(ErrorMessages.encoding_not_found(asset_key, encoding_name));

    let aliases = if (asset.is_aliased == ?true) Common.get_html_file_aliases(fs, asset_key).vals() else Itertools.empty();

    let key_and_aliases = Itertools.add(aliases, asset_key);

    let headers = build_headers(asset, encoding_name, encoding.sha256);
    let headers_array = Map.toArray(headers);

    for (key_or_alias in key_and_aliases) {

      let success_endpoint = get_success_endpoint(key_or_alias, encoding, headers_array, asset.is_aliased == ?true);
      let cache_endpoint = get_cache_endpoint(key_or_alias, encoding, headers_array, asset.is_aliased == ?true);

      CertifiedAssets.certify(fs.certs, success_endpoint);
      CertifiedAssets.certify(fs.certs, cache_endpoint);

    };

    encoding.certified := true;

    // only used for certification v1
    asset.last_certified_encoding := ?encoding_name;

    #ok();
  };

  /// Certifies either the specified encoding or all encodings of an asset.
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
    };
  };

  public func remove_asset_certificates(fs : T.FileSystem, key : Text, asset : T.Asset, only_aliases : Bool) {

    if (not only_aliases) {
      CertifiedAssets.remove_all(fs.certs, key);
    } else for ((encoding_name, encoding) in Map.entries(asset.encodings)) {
      remove_encoding_certificate(fs, key, asset, encoding_name, encoding, only_aliases);
    };

  };

  public func remove_encoding_certificate(fs : T.FileSystem, asset_key : Text, asset : T.Asset, encoding_name : Text, encoding : T.AssetEncoding, only_aliases : Bool) {

    // verify that `only_aliases is set to true only if the asset is aliased
    // if not, then we are probably calling this function incorrectly
    if (only_aliases) { assert asset.is_aliased == ?true };

    let aliases = if (asset.is_aliased == ?true) Common.get_html_file_aliases(fs, asset_key).vals() else Itertools.empty();
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
