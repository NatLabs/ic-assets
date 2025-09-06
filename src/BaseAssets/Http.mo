import Array "mo:base@0/Array";
import Text "mo:base@0/Text";
import Blob "mo:base@0/Blob";
import Option "mo:base@0/Option";
import Debug "mo:base@0/Debug";
import Buffer "mo:base@0/Buffer";
import Principal "mo:base@0/Principal";

import Map "mo:map@9/Map";
import BaseX "mo:base-x-encoder@2";
import CertifiedAssets "mo:certified-assets@0/Stable";
import HttpParser "mo:http-parser@0";

import T "Types";
import Utils "Utils";
import FileSystem "FileSystem";
import ErrorMessages "ErrorMessages";
import Encoding "FileSystem/Encoding";
import Certs "FileSystem/Certs";

module {

  public func build_ok_response(
    self : T.StableStore,
    key : T.Key,
    asset : T.Asset,
    encoding_name : Text,
    encoding : T.AssetEncoding,
    chunk_index : Nat,
    etags : [Text],
    http_req : T.HttpRequest,
    opt_fallback_key : ?T.Key,
  ) : T.Result<T.HttpResponse, Text> {

    let headers = Certs.build_headers(asset, encoding_name, encoding.sha256);
    let next_token : T.CustomStreamingToken = {
      key;
      content_encoding = encoding_name;
      index = chunk_index + 1;
      sha256 = ?encoding.sha256;
    };

    let ?callback : ?T.StreamingCallback = self.streaming_callback else return #err("Streaming callback not set");
    let streaming_strategy : T.StreamingStrategy = #Callback({
      token = to_candid (next_token);
      callback;
    });

    let contains_hash = Option.isSome(
      Array.find(
        etags,
        func(etag : Text) : Bool {
          let unwrapped_etag = Text.replace(etag, #text("\""), "");

          let #ok(etag_bytes) = BaseX.fromHex(unwrapped_etag, { prefix = #none }) else return false;
          Blob.fromArray(etag_bytes) == encoding.sha256;
        },
      )
    );

    let (status_code, body, opt_body_hash) : (Nat16, Blob, ?Blob) = if (contains_hash) {
      (304, "", null);
    } else {
      let content_chunk : Blob = switch (Encoding.get_chunk(self.fs, encoding, chunk_index)) {
        case (?content) content;
        case (null) "";
      };

      assert content_chunk.size() <= 2 * (1024 ** 2);
      (200, content_chunk, ?encoding.sha256);
    };

    let headers_buffer = Buffer.Buffer<(Text, Text)>(Map.size(headers));
    for ((key, value) in Map.entries(headers)) {
      headers_buffer.add((key, value));
    };

    let http_res = {
      status_code;
      headers = Buffer.toArray(headers_buffer);
      body;
      upgrade = null;
      streaming_strategy = null;
    };

    let certified_headers_result = switch (opt_fallback_key) {
      case (?fallback_key) CertifiedAssets.get_fallback_certificate(
        self.fs.certs,
        http_req,
        fallback_key,
        http_res,
        opt_body_hash,
      );
      case (null) CertifiedAssets.get_certificate(
        self.fs.certs,
        http_req,
        http_res,
        opt_body_hash,
      );
    };

    switch (certified_headers_result) {
      case (#ok(certified_headers)) {

        for ((key, value) in certified_headers.vals()) {
          headers_buffer.add((key, value));
        };

        let num_chunks = Encoding.get_chunks_size(encoding);

        let certified_res : T.HttpResponse = {
          http_res with headers = Buffer.toArray(headers_buffer);
          streaming_strategy = if (num_chunks > 1 and status_code != 304) ?streaming_strategy else null;
        };

        return #ok(certified_res);
      };
      case (#err(err_msg)) return #err("CertifiedAssets.get_certificate failed: " # err_msg # "\n" # debug_show { http_req; http_res = { http_res with streaming_strategy = null } });
    };
  };

  let ENCODING_CERTIFICATION_ORDER : [Text] = ["identity", "gzip", "compress", "deflate", "br"];

  public func encoding_order(accept_encodings : [Text]) : [Text] {
    Array.sort(
      accept_encodings,
      func(a : Text, b : Text) : T.Order {
        let a_index = Array.indexOf(a, ENCODING_CERTIFICATION_ORDER, Text.equal);
        let b_index = Array.indexOf(b, ENCODING_CERTIFICATION_ORDER, Text.equal);

        switch (a_index, b_index) {
          case (?a_index, ?b_index) {
            if (a_index < b_index) return #less;
            if (a_index > b_index) return #greater;
            return #equal;
          };
          case (_, ?_) #greater;
          case (?_, _) #less;
          case (_, _) #equal;
        };
      },
    );
  };

  public func redirect_to_certified_domain(self : T.StableStore, url : T.URL) : T.HttpResponse {
    let canister_id = self.canister_id;

    let path = url.path.original;
    let domain = url.host.original;
    let location = if (Text.contains(domain, #text("ic0.app"))) {
      "https://" # Principal.toText(canister_id) # ".ic0.app" # path;
    } else {
      "https://" # Principal.toText(canister_id) # ".icp0.io" # path;
    };

    return {
      status_code = 308; // Permanent Redirect
      headers = [("Location", location)];
      body = "";
      upgrade = null;
      streaming_strategy = null;
    };
  };

  public func build_http_response(self : T.StableStore, _req : T.HttpRequest, url : T.URL, encodings : [Text]) : T.Result<T.HttpResponse, Text> {
    let path = url.path.original;
    var key = Utils.format_key(path);
    var opt_fallback_key : ?Text = null;
    var req = _req;

    let cert_version : Nat16 = switch (req.certificate_version) {
      case (?v) v;
      case (_) 2;
    };

    let asset = switch (FileSystem.get_asset_using_aliases(self.fs, key, true)) {
      case (#ok(?asset)) {
        if ((not Option.get(asset.allow_raw_access, true)) and Text.contains(url.host.original, #text "raw.ic")) {
          return redirect_to_certified_domain(self, url) |> #ok(_);
        };

        asset;
      };
      case (#ok(null) or #err(_)) switch (FileSystem.get_fallback_asset(self.fs, key)) {
        case (?(fallback_key, asset)) {
          if ((not Option.get(asset.allow_raw_access, true)) and Text.contains(url.host.original, #text "raw.ic")) {
            return redirect_to_certified_domain(self, url) |> #ok(_);
          };

          opt_fallback_key := ?fallback_key;

          asset;
        };
        case (null) return #err(ErrorMessages.asset_not_found(path));
      };
    };

    let etag_value = Array.find(
      req.headers,
      func(header : (Text, Text)) : Bool {
        header.0 == "if-none-match";
      },
    );

    let etag_values = switch (etag_value) {
      case (?(field, val)) [val];
      case (_) [];
    };

    if (cert_version == 1) {
      switch (asset.last_certified_encoding) {
        case (?encoding_name) {
          let ?encoding = Map.get(asset.encodings, T.thash, encoding_name) else return #err("Asset.http_request(): asset.last_certified_encoding not found in asset.encodings");
          return (build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req, opt_fallback_key));
        };
        case (null) {};
      };
    };

    let ordered_encodings = encoding_order(encodings);
    label search_for_matching_encoding for (encoding_name in ordered_encodings.vals()) {
      let ?encoding = Map.get(asset.encodings, T.thash, encoding_name) else continue search_for_matching_encoding;
      return (build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req, opt_fallback_key));
    };

    label search_for_encoding_in_default_list for (encoding_name in ENCODING_CERTIFICATION_ORDER.vals()) {
      let ?encoding = Map.get(asset.encodings, T.thash, encoding_name) else continue search_for_encoding_in_default_list;
      return (build_ok_response(self, path, asset, encoding_name, encoding, 0, etag_values, req, opt_fallback_key));
    };

    #err("No encoding found for " # debug_show url.path.original);

  };

  public func http_request_streaming_callback(self : T.StableStore, rawToken : T.StreamingToken) : T.Result<T.StreamingCallbackResponse, Text> {
    let ?t : ?T.CustomStreamingToken = from_candid (rawToken) else return #err("http_request_streaming_callback(): Invalid token");
    let token : T.CustomStreamingToken = {
      t with key = Utils.format_key(t.key)
    };

    let asset = switch (FileSystem.get_asset_using_aliases(self.fs, token.key, true)) {
      case (#ok(?asset)) asset;
      case (#ok(null)) return #err(ErrorMessages.asset_not_found(token.key));
      case (#err(msg)) return #err("http_request_streaming_callback(): " # msg);
    };

    let ?encoding = Map.get(asset.encodings, T.thash, token.content_encoding) else return #err(
      ErrorMessages.encoding_not_found(token.key, token.content_encoding)
    );

    if (?encoding.sha256 != token.sha256) return #err(
      ErrorMessages.sha256_hash_mismatch(
        Option.get(token.sha256, "" : Blob),
        encoding.sha256,
      )
    );

    let num_chunks = Encoding.get_chunks_size(encoding);

    let chunk : Blob = switch (Encoding.get_chunk(self.fs, encoding, token.index)) {
      case (?chunk) chunk;
      case (null) "";
    };

    let next_token : T.CustomStreamingToken = {
      key = token.key;
      content_encoding = token.content_encoding;
      index = token.index + 1;
      sha256 = ?encoding.sha256;
    };

    let response : T.StreamingCallbackResponse = {
      body = chunk;
      token = if (next_token.index < num_chunks) ?to_candid (next_token) else (null);
    };

    #ok(response);

  };

  public func process_http_request(self : T.StableStore, req : T.HttpRequest) : T.Result<T.HttpResponse, Text> {
    let headers = HttpParser.Headers(req.headers);
    let content_encoding = switch (headers.get("content-encoding")) {
      case (?encoding) { encoding };
      case (null) { ["identity"] };
    };

    let url = HttpParser.URL(req.url, headers);

    build_http_response(self, req, url, content_encoding);

  };

};
