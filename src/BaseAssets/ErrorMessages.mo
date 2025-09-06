import T "Types";

module ErrorMessages {
  public func asset_not_found(key : T.Key) : Text {
    "Asset not found for path: " # debug_show key;
  };

  public func encoding_not_found(asset_key : T.Key, encoding_name : Text) : Text {
    "Encoding not found for asset " # debug_show asset_key # " with encoding " # encoding_name;
  };

  public func batch_not_found(batch_id : Nat) : Text {
    "Batch not found with id " # debug_show batch_id;
  };

  public func sha256_hash_mismatch(provided_hash : Blob, computed_hash : Blob) : Text {
    "Provided hash does not match computed hash: " # debug_show ({
      provided_hash;
      computed_hash;
    });
  };

  public func chunk_not_found(chunk_id : Nat) : Text {
    "Chunk not found with id " # debug_show chunk_id;
  };

  public func missing_permission(permission : Text) : Text {
    "Caller does not have " # debug_show permission # " permission";
  };

};
