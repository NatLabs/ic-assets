import Nat "mo:base@0/Nat";
import Time "mo:base@0/Time";

import MemoryRegion "mo:memory-region@1/MemoryRegion";

import T "../Types";
import Utils "../Utils";
import Const "../Const";

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

  public func replace_content(fs : T.FileSystem, encoding : T.AssetEncoding, content : Blob, content_hash : Blob) {
    let chunks_iter = [content].vals();
    replace_content_via_chunks(fs, encoding, chunks_iter, content.size(), content_hash);
  };

  public func get_content(fs : T.FileSystem, encoding : T.AssetEncoding) : Blob {
    MemoryRegion.loadBlob(fs.region, encoding.content_pointer.0, encoding.content_pointer.1);
  };

  /// Returns the total number of chunks stored in the encoding
  public func get_chunks_size(encoding : T.AssetEncoding) : Nat {
    Utils.div_ceiling(encoding.content_pointer.1, Const.MAX_CHUNK_SIZE);
  };

  public func get_chunk(fs : T.FileSystem, encoding : T.AssetEncoding, chunk_index : Nat) : ?Blob {
    let content_address = encoding.content_pointer.0;
    let content_size = encoding.content_pointer.1;

    let num_chunks = Utils.div_ceiling(content_size, Const.MAX_CHUNK_SIZE);

    if (chunk_index >= num_chunks) return null;

    let chunk_offset = chunk_index * Const.MAX_CHUNK_SIZE;
    let chunk_size = Nat.min(Const.MAX_CHUNK_SIZE, content_size - chunk_offset);

    ?MemoryRegion.loadBlob(fs.region, content_address + chunk_offset, chunk_size);

  };

};
