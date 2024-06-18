/// Cycle-optimized Sha256 variants.
///
/// Features:
///
/// * Algorithms: `sha256`, `sha224`
/// * Input types: `Blob`, `[Nat8]`, `Iter<Nat8>`
/// * Output types: `Blob`

import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Nat64 "mo:base/Nat64";
import Prim "mo:prim";

import StaticSha256 "Static";

module {

  public type Algorithm = StaticSha256.Algorithm;

  public class Digest(algo_ : Algorithm) {
    let state = StaticSha256.Digest(algo_);

    public func algo() : Algorithm = state.algo;

    public func reset() = StaticSha256.reset(state);

    reset();

    var word : Nat16 = 0;
    private func writeByte(val : Nat8) : () = StaticSha256.writeByte(state, val);

    // We must be at a Nat16 boundary, i.e. high must be true
    /*
    private func writeWord(val : Nat32) : () {
      assert (high);
      msg[Nat8.toNat(i_msg)] := nat32To16(val >> 16);
      msg[Nat8.toNat(i_msg +% 1)] := nat32To16(val & 0xffff);
      i_msg +%= 2;
      if (i_msg == 32) {
        process_block();
        i_msg := 0;
        i_block +%= 1;
      };
    };
    */

    private func writePadding() : () = StaticSha256.writePadding(state);

    public func writeIter(iter : { next() : ?Nat8 }) : () = StaticSha256.writeIter(state, iter);

    public func writeArray(arr : [Nat8]) : () = StaticSha256.writeIter(state, arr.vals());
    public func writeBlob(blob : Blob) : () = StaticSha256.writeIter(state, blob.vals());

    public func sum() : Blob = StaticSha256.sum(state);
  }; // class Digest

  // Calculate SHA256 hash digest from [Nat8].
  public func fromArray(algo : Algorithm, arr : [Nat8]) : Blob {
    let digest = Digest(algo);
    digest.writeIter(arr.vals());
    return digest.sum();
  };

  // Calculate SHA2 hash digest from Iter.
  public func fromIter(algo : Algorithm, iter : { next() : ?Nat8 }) : Blob {
    let digest = Digest(algo);
    digest.writeIter(iter);
    return digest.sum();
  };

  /// Calculate the SHA2 hash digest from `Blob`.
  /// Allowed values for `algo` are: `#sha224`, `#256`
  public func fromBlob(algo : Algorithm, b : Blob) : Blob {
    let digest = Digest(algo);
    digest.writeIter(b.vals());
    return digest.sum();
  };
};
