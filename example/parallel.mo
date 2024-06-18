import Buffer "mo:base/Buffer";
import Iter "mo:base/Iter";
import Nat64 "mo:base/Nat64";

actor Echo {

  func square(buffer: Buffer.Buffer<Nat64>) : async (){
    for (i in Iter.range(0, buffer.size() - 1)){
        let n64 = buffer.get(i);

        buffer.put(i,  ((n64 **% n64)));
      };
  };

  // Say the given phase.
  public func increment(n: Nat) : async Nat64 {
    let size = 1_000_000;
    let buffer = Buffer.Buffer<Nat64>(size);

    for (i in Iter.range(1, size)){
      buffer.add(Nat64.fromNat(i));
    };

    // let parallel = Buffer.Buffer<async ()>(n);

    // for (_ in Iter.range(1, n)){
    //   parallel.add(square(buffer));
    // };

    for (i in Iter.range(0, n - 1)){
      await square(buffer);
    };

    var sum : Nat64 = 0;

    for (i in Iter.range(0, size - 1)){
      sum +%= buffer.get(i);
    };

    sum;
  };

  // func batch(start: )
};
