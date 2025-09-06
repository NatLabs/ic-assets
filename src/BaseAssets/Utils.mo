import Result "mo:base@0/Result";
import Debug "mo:base@0/Debug";
import Order "mo:base@0/Order";
import Iter "mo:base@0/Iter";
import Blob "mo:base@0/Blob";
import Prelude "mo:base@0/Prelude";
import Array "mo:base@0/Array";
import Buffer "mo:base@0/Buffer";
import Text "mo:base@0/Text";
import Error "mo:base@0/Error";
import HttpParser "mo:http-parser@0";

import Map "mo:map@9/Map";

import Itertools "mo:itertools@0/Iter";

module {
  type Map<K, V> = Map.Map<K, V>;
  type Iter<A> = Iter.Iter<A>;
  type Result<T, E> = Result.Result<T, E>;
  type Order = Order.Order;

  public func div_ceiling(a : Nat, b : Nat) : Nat {
    (a + (b - 1)) / b;
  };

  public func format_key(key : Text) : Text {
    let url = HttpParser.URL(key, HttpParser.Headers([]));
    let path = url.path.array.vals()
    |> Iter.filter(_, func(x : Text) : Bool { x != "" })
    |> Text.join("/", _);
    "/" # path;
  };

  public func map_get_or_put<K, V>(map : Map<K, V>, hash_util : Map.HashUtils<K>, key : K, default : () -> V) : V {
    switch (Map.get(map, hash_util, key)) {
      case (?encoding) encoding;
      case (_) {
        let val = default();
        ignore Map.put(map, hash_util, key, val);
        val;
      };
    };
  };

  public func assert_result<A>(result : Result<A, Text>) {
    switch (result) {
      case (#ok(_)) return;
      case (#err(errMsg)) Debug.trap(errMsg);
    };
  };

  public func extract_result<A>(result : Result<A, Text>) : A {
    switch (result) {
      case (#ok(a)) return a;
      case (#err(errMsg)) Debug.trap(errMsg);
    };
  };

  public func throw_if_error(result : Result<Any, Text>) : async* () {
    switch (result) {
      case (#ok(_)) return;
      case (#err(errMsg)) throw Error.reject(errMsg);
    };
  };

  // public func extract_or_throw_error<A>(result : Result<A, Text>) : async* A {
  //     switch (result) {
  //         case (#ok(val)) return val;
  //         case (#err(errMsg)) throw Error.reject(errMsg);
  //     };
  // };

  public func send_error<A, B>(result : Result<A, Text>) : Result<B, Text> {
    switch (result) {
      case (#ok(a)) Prelude.unreachable();
      case (#err(errMsg)) #err(errMsg);
    };
  };

  public func reverse_order<A>(fn : (A, A) -> Order) : (A, A) -> Order {
    func(a : A, b : A) : Order {
      switch (fn(a, b)) {
        case (#less) return #greater;
        case (#greater) return #less;
        case (#equal) return #equal;
      };
    };
  };

  public func blob_concat(blobs : [Blob]) : Blob {
    let bytes : Iter<Nat8> = Itertools.flatten(
      Iter.map(
        blobs.vals(),
        func(blob : Blob) : Iter<Nat8> = blob.vals(),
      )
    );

    Blob.fromArray(Iter.toArray(bytes));
  };

  public func append_to_buffer<A>(buffer : Buffer.Buffer<A>, items : Iter.Iter<A>) {
    for (item in items) { buffer.add(item) };
  };

};
