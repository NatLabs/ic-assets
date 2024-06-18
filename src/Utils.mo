import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Order "mo:base/Order";
import Iter "mo:base/Iter";
import Blob "mo:base/Blob";
import Prelude "mo:base/Prelude";

import Map "mo:map/Map";

import Itertools "mo:itertools/Iter";

module {
    type Map<K, V> = Map.Map<K, V>;
    type Iter<A> = Iter.Iter<A>;
    type Result<T, E> = Result.Result<T, E>;
    type Order = Order.Order;

    public func map_get_or_put<K, V>(map: Map<K, V>, hash_util: Map.HashUtils<K>, key: K, default: () ->V): V {
        switch(Map.get(map, hash_util, key)) {
            case (?encoding) encoding;
            case (_) {
                let val = default();
                ignore Map.put(map, hash_util, key, val);
                val;
            };
        };
    };

    public func assert_result<A>(result: Result<A, Text>){
        switch(result){
            case (#ok(_)) return;
            case (#err(errMsg)) Debug.trap(errMsg);
        }
    };

    public func extract_result<A>(result: Result<A, Text>): A {
        switch(result){
            case (#ok(a)) return a;
            case (#err(errMsg)) Debug.trap(errMsg);
        }
    };

    public func send_error<A, B>(result: Result<A, Text>): Result<B, Text> {
        switch(result){
            case (#ok(a)) Prelude.unreachable();
            case (#err(errMsg)) #err(errMsg);
        }
    };

    public func reverse_order<A>(fn: (A, A) -> Order): (A, A) -> Order {
        func (a: A, b: A) : Order {
            switch(fn(a, b)){
                case (#less) return #greater;
                case (#greater) return #less;
                case (#equal) return #equal;
            }
        }
    };

    public func blob_concat(blobs: [Blob]): Blob {
        let bytes : Iter<Nat8> = Itertools.flatten(
            Iter.map(
                blobs.vals(),
                func (blob: Blob): Iter<Nat8> = blob.vals()
            )
        );


        Blob.fromArray(Iter.toArray(bytes))
    };
}