import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import Nat "mo:base/Nat";

import { test; suite } "mo:test";

import AssetUtils "../src/AssetUtils";
import Asset "../src"

suite(
    "AssetUtils tests",
    func() {
        test(
            "binary_search",
            func() {

                let sorted_array = [1, 4, 4, 14, 21, 35];

                assert ?0 == AssetUtils.binary_search(sorted_array, Nat.compare, 1);
                assert ?0 == AssetUtils.binary_search(sorted_array, Nat.compare, 0);

                let search_for_4 = AssetUtils.binary_search(sorted_array, Nat.compare, 4);
                assert ?1 == search_for_4 or ?2 == search_for_4;

                assert ?5 == AssetUtils.binary_search(sorted_array, Nat.compare, 23);
                assert ?5 == AssetUtils.binary_search(sorted_array, Nat.compare, 35);
                assert null == AssetUtils.binary_search(sorted_array, Nat.compare, 36);

            },
        );

        test(
            "get_encoding_chunk",
            func() {
                let encoding = AssetUtils.create_new_asset_encoding();

                encoding.content_chunks := [
                    [0, 1, 2, 3],
                    [4],
                    [5, 6, 7],
                    [],
                    [8, 9],
                ];
                encoding.content_chunks_prefix_sum := [4, 5, 8, 8, 10];

                assert ?"\00\01\02" == AssetUtils.get_encoding_chunk_with_given_size(encoding, 3, 0);
                assert ?"\03\04\05" == AssetUtils.get_encoding_chunk_with_given_size(encoding, 3, 1);
                assert ?"\06\07\08" == AssetUtils.get_encoding_chunk_with_given_size(encoding, 3, 2);
                assert ?"\09" == AssetUtils.get_encoding_chunk_with_given_size(encoding, 3, 3);
                assert null == AssetUtils.get_encoding_chunk_with_given_size(encoding, 3, 4);

            },
        )

    },
);
