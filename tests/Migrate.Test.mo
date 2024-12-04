import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";

import { test; suite } "mo:test";
import Sha256 "mo:sha2/Sha256";
import Vector "mo:vector";
import Map "mo:map/Map";

import BaseAssets "../src/BaseAssets";
import Assets "../src/";
import Migrations "../src/Migrations";

import V0_types "../src/Migrations/V0/types";
import V0_upgrade "../src/Migrations/V0/upgrade";

import V0_1_0_types "../src/Migrations/V0_1_0/types";
import V0_1_0_upgrade "../src/Migrations/V0_1_0/upgrade";

let { thash; nhash } = Map;

let canister_id = Principal.fromText("r7inp-6aaaa-aaaaa-aaabq-cai");
let owner = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
let caller = Principal.fromText("tde7l-3qaaa-aaaah-qansa-cai");

suite(
    "Assets version tests",
    func() {
        test(
            "ensure init_stable_store() returns the current version",
            func() {

                let asset = Assets.init_stable_store(owner);

                switch (asset) {
                    // current version
                    case (#v0_1_0(internal_state)) assert true;
                    case (_) assert false;
                };

                let internal_state = Migrations.get_current_state(asset); // should not if the version matches the current version

            },
        );

        test(
            "upgrade from version #v0 to current version",
            func() {
                let v0_internal_asset = V0_upgrade.init_stable_store(caller);

                ignore Map.put<Nat, V0_types.Batch>(
                    v0_internal_asset.batches,
                    nhash,
                    0,
                    {
                        var expires_at = 0;
                        var commit_batch_arguments = null;
                        var evidence_computation = null;
                        var chunk_content_total_size = 5;
                    },
                );

                assert Map.size(v0_internal_asset.batches) == 1;

                ignore Map.put<Nat, V0_types.Chunk>(
                    v0_internal_asset.chunks,
                    nhash,
                    0,
                    {
                        batch_id = 0;
                        content : Blob = "hello";
                    },
                );

                assert Map.size(v0_internal_asset.chunks) == 1;

                ignore Map.put<Text, V0_types.Assets>(
                    v0_internal_asset.assets,
                    thash,
                    "/file.txt",
                    {
                        var content_type = "text/plain";
                        var is_aliased = ?true;
                        var max_age = null;
                        var allow_raw_access = ?false;
                        headers = Map.fromIter<Text, Text>(
                            [
                                ("content-length", "5"),
                            ].vals(),
                            thash,
                        );
                        encodings = Map.fromIter<Text, V0_types.AssetEncoding>(
                            [("identity", { var modified = 1733296976171; var content_chunks = Vector.fromArray<Blob>([Blob.fromArray([1, 2, 3, 4, 5])]); var total_length = 5; var certified = false; var sha256 = Sha256.fromArray(#sha256, [0, 1, 2, 3, 4, 5]) } : V0_types.AssetEncoding)].vals(),
                            thash,
                        );
                    },
                );

                assert Map.size(v0_internal_asset.assets) == 1;

                let v0_asset = #v0(v0_internal_asset);

                let v0_1_0_asset = Migrations.upgrade(v0_asset);

                switch (v0_1_0_asset) {
                    // current version
                    case (#v0_1_0(internal_state)) assert true;
                    case (_) assert false;
                };

                let internal_asset = Migrations.get_current_state(v0_1_0_asset); // should not if the version matches the current version

                // clears batches and chunks on upgrade
                assert Map.size(internal_asset.batches) == 0;
                assert Map.size(internal_asset.chunks) == 0;

                let ?file_txt = Map.get<Text, V0_1_0_types.Assets>(internal_asset.assets, thash, "/file.txt") else return assert false;

                assert file_txt.content_type == "text/plain";
                assert file_txt.is_aliased == ?true;
                assert file_txt.max_age == null;
                assert file_txt.allow_raw_access == ?false;
                assert Map.get(file_txt.headers, thash, "content-length") == ?"5";

                let ?file_txt_identity_encoding = Map.get<Text, V0_1_0_types.AssetEncoding>(file_txt.encodings, thash, "identity") else return assert false;

                assert file_txt_identity_encoding.modified == 1733296976171;
                assert file_txt_identity_encoding.total_length == 5;
                assert file_txt_identity_encoding.certified == false;
                assert file_txt_identity_encoding.sha256 == Sha256.fromArray(#sha256, [0, 1, 2, 3, 4, 5]);

                // fields updated or added in #v0_1_0
                assert file_txt_identity_encoding.content_chunks == [[1, 2, 3, 4, 5]];
                assert file_txt_identity_encoding.content_chunks_prefix_sum == [5];

            },
        );
    },
);
