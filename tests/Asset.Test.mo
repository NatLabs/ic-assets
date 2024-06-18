// @testmode wasi
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import None "mo:base/None";
import { test; suite } "mo:test";

import BaseAsset "../src/BaseAsset";
import Asset "../src";
import Migrations "../src/Migrations";

suite(
    "BaseAsset tests",
    func() {
        test(
            "true",
            func() {
                assert true;
            },
        );
    },
);

let canister_id = Principal.fromText("r7inp-6aaaa-aaaaa-aaabq-cai");
let owner = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
let caller = Principal.fromText("tde7l-3qaaa-aaaah-qansa-cai");

func init_test_store(caller : Principal) : Migrations.VersionedStableStore {
    let asset = BaseAsset.init_stable_store(owner);

    let test_store : Migrations.StableStoreTestVersion = {
        asset with
        canister_id = canister_id;
        var next_chunk_id = 1;
        var next_batch_id = 1;
        deprecated_field = ();
        configuration = ();
    };

    #_test(test_store);
};

suite(
    "BaseAsset version tests",
    func() {
        test(
            "ensure init_stable_store() and share_version() return the current version",
            func() {

                let asset = BaseAsset.init_stable_store(owner);
                let sharable_asset = BaseAsset.share_version(asset);

                ignore Migrations.get_current_state(sharable_asset); // should not trap

            },
        );

        test(
            "migrate from test version",
            func() {
                let test_store = init_test_store(caller);

                let updated_state = Migrations.migrate(test_store);

                ignore Migrations.get_current_state(updated_state); // should not trap

                let asset = BaseAsset.from_version(test_store); // should not trap
            },
        );
    },
);
