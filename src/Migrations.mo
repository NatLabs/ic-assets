import Debug "mo:base/Debug";
import Map "mo:map/Map";
import T "Types";

module {
    public type StableStoreTestVersion = T.StableStoreTestVersion;
    public type StableStoreV0 = T.StableStoreV0;
    public type VersionedStableStore = T.VersionedStableStore;

    public func migrate(versions : T.VersionedStableStore) : T.VersionedStableStore {
        switch (versions) {
            case (#_test(test_store)) migrate_from_test_version(test_store) |> #v0(_);
            case (#v0(_)) versions;
        };
    };

    public func get_current_state(asset_versions : T.VersionedStableStore) : StableStoreV0 {
        switch (asset_versions) {
            case (#v0(stable_store_v0)) { stable_store_v0 };
            case (_) Debug.trap("Invalid version of stable store. Please call migrate() first.");
        };
    };

    func migrate_from_test_version(test_version : T.StableStoreTestVersion) : T.StableStoreV0 {
        let curr : StableStoreV0 = {
            var canister_id = ?test_version.canister_id;
            var streaming_callback = null;
            assets = test_version.assets;
            certificate_store = test_version.certificate_store;
            chunks = Map.new();
            batches = test_version.batches;
            copy_on_write_batches = Map.new();

            commit_principals = test_version.commit_principals;
            prepare_principals = test_version.prepare_principals;
            manage_permissions_principals = test_version.manage_permissions_principals;

            var next_chunk_id = 1;
            var next_batch_id = 1;

            configuration = {
                var max_batches = null;
                var max_chunks = null;
                var max_bytes = null;
            };

            var fallback_page = "";
        };
    };
};
