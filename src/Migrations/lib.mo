import Blob "mo:base/Blob";
import Debug "mo:base/Debug";

import Map "mo:map/Map";

import V0_types "V0/types";
import V0_upgrade "V0/upgrade";

import V0_1_0_types "V0_1_0/types";
import V0_1_0_upgrade "V0_1_0/upgrade";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

    let { thash } = Map;

    public type VersionedStableStore = {
        #v0 : V0_types.StableStore;
        #v0_1_0 : V0_1_0_types.StableStore;
    };

    public func upgrade(versions : VersionedStableStore) : VersionedStableStore {
        switch (versions) {
            case (#v0(v0)) #v0_1_0(
                V0_1_0_upgrade.upgrade_from_v0(v0)
            );

            case (#v0_1_0(v0_1_0)) { #v0_1_0(v0_1_0) };
        };
    };

    public func get_current_state(asset_versions : VersionedStableStore) : V0_1_0_types.StableStore {
        switch (asset_versions) {
            case (#v0_1_0(stable_store_v0)) { stable_store_v0 };
            case (_) Debug.trap(
                "
                ic-assets: Invalid version of stable store. Please call upgrade() on the stable store.
                    stable assets_sstore = Assets.init_stable_store();
                    assets_sstore := Assets.upgrade(assets_sstore);
                    let assets = Assets.Assets(assets_sstore);
                "
            );
        };
    };

    public func share_version(sstore : V0_1_0_types.StableStore) : VersionedStableStore {
        #v0_1_0(sstore);
    };

};
