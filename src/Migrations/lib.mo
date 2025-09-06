import Blob "mo:base@0/Blob";
import Debug "mo:base@0/Debug";

import Map "mo:map@9/Map";

import V0_types "V0/types";
import V0_1_0_types "V0_1_0/types";
import V0_2_0_types "V0_2_0/types";
import V0_3_0_types "V0_3_0/types";
import V1_0_0_types "V1_0_0/types";

import V1_0_0_upgrade "V1_0_0/upgrade";

// the versions are seperated into the types and methods directories to prevent circular dependencies
module {

  let { thash } = Map;

  public type VersionedStableStore = {
    #v0 : V0_types.StableStore;
    #v0_1_0 : V0_1_0_types.StableStore;
    #v0_2_0 : V0_2_0_types.StableStore;
    #v0_3_0 : V0_3_0_types.StableStore;
    #v1_0_0 : V1_0_0_types.StableStore;
  };

  type CurrentStableStore = V1_0_0_types.StableStore;

  public func upgrade_stable_store(versions : VersionedStableStore) : VersionedStableStore {
    switch (versions) {
      case (#v0(v0)) #v1_0_0(V1_0_0_upgrade.upgrade_from_v0(v0));
      case (#v0_1_0(v0_1_0)) {
        #v1_0_0(V1_0_0_upgrade.upgrade_from_v0_1_0(v0_1_0));
      };
      case (#v0_2_0(v0_2_0)) {
        #v1_0_0(
          V1_0_0_upgrade.upgrade_from_v0_2_0(v0_2_0)
        );
      };
      case (#v0_3_0(v0_3_0)) {
        #v1_0_0(V1_0_0_upgrade.upgrade_from_v0_3_0(v0_3_0));
      };
      case (#v1_0_0(v1_0_0)) {
        #v1_0_0(v1_0_0);
      };
    };
  };

  public func get_current_state(asset_versions : VersionedStableStore) : CurrentStableStore {
    switch (asset_versions) {
      case (#v1_0_0(stable_store)) { stable_store };
      case (_) Debug.trap(
        "
                ic-assets: Invalid version of stable store. Please call upgrade_stable_store() on the stable store.
                    stable assets_sstore = Assets.init_stable_store();
                    assets_sstore := Assets.upgrade_stable_store(assets_sstore);
                    let assets = Assets.Assets(assets_sstore, null);
                "
      );
    };
  };

  public func share_version(sstore : CurrentStableStore) : VersionedStableStore {
    #v1_0_0(sstore);
  };

};
