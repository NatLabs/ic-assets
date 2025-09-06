import V0_types "../V0/types";
import V0_1_0_types "../V0_1_0/types";
import V0_2_0_types "../V0_2_0/types";
import V0_3_0_types "../V0_3_0/types";
import V1_0_0_types "types";

import V0_3_0_upgrade "../V0_3_0/upgrade";

module {
  let T = V1_0_0_types;

  public func upgrade_from_v0(v0 : V0_types.StableStore) : V1_0_0_types.StableStore {
    upgrade_from_v0_3_0(
      V0_3_0_upgrade.upgrade_from_v0(v0)
    );
  };

  public func upgrade_from_v0_1_0(v0_1_0 : V0_1_0_types.StableStore) : V1_0_0_types.StableStore {
    upgrade_from_v0_3_0(
      V0_3_0_upgrade.upgrade_from_v0_1_0(v0_1_0)
    );
  };

  public func upgrade_from_v0_2_0(v0_2_0 : V0_2_0_types.StableStore) : V1_0_0_types.StableStore {
    upgrade_from_v0_3_0(
      V0_3_0_upgrade.upgrade_from_v0_2_0(v0_2_0)
    );
  };

  public func upgrade_from_v0_3_0(v0_3_0 : V0_3_0_types.StableStore) : V1_0_0_types.StableStore {
    let updated_streaming_callback : ?T.StreamingCallback = null; // Reset to have them reset
    {

      var canister_id = v0_3_0.canister_id;
      var streaming_callback = updated_streaming_callback;
      shared_region = v0_3_0.shared_region;

      fs = v0_3_0.fs;
      upload = v0_3_0.upload;
      permissions = v0_3_0.permissions;
    };
  };

};
