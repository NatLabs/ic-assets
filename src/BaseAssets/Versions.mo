import V0_types "../Migrations/V0/types";
import V0_1_0_types "../Migrations/V0_1_0/types";
import V0_2_0_types "../Migrations/V0_2_0/types";
import V0_3_0_types "../Migrations/V0_3_0/types";
import V1_0_0_types "../Migrations/V1_0_0/types";

module {
  public type VersionedStableStore = {
    #v0 : V0_types.StableStore;
    #v0_1_0 : V0_1_0_types.StableStore;
    #v0_2_0 : V0_2_0_types.StableStore;
    #v0_3_0 : V0_3_0_types.StableStore;
    #v1_0_0 : V1_0_0_types.StableStore;
  };
};
