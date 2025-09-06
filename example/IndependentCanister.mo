import Text "mo:base@0/Text";
import Option "mo:base@0/Option";
import Cycles "mo:base@0/ExperimentalCycles";
import Principal "mo:base@0/Principal";
import Debug "mo:base@0/Debug";

import Assets "../src";
import AssetsCanister "../src/Canister";

shared ({ caller = owner }) actor class () = this_canister {

  let canister_id = Principal.fromActor(this_canister);
  stable var assets : AssetsCanister.AssetsCanister = actor ("aaaaa-aa");

  public shared ({ caller }) func create_assets_canister<system>() : async () {
    if (
      Principal.toText(Principal.fromActor(assets)) == "aaaaa-aa"
    ) {
      Cycles.add<system>(100_000_000_000);
      assets := await AssetsCanister.AssetsCanister(
        #Upgrade({
          set_permissions = ?({
            prepare = [];
            commit = [owner, canister_id];
            manage_permissions = [];
          });
        })
      );
    };
  };

  public shared func store_hello_file() : async () {
    let args : Assets.StoreArgs = {
      key = "/assets/hello.txt";
      content_type = "text/plain";
      content = "Hello, World!";
      sha256 = null;
      content_encoding = "identity";
      is_aliased = ?true;
    };

    await assets.store(args);

    let file = await assets.get({
      key = "/assets/hello.txt";
      accept_encodings = ["identity"];
    });

    assert file.content == "Hello, World!";
    assert file.content_type == "text/plain";
    assert file.content_encoding == "identity";
    assert file.total_length == 13;
    assert Option.isSome(file.sha256);
  };

  public query func get_assets_canister_id() : async (Text) {
    Principal.toText(Principal.fromActor(assets));
  };

};
