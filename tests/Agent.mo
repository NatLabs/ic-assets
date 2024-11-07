import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import None "mo:base/None";
import Time "mo:base/Time";
import { test; suite } "mo:test/async";

import BaseAssets "../src/BaseAssets";
import Assets "../src";
import Migrations "../src/Migrations";

import Canister "../src/Canister";

// let canister_id = Principal.fromText("r7inp-6aaaa-aaaaa-aaabq-cai");
// let owner = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
let caller = Principal.fromText("tde7l-3qaaa-aaaah-qansa-cai");

let asset = await Canister.AssetsCanister(#Init({}));
await asset.init();
let authorized = await asset.list_authorized();
let owner = authorized[0];

Debug.print("actor principal: " # debug_show Principal.fromActor(asset));

Debug.print(debug_show (Time.now()));

await suite(
    "BaseAssets Service Test",
    func() : async () {
        await test(
            "permissions",
            func() : async () {

                assert (await asset.list_permitted({ permission = #Prepare })) == [];
                assert (await asset.list_permitted({ permission = #Commit })) == [owner];
                assert (await asset.list_permitted({ permission = #ManagePermissions })) == [];

                await asset.grant_permission({
                    permission = #Commit;
                    to_principal = caller;
                });
                assert (await asset.list_permitted({ permission = #Commit })) == [owner, caller];

                await asset.revoke_permission({
                    permission = #Commit;
                    of_principal = caller;
                });
                assert (await asset.list_permitted({ permission = #Commit })) == [owner];

                await asset.grant_permission({
                    permission = #Prepare;
                    to_principal = caller;
                });
                assert (await asset.list_permitted({ permission = #Prepare })) == [caller];

                await asset.revoke_permission({
                    permission = #Prepare;
                    of_principal = caller;
                });
                assert (await asset.list_permitted({ permission = #Prepare })) == [];

                await asset.grant_permission({
                    permission = #ManagePermissions;
                    to_principal = caller;
                });
                assert (await asset.list_permitted({ permission = #ManagePermissions })) == [caller];

                await asset.revoke_permission({
                    permission = #ManagePermissions;
                    of_principal = caller;
                });

                assert (await asset.list_permitted({ permission = #ManagePermissions })) == [];

            },
        );
        await test(
            "api_version",
            func() : async () {
                assert 1 == (await asset.api_version());
            },
        );
        // await test(
        //     "store and retrieve asset",
        //     func() : async () {
        //         let store_args : BaseAssets.StoreArgs = {
        //             key = "/hello";
        //             content = "Hello, World!";
        //             sha256 = null;
        //             content_type = "text/plain";
        //             content_encoding = "utf-8";
        //             is_aliased = null;
        //         };

        //         await asset.store(store_args);
        //     },
        // );
    },
);
