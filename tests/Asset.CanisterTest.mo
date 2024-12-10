import Array "mo:base/Array";
import Blob "mo:base/Blob";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Option "mo:base/Option";
import Principal "mo:base/Principal";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Nat "mo:base/Nat";

import Fuzz "mo:fuzz";
import IC "mo:ic";
import Itertools "mo:itertools/Iter";
import Map "mo:map/Map";
import Vector "mo:vector";
import { test; suite } "mo:test/async";
import CertifiedAssets "mo:certified-assets/Stable";

import Assets "../src";
import CanisterTests "CanisterTests/tools";
import Sha256 "mo:sha2/Sha256";

shared ({ caller = owner }) actor class () = this_canister {

    type Buffer<A> = Buffer.Buffer<A>;
    type Result<A, B> = Result.Result<A, B>;
    type Map<K, V> = Map.Map<K, V>;
    let { nhash ; thash } = Map;

    let { ic } = IC;
    let { exists_in } = CanisterTests;
    let canister_id = Principal.fromActor(this_canister);

    // Set up the assets library
    let assets_sstore = Assets.init_stable_store(canister_id, owner);
    let #v0_1_0(assets_internal) = assets_sstore;

    let assets = Assets.Assets(assets_sstore);
    assets.set_canister_id(canister_id);

    public query func http_request_streaming_callback(
        token : Assets.StreamingToken
    ) : async Assets.StreamingCallbackResponse {
        assets.http_request_streaming_callback(token);
    };

    assets.set_streaming_callback(http_request_streaming_callback);

    // Set up test suite
    let suite = CanisterTests.Suite();
    let fuzz = Fuzz.fromSeed(0x4321feed);

    // for running tests
    public query func run_query_test(test_name: Text) : async CanisterTests.TestResult { suite.run_query(test_name).0; };

    public func run_test(test_name: Text) : async CanisterTests.TestResult { (await suite.run(test_name)).0; };

    public func get_test_details() : async [CanisterTests.TestDetails] { suite.get_test_details().0; };

    public func get_test_result(test_name: Text) : async CanisterTests.TestResult { suite.get_test_result(test_name).0; };

    public func get_finished_test_results() : async [CanisterTests.TestResult] { suite.get_finished_test_results().0 };

    func get_controllers() : async* [Principal] {

        let { controllers } = await ic.canister_info({
            canister_id;
            num_requested_changes = ?0;
        });

        controllers;
    };

    func add_controller(principal : Principal) : async* () {
        let controllers = await* get_controllers();

        await ic.update_settings({
            canister_id;
            sender_canister_version = null;
            settings = {
                controllers = ?Array.append(controllers, [principal]);
                compute_allocation = null;
                memory_allocation = null;
                freezing_threshold = null;
                reserved_cycles_limit = null;
                log_visibility = null;
                wasm_memory_limit = null;
            };
        });
    };

    func remove_controller(principal : Principal) : async* () {
        let controllers = await* get_controllers();

        let filtered_controllers = Array.filter(
            controllers,
            func(c : Principal) : Bool = not Principal.equal(c, principal),
        );

        await ic.update_settings({
            canister_id;
            sender_canister_version = null;
            settings = {
                controllers = ?filtered_controllers;
                compute_allocation = null;
                memory_allocation = null;
                freezing_threshold = null;
                reserved_cycles_limit = null;
                log_visibility = null;
                wasm_memory_limit = null;
            };
        });
    };

    let MB = 1048576;
    // setting to constant values to stables saves time during updates
    // however if you want to update these values, you would need to remove the stable keyword
    stable let chunk_1mb_bytes = Array.tabulate(MB, func(i : Nat) : Nat8 = Nat8.fromNat(i % 256));
    stable let chunk_1mb_2_bytes = Array.tabulate(MB, func(i : Nat) : Nat8 = Nat8.fromNat(255 - ((i) % 256)));
    stable let chunk_2mb_bytes = Array.tabulate(2 * MB, func(i : Nat) : Nat8 = Nat8.fromNat(i % 256));

    stable let chunk_1mb_blob = Blob.fromArray(chunk_1mb_bytes);
    stable let chunk_1mb_2_blob = Blob.fromArray(chunk_1mb_2_bytes);
    stable let chunk_2mb_blob = Blob.fromArray(chunk_2mb_bytes);

    let random_principal = Principal.fromText("u5jjd-z7cou-uect2-4m23z-esfhk-x74s4-rxsjn-3nedq-v3vx5-3dz7m-7o6");
    let preparer = Principal.fromText("rkmf3-ufuh3-7k7pl-byxhv-mjgqa-dusgu-3nt2f-nssrd-hanos-a3q2h-2to");
    let committer = Principal.fromText("6kxko-6ftza-wm5wx-wvl4x-2piqy-xb3z2-uhgf2-diniq-qevhm-leqze-a3s");
    let manager = Principal.fromText("6fwxe-5lwf3-afmwn-e36v4-idfv6-7ljas-d6kyg-v3hre-hlbtw-ziocy-mn4");
    let controller = Principal.fromText("ino5l-e2w3m-fkugj-hus7l-impv6-v3jj3-4midm-wpgvl-5ywuy-3gcma-mfq");
    let no_permission = Principal.fromText("3knoz-wr3c6-5zb7t-p42da-3swyg-d4fiy-qs63x-vfnrc-674z2-s6syn-uky");

    func get_principal_tag(p : Principal) : Text {
        if (p == owner) {
            return "owner";
        } else if (p == preparer) {
            return "preparer";
        } else if (p == committer) {
            return "committer";
        } else if (p == manager) {
            return "manager";
        } else if (p == controller) {
            return "controller";
        } else if (p == no_permission) {
            return "no_permission";
        } else if (p == canister_id) {
            return "canister_id";
        } else if (p == random_principal) {
            return "random_principal";
        }else {
            return "unknown_principal";
        };
    };

 
     func get_certified_endpoints() : Iter.Iter<CertifiedAssets.EndpointRecord> {
        CertifiedAssets.endpoints(assets_internal.certificate_store);
    };

    suite.add(
        "owner grants permissions",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            ts_assert_or_print((assets.list_permitted({ permission = #Prepare })) == [], "Prepare permission should be empty");
            ts_assert_or_print((assets.list_permitted({ permission = #Commit })) == [owner, canister_id], "Commit permission should only be owner: " # debug_show Array.map(assets.list_permitted({ permission = #Commit }), get_principal_tag));
            ts_assert_or_print((assets.list_permitted({ permission = #ManagePermissions })) == [], "ManagePermissions permission should be empty");

            ts_assert_or_print(
                Result.isOk(
                    await* assets.grant_permission(
                        owner,
                        {
                            permission = #Prepare;
                            to_principal = preparer;
                        },
                    )
                ),
                "Unexpected error granting #Prepare permission from owner to preparer",
            );

            ts_assert_or_print((assets.list_permitted({ permission = #Prepare })) == [preparer], "Only the preparer should have Prepare permission");

            ts_assert_or_print(
                Result.isOk(
                    await* assets.grant_permission(
                        owner,
                        {
                            permission = #Commit;
                            to_principal = committer;
                        },
                    )
                ),
                "Unexpected error granting #Commit permission from owner to committer",
            );

            ts_assert_or_print((assets.list_permitted({ permission = #Commit })) == [owner, canister_id, committer], "Owner and committer should have Commit permission");

            ts_assert_or_print(
                Result.isOk(
                    await* assets.grant_permission(
                        owner,
                        {
                            permission = #ManagePermissions;
                            to_principal = manager;
                        },
                    )
                ),
                "Unexpected error granting #ManagePermissions permission from owner to manager",
            );

            ts_assert_or_print((assets.list_permitted({ permission = #ManagePermissions })) == [manager], "Only the manager should have ManagePermissions permission");

            await* add_controller(controller);
            let controllers = await* get_controllers();
            ts_print("controllers: " # debug_show Array.map(controllers, get_principal_tag));
            ts_assert_or_print(exists_in(controllers, Principal.equal, controller), "Controller is missing from list of controllers");

        },
    );

    suite.add(
        "manager and controller principals grant & revoke permissions",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            func permit(principal : Principal, permission : Assets.Permission, to_principal : Principal) : async* () {
                let call_result = await* assets.grant_permission(
                    principal,
                    {
                        permission;
                        to_principal;
                    },
                );

                ts_assert(Result.isOk(call_result));

                let permitted = assets.list_permitted({ permission });

                ts_assert(
                    exists_in(
                        permitted,
                        Principal.equal,
                        to_principal,
                    )
                );
            };

            func revoke(principal : Principal, permission : Assets.Permission, of_principal : Principal) : async* () {
                ts_assert(
                    Result.isOk(
                        await* assets.revoke_permission(
                            principal,
                            {
                                permission;
                                of_principal;
                            },
                        )
                    )
                );

                let permitted = assets.list_permitted({ permission });

                ts_assert_or_print(
                    not exists_in(
                        permitted,
                        Principal.equal,
                        of_principal,
                    ),
                    get_principal_tag(principal) # " failed to revoke " # get_principal_tag(of_principal) # " from " # debug_show permission # " permission: " # debug_show permitted,
                );

            };

            for (manager_or_controller in [manager, controller].vals()) {
                
                await* permit(manager_or_controller, #Prepare, random_principal);
                await* revoke(manager_or_controller, #Prepare, random_principal);

                await* permit(manager_or_controller, #Commit, random_principal);
                await* revoke(manager_or_controller, #Commit, random_principal);

                await* permit(manager_or_controller, #ManagePermissions, random_principal);
                await* revoke(manager_or_controller, #ManagePermissions, random_principal);

                await* add_controller(random_principal);
                var controllers = await* get_controllers();
                ts_assert(exists_in(controllers, Principal.equal, random_principal));

                await* remove_controller(random_principal);
                controllers := await* get_controllers();
                ts_assert(not exists_in(controllers, Principal.equal, random_principal));
            };
        },
    );

    suite.add(
        "none manager and none controller principals fail to grant  permissions",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            ts_assert_or_print(
                not exists_in(
                    assets.list_permitted({ permission = #Commit }),
                    Principal.equal,
                    random_principal,
                ),
                "Random Principal was unexpectedly in Commit permission before test",
            );

            for (non_manager in [no_permission, preparer, committer].vals()) {

                ts_assert(
                    Result.isErr(
                        await* assets.grant_permission(
                            non_manager,
                            {
                                permission = #Prepare;
                                to_principal = random_principal;
                            },
                        )
                    )
                );

                let prepare_permissions = assets.list_permitted({ permission = #Prepare });

                ts_assert_or_print(
                    not exists_in(
                        prepare_permissions,
                        Principal.equal,
                        random_principal,
                    ),
                    "Random Principal was unexpectedly was added by : " # debug_show get_principal_tag(non_manager) # " to the list of principals with Prepare permission: " # debug_show prepare_permissions,
                );

                ts_assert(
                    Result.isErr(
                        await* assets.grant_permission(
                            non_manager,
                            {
                                permission = #Commit;
                                to_principal = random_principal;
                            },
                        )
                    )
                );

                let commit_permissions = assets.list_permitted({ permission = #Commit });

                ts_assert_or_print(
                    not exists_in( commit_permissions, Principal.equal, random_principal)
                    , "[0x01] Random Principal was unexpectedly added by " # debug_show get_principal_tag(non_manager) # " to the list of principals with Commit permission: "  # debug_show commit_permissions
                );

                ts_assert(
                    Result.isErr(
                        await* assets.grant_permission(
                            non_manager,
                            {
                                permission = #ManagePermissions;
                                to_principal = random_principal;
                            },
                        )
                    )
                );

                let manage_permissions = assets.list_permitted({ permission = #ManagePermissions });

                ts_assert_or_print(
                    not exists_in(
                        manage_permissions,
                        Principal.equal,
                        random_principal,
                    ),
                    "[0x02] Random Principal was unexpectedly added by " # debug_show get_principal_tag(non_manager) # " to the list of principals with ManagePermissions permission: " # debug_show manage_permissions,
                );
            };
        },
    );

    suite.add(
        "get configuration",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {

            let config = switch (assets.get_configuration(owner)) {
                case (#ok(config)) config;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting configuration: " # msg);
                };
            };

            ts_assert(Result.isErr(assets.get_configuration(no_permission)));

            ts_assert(assets.get_configuration(preparer) == #ok(config));
            ts_assert(assets.get_configuration(committer) == #ok(config));

            ts_assert(
                config == {
                    max_batches = null;
                    max_chunks = null;
                    max_bytes = null;
                }
            );

        },
    );

    suite.add(
        "set configuration",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {

            func set_configuration(p : Principal) {
                let max_batches = fuzz.nat64.randomRange(0, 100);
                let max_chunks = fuzz.nat64.randomRange(0, 100_000);
                let max_bytes = fuzz.nat64.randomRange(0, 100_000_000_000);

                ts_assert(
                    Result.isOk(
                        assets.configure(
                            p,
                            {
                                max_batches = ??max_batches;
                                max_chunks = ??max_chunks;
                                max_bytes = ??max_bytes;
                            },
                        )
                    )
                );

                ts_assert(
                    assets.get_configuration(p) == #ok({
                        max_batches = ?max_batches;
                        max_chunks = ?max_chunks;
                        max_bytes = ?max_bytes;
                    })
                );
            };

            for (principal in [owner, committer].vals()) {
                set_configuration(principal);
            };

            let config = switch (assets.get_configuration(owner)) {
                case (#ok(config)) config;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting configuration: " # msg);
                };
            };

            func fail_to_set_configuration(p : Principal) {
                ts_assert(
                    Result.isErr(
                        assets.configure(
                            p,
                            {
                                max_batches = ??100;
                                max_chunks = ??100_000;
                                max_bytes = ??100_000_000_000;
                            },
                        )
                    )
                );
            };

            for (not_committer in [no_permission, preparer].vals()) {
                fail_to_set_configuration(not_committer);
            };

        },
    );

    suite.add(
        "test configuration limits",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {
            ts_assert(
                Result.isOk(
                    assets.configure(
                        owner,
                        {
                            max_batches = ??1;
                            max_chunks = ??1;
                            max_bytes = ??1;
                        },
                    )
                )
            );

            let batch_id = switch (assets.create_batch(owner, {})) {
                case (#ok({ batch_id })) batch_id;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating first batch: " # msg);
                };
            };

            // shouldn't be able to create more batches than the max_batches limit
            ts_assert(Result.isErr(assets.create_batch(owner, {})));

            // "aaa" is 3 bytes, greater than the max_bytes limit
            let chunk_greater_than_max_bytes = assets.create_chunk(owner, { batch_id; content = "aaa" });
            ts_assert(Result.isErr(chunk_greater_than_max_bytes));

            // successful chunk creation
            switch (assets.create_chunk(owner, { batch_id; content = "a" })) {
                case (#ok(_)) {};
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating first chunk: " # msg);
                };
            };

            // shouldn't be able to create more chunks than the max_chunks limit
            ts_assert(Result.isErr(assets.create_chunk(owner, { batch_id; content = "" })));

        },
    );

    let hello_world_gzip : Assets.StoreArgs = {
        key = "/test/store/hello";

        // "Hello, World!" encoded in gzip
        content = "\1f\8b\08\00\00\00\00\00\00\03\f3\48\cd\c9\c9\d7\51\08\cf\2f\ca\49\51\e4\02\00\84\9e\e8\b4\0e\00\00\00";
        sha256 = null;
        content_type = "text/html";
        content_encoding = "gzip";
        is_aliased = ?true;
    };

    suite.add(
        "store and get asset",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {

            for (none_commiter in [no_permission, preparer, manager, controller].vals()) {
                ts_assert(
                    Result.isErr(
                        assets.store(none_commiter, hello_world_gzip)
                    )
                );
            };

            ts_assert(
                Result.isOk(
                    assets.store(committer, hello_world_gzip)
                )
            );

            let get_args = {
                key = "/test/store/hello";
                accept_encodings = ["gzip"];
            };

            let asset_details = switch (assets.get(get_args)) {
                case (#ok(asset_details)) asset_details;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting asset: " # msg);
                };
            };

            ts_assert(
                asset_details == {
                    content = hello_world_gzip.content;
                    content_type = hello_world_gzip.content_type;
                    content_encoding = hello_world_gzip.content_encoding;
                    total_length = hello_world_gzip.content.size();
                    sha256 = ?(Sha256.fromBlob(#sha256, hello_world_gzip.content));
                }
            );
        },
    );

    let hello_world_identity : Assets.StoreArgs = {
        key = "/test/store/hello";
        content = "<h1>Hello, World!</h1>";
        sha256 = null;
        content_type = "text/html";
        content_encoding = "identity";
        is_aliased = ?true;
    };

    suite.add(
        "assets.get() returns first matching asset in the given accept-encodings order",
        func({ ts_assert; ts_print; ts_assert_or_print} : CanisterTests.TestTools) : async () {

            ts_assert(
                Result.isOk(
                    assets.store(committer, hello_world_identity)
                )
            );

            ts_assert_or_print(
                Itertools.any(
                    get_certified_endpoints(),
                    func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                        endpoint.url == "/test/store/hello";
                    },
                ),
                "Failed to find certified endpoint for /test/store/hello",
            );

            var get_args = {
                key = "/test/store/hello";
                accept_encodings = ["identity", "gzip"];
            };

            var asset_details = switch (assets.get(get_args)) {
                case (#ok(asset_details)) asset_details;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting asset: " # msg);
                };
            };

            let expected_identity_asset_details = {
                content = hello_world_identity.content;
                content_type = hello_world_identity.content_type;
                content_encoding = hello_world_identity.content_encoding;
                total_length = hello_world_identity.content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, hello_world_identity.content));
            };

            ts_assert_or_print(
                asset_details == expected_identity_asset_details,
                "[0xd1] Failed to match asset details (actual vs expected): " #  debug_show (asset_details, expected_identity_asset_details),
            );

            get_args := {
                get_args with accept_encodings = ["gzip", "identity"]
            };

            asset_details := switch (assets.get(get_args)) {
                case (#ok(asset_details)) asset_details;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting asset: " # msg);
                };
            };

            let expected_gzip_encoding_details = {
                content = hello_world_gzip.content;
                content_type = hello_world_gzip.content_type;
                content_encoding = hello_world_gzip.content_encoding;
                total_length = hello_world_gzip.content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, hello_world_gzip.content));
            };

            ts_assert_or_print(
                asset_details == expected_gzip_encoding_details,
                "[0xd2] Failed to match asset details (actual vs expected): " #  debug_show (asset_details, expected_gzip_encoding_details),
            );

            get_args := {
                get_args with accept_encodings = ["compress", "identity", "gzip"];
            };

            asset_details := switch (assets.get(get_args)) {
                case (#ok(asset_details)) asset_details;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting asset: " # msg);
                };
            };

            ts_assert_or_print(
                asset_details == expected_identity_asset_details,
                "[0xd3] Failed to match asset details (actual vs expected): " #  debug_show (asset_details, expected_identity_asset_details),
            );

            ts_assert_or_print(
                Result.isErr(
                    assets.get({ get_args with accept_encodings = [] })
                ),
                "[0xd4] Unexpected success getting asset with empty accept-encodings",
            );

            ts_assert_or_print(
                Result.isErr(
                    assets.get({ get_args with accept_encodings = ["compress"] })
                ),
                "[0xd4] Unexpected success getting asset with incorrect encoding",
            );
        },
    );

    suite.add(
        "set and get asset properties",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            for (none_commiter in [no_permission, preparer, manager, controller].vals()) {
                ts_assert_or_print(
                    Result.isErr(
                        assets.set_asset_properties(
                            none_commiter,
                            {
                                key = "/test/store/hello";
                                max_age = null;
                                headers = null;
                                allow_raw_access = ??true;
                                is_aliased = ??false;
                            },
                        )
                    ),
                    "Unexpected success setting asset properties by " # get_principal_tag(none_commiter) # " principal without commit permission",
                );
            };

            var asset_properties = switch (assets.get_asset_properties("/test/store/hello")) {
                case (#ok(asset_properties)) asset_properties;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error setting asset properties: " # msg);
                };
            };

            ts_assert_or_print(
                asset_properties == {
                    max_age = null;
                    headers = null;
                    allow_raw_access = null;
                    is_aliased = ?true;
                },
                "Unexpected asset properties: " # debug_show asset_properties,
            );

             ts_assert_or_print(
                Result.isOk(
                    assets.set_asset_properties(
                        committer,
                        {
                            key = "/test/store/hello";
                            max_age = ?null;
                            headers = ?null;
                            allow_raw_access = ?null;
                            is_aliased = ?null;
                        },
                    )
                ),
                "Failed to reset asset properties",
            );

            asset_properties := switch (assets.get_asset_properties("/test/store/hello")) {
                case (#ok(asset_properties)) asset_properties;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting asset properties: " # msg);
                };
            };

            ts_assert_or_print(
                asset_properties == {
                    max_age = null;
                    headers = null;
                    allow_raw_access = null;
                    is_aliased = null;
                },
                "Unexpected asset properties after reset: " # debug_show asset_properties,
            );

            ts_assert_or_print(
                Result.isOk(
                    assets.set_asset_properties(
                        committer,
                        {
                            key = "/test/store/hello";
                            max_age = ??100;
                            headers = ??[("Custom-Header", "custom-value")];
                            allow_raw_access = ??true;
                            is_aliased = ??true;
                        },
                    )
                ),
                "Failed to set asset properties",
            );

            asset_properties := switch (assets.get_asset_properties("/test/store/hello")) {
                case (#ok(asset_properties)) asset_properties;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error getting asset properties: " # msg);
                };
            };

            ts_assert_or_print(
                asset_properties == {
                    max_age = ?100;
                    headers = ?[("Custom-Header", "custom-value")];
                    allow_raw_access = ?true;
                    is_aliased = ?true;
                },
                "Unexpected asset properties after 2nd set: " # debug_show asset_properties,
            );

           

        },
    );

 // todo- implement
    suite.add(
        "get asset with aliasing",
       func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {  
        // set the property in one of the assets to support aliasing
        // set the other to not support it, then try to get the asset with aliasing

        // key -> "/test/asset/hello" already supports aliasing

        let hello_world_content : Blob = "<h1>Hello, World!</h1>";

        let actual_hello_html = assets.get({key="/test/store/hello.html";accept_encodings=["identity"]});
        let expected_hello_html : Result<Assets.EncodedAsset, Text> = #ok({
            content = hello_world_content;
            content_type = "text/html";
            content_encoding = "identity";
            total_length = hello_world_content.size();
            sha256 = ?(Sha256.fromBlob(#sha256, hello_world_content));
        });

        ts_assert_or_print(
            actual_hello_html == expected_hello_html ,
            "[0xtf] Failed to match asset content (actual vs expected): " # debug_show (actual_hello_html, expected_hello_html),
        );

        ts_assert_or_print(
            Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello.html"
                }
            ),
            "Failed to find certified endpoint for /test/store/hello.html",
        );

        let actual_hello_index_html = assets.get({key="/test/store/hello/index.html";accept_encodings=["identity"]});
        let expected_hello_index_html : Result<Assets.EncodedAsset, Text> = #ok({
            content = hello_world_content;
            content_type = "text/html";
            content_encoding = "identity";
            total_length = hello_world_content.size();
            sha256 = ?(Sha256.fromBlob(#sha256, hello_world_content));
        });

        ts_assert_or_print(
            actual_hello_index_html == expected_hello_index_html,
            "[0xth] Failed to match asset content (actual vs expected): " # debug_show (actual_hello_index_html, expected_hello_index_html),
        );

        ts_assert_or_print(
            Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello/index.html"
                }
            ),
            "Failed to find certified endpoint for /test/store/hello/index.html",
        );

        ts_assert_or_print(
            Result.isErr(
               assets.set_asset_properties(
                    committer,
                    {
                        key = "/test/store/hello.html";
                        max_age = null;
                        headers = null;
                        allow_raw_access = null;
                        is_aliased = ??false;
                    },
                ) 
            ),
            "[0xtj] Unexpected success making updates using alias /test/store/hello.html instead of original key",
        );

        ts_assert_or_print(
            Result.isErr(
               assets.set_asset_properties(
                    committer,
                    {
                        key = "/test/store/hello/index.html";
                        max_age = null;
                        headers = null;
                        allow_raw_access = null;
                        is_aliased = ??false;
                    },
                ) 
            ),
            "[0xtk] Unexpected success making updates using alias /test/store/hello/index.html instead of original key",
        );

        ts_assert_or_print(
            Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello"
                }
            ),
            "[0xt8] Certified endpoint for /test/store/hello should be present if aliasing is enabled",
        );

        ts_assert_or_print(
            Result.isOk(
                assets.set_asset_properties(
                    committer,
                    {
                        key = "/test/store/hello";
                        max_age = null;
                        headers = null;
                        allow_raw_access = null;
                        is_aliased = ??false;
                    },
                )
            ),
            "[0xti] Failed to revoke aliasing for /test/store/hello",
        );
 

        ts_assert_or_print(
            Result.isErr(
                assets.get({key="/test/store/hello.html";accept_encodings=["identity"]})
            ),
            "Retrieving /test/store/hello.html should fail after revoking aliasing",
        );

        ts_assert_or_print(
            not Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello.html"
                }
            ),
            "Certified endpoint for /test/store/hello.html should be removed after revoking aliasing",
        );

        ts_assert_or_print(
            Result.isErr(
                assets.get({key="/test/store/hello/index.html";accept_encodings=["identity"]})
            ),
            "Retrieving /test/store/hello/index.html should fail after revoking aliasing",
        );

        ts_assert_or_print(
            not Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello/index.html"
                }
            ),
            "Certified endpoint for /test/store/hello/index.html should be removed after revoking aliasing",
        );

        ts_assert_or_print(
            Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello"
                }
            ),
            "[0xt8] Main key certificate should still be present after revoking aliasing",
        );

        ts_assert_or_print(
            Result.isOk(
                assets.set_asset_properties(
                    committer,
                    {
                        key = "/test/store/hello";
                        max_age = null;
                        headers = null;
                        allow_raw_access = null;
                        is_aliased = ??true;
                    },
                )
            ),
            "[0xt2] Failed to re-enable aliasing for /test/store/hello",
        );

        ts_assert_or_print(
            Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello.html"
                }
            ),
            "[0xt3] Failed to re-certify endpoint for /test/store/hello.html after enabling aliasing",
        );

       ts_assert_or_print(
            Itertools.any(
                get_certified_endpoints(),
                func(endpoint : CertifiedAssets.EndpointRecord) : Bool {
                    endpoint.url == "/test/store/hello/index.html"
                }
            ),
            "[0xt3] Failed to re-certify endpoint for /test/store/hello/index.html after enabling aliasing",
        );

       }
    );

    suite.add(
        "certified_tree() fails in update call",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {
            ts_assert(
                Result.isErr(
                    assets.certified_tree()
                )
            );
        },
    );

    suite.add_query(
        "certified_tree() passes in query call",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) {
            ts_assert(
                Result.isOk(
                    assets.certified_tree()
                )
            );
        },
    );

    suite.add(
        "create batch",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            // remove all configured limits
            ts_assert_or_print(
                Result.isOk(
                    assets.configure(
                        owner,
                        {
                            max_batches = ?null;
                            max_chunks = ?null;
                            max_bytes = ?null;
                        },
                    )
                ),
                "Failed to remove all configured limits",
            );

            for (not_preparer in [no_permission, manager, controller].vals()) {
                ts_assert_or_print(
                    Result.isErr(
                        assets.create_batch(not_preparer, {})
                    ),
                    "Unexpected creation of batch by " # get_principal_tag(not_preparer) # " principal without commit permission",
                );
            };

            for (principal_with_preparer_permission in [preparer, committer].vals()) {
                let batch_id = switch (assets.create_batch(principal_with_preparer_permission, {})) {
                    case (#ok({ batch_id })) batch_id;
                    case (#err(msg)) {
                        ts_assert(false);
                        return ts_print("Error creating batch: " # msg);
                    };
                };

                ts_assert(
                    Option.isSome(
                        Map.get(assets_internal.batches, nhash, batch_id)
                    )
                );
            };

        },
    );

    var test_create_chunk_batch_id = 0x010101010;

    suite.add(
        "create chunk",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            let batch_id = switch (assets.create_batch(committer, {})) {
                case (#ok({ batch_id })) batch_id;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating batch: " # msg);
                };
            };

            for (not_preparer in [no_permission, manager, controller].vals()) {
                ts_assert_or_print(
                    Result.isErr(
                        assets.create_chunk(not_preparer, { batch_id; content = "" })
                    ),
                    "Unexpected creation of chunk by " # get_principal_tag(not_preparer) # " principal without commit permission",
                );
            };

            let chunk_ids = Buffer.Buffer<Nat>(8);
            let chunk_1 : Blob = fuzz.blob.randomBlob(fuzz.nat.randomRange(0, Assets.MAX_CHUNK_SIZE));

            switch (assets.create_chunk(committer, { batch_id; content = chunk_1 })) {
                case (#ok({ chunk_id })) chunk_ids.add(chunk_id);
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating chunk: " # msg);
                };
            };

            ts_assert(
                Map.get(assets_internal.chunks, nhash, chunk_ids.get(0)) == ?{
                    content = Blob.toArray(chunk_1);
                    batch_id;
                }
            );

            let chunk_2 : Blob = fuzz.blob.randomBlob(fuzz.nat.randomRange(0, Assets.MAX_CHUNK_SIZE));

            switch (assets.create_chunk(preparer, { batch_id; content = chunk_2 })) {
                case (#ok({ chunk_id })) chunk_ids.add(chunk_id);
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating chunk: " # msg);
                };
            };

            ts_assert(
                Map.get(assets_internal.chunks, nhash, chunk_ids.get(1)) == ?{
                    content = Blob.toArray(chunk_2);
                    batch_id;
                }
            );

            let batch = switch (Map.get(assets_internal.batches, nhash, batch_id)) {
                case (?batch) batch;
                case (null) {
                    ts_assert(false);
                    return ts_print("Batch not found");
                };
            };

            ts_assert(batch.total_bytes == chunk_1.size() + chunk_2.size());
            ts_assert(Vector.toArray(batch.chunk_ids) == Buffer.toArray(chunk_ids));

            // used in 'delete batch before commit'
            test_create_chunk_batch_id := batch_id;
        },
    );

    suite.add(
        "delete batch before commit",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {

            let batch_id = test_create_chunk_batch_id;

            for (not_preparer in [no_permission, manager, controller].vals()) {
                ts_assert(
                    Result.isErr(
                        assets.delete_batch(not_preparer, { batch_id })
                    )
                );
            };

            let batch = switch (Map.get(assets_internal.batches, nhash, batch_id)) {
                case (?batch) batch;
                case (null) {
                    ts_assert(false);
                    return ts_print("Batch not found");
                };
            };

            ts_assert(
                Result.isOk(
                    assets.delete_batch(preparer, { batch_id })
                )
            );

            ts_assert(
                Option.isNull(
                    Map.get(assets_internal.batches, nhash, batch_id)
                )
            );

            for (chunk_id in Vector.vals(batch.chunk_ids)) {
                ts_assert(
                    Option.isNull(
                        Map.get(assets_internal.chunks, nhash, chunk_id)
                    )
                );
            };
        },
    );

    var test_create_asset_batch_id = 0x020202020;

    suite.add(
        "create asset",
        func({ ts_assert; ts_print } : CanisterTests.TestTools) : async () {

            let batch_id = switch (assets.create_batch(committer, {})) {
                case (#ok({ batch_id })) batch_id;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating batch: " # msg);
                };
            };

            test_create_asset_batch_id := batch_id;

            for (not_commiter in [no_permission, preparer, manager, controller].vals()) {
                ts_assert(
                    Result.isErr(
                        assets.create_asset(
                            not_commiter,
                            {
                                key = "/test/asset/hello";
                                content_type = "text/plain";
                                max_age = null;
                                headers = null;
                                enable_aliasing = ?true;
                                allow_raw_access = ?false;
                            },
                        )
                    )
                );
            };

            switch (
                assets.create_asset(
                    committer,
                    {
                        key = "/test/asset/hello";
                        content_type = "text/plain";
                        max_age = null;
                        headers = ?[("Custom-Header", "custom-value")];
                        enable_aliasing = ?true;
                        allow_raw_access = ?false;
                    },
                )
            ) {
                case (#ok()) {};
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating asset: " # msg);
                };
            };

            let asset_properties = switch (assets.get_asset_properties("/test/asset/hello")) {
                case (#ok(asset_properties)) asset_properties;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error retrieving asset properties: " # msg);
                };
            };

            ts_assert(
                asset_properties == {
                    max_age = null;
                    headers = ?[("Custom-Header", "custom-value")];
                    allow_raw_access = ?false;
                    is_aliased = ?true;
                }
            );

        },

    );

    
    let test_create_asset_chunk_ids = Buffer.Buffer<Nat>(8);

    suite.add(
        "upload chunks and set asset content",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            let batch_id = test_create_asset_batch_id;
            let chunk_ids = test_create_asset_chunk_ids;

            let chunk_0 : Blob = "👋 ";
            let chunk_1 : Blob = "Hello, ";
            let chunk_2 : Blob = "World!";

            for (chunk in [chunk_0, chunk_1, chunk_2].vals()) {
                switch (assets.create_chunk(committer, { batch_id; content = chunk })) {
                    case (#ok({ chunk_id })) chunk_ids.add(chunk_id);
                    case (#err(msg)) {
                        ts_assert(false);
                        return ts_print("Error creating chunk: " # msg);
                    };
                };
            };

            ts_assert_or_print(
                Result.isOk(
                    await* assets.set_asset_content(
                        committer,
                        {
                            key = "/test/asset/hello";
                            sha256 = null; 
                            chunk_ids = Buffer.toArray(chunk_ids);
                            content_encoding = "identity";
                        },
                    )
                ),
                "Failed to set asset content",
            );

            let actual_asset_content = assets.get({ key = "/test/asset/hello"; accept_encodings = ["identity"] });

            let expected_content : Blob = "👋 Hello, World!";
            let expected_asset_content : Result<Assets.EncodedAsset, Text> = #ok({
                content = expected_content;
                content_type = "text/plain";
                content_encoding = "identity";
                total_length = expected_content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, expected_content));
            });

            ts_assert_or_print(
                actual_asset_content == expected_asset_content,
                "[0xfe] Failed to match asset content (actual vs expected): " # debug_show (actual_asset_content, expected_asset_content),
            );


        },
    );

    suite.add(
        "referencing chunk present in a batch multiple times for a single asset",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            let chunk_ids = test_create_asset_chunk_ids;

            switch(assets.create_asset(
                committer,
                {
                    key = "/test/asset/multiple-hello";
                    content_type = "text/plain";
                    max_age = null;
                    headers = null;
                    enable_aliasing = ?true;
                    allow_raw_access = ?false;
                },
            )) {
                case (#ok()) {};
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating asset: " # msg);
                };
            };

            ts_assert_or_print(
                Result.isOk(
                    await* assets.set_asset_content(
                        committer,
                        {
                            key = "/test/asset/multiple-hello";
                            sha256 = null;
                            chunk_ids = [chunk_ids.get(0), chunk_ids.get(0), chunk_ids.get(0), chunk_ids.get(1), chunk_ids.get(1), chunk_ids.get(2)];
                            content_encoding = "identity";
                        },
                    )
                ),
                "Failed to set asset content",
            );

            let actual_asset_details = assets.get({ key = "/test/asset/multiple-hello"; accept_encodings = ["identity"] });

            let expected_content : Blob = "👋 👋 👋 Hello, Hello, World!";
            let expected_asset_details : Result<Assets.EncodedAsset, Text> = #ok({
                content = expected_content;
                content_type = "text/plain";
                content_encoding = "identity";
                total_length = expected_content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, expected_content));
            });

            ts_assert_or_print(
                actual_asset_details == expected_asset_details,
                "[0xtr] Failed to match asset content (actual vs expected): " # debug_show (actual_asset_details, expected_asset_details),
            );


        },

    );
   
    suite.add(
        "creating an asset with content greater than 2MB ingress limit",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 

            let batch_id = test_create_asset_batch_id;

            let chunk_3_bytes = chunk_1mb_bytes;
            let chunk_4_bytes = chunk_1mb_2_bytes;
            let chunk_5_bytes = chunk_2mb_bytes;

            let chunk_3 = chunk_1mb_blob;
            let chunk_4 = chunk_1mb_2_blob;
            let chunk_5 = chunk_2mb_blob;

            let #ok({chunk_ids}) = await* assets.create_chunks(committer, {batch_id; content = [chunk_3, chunk_4, chunk_5]});

            ts_print("Uploaded chunks");

            ts_assert_or_print(
                Result.isOk(
                    assets.create_asset(
                        committer,
                        {
                            key = "/test/asset/more_than_2mb";
                            content_type = "text/plain";
                            max_age = null;
                            headers = null;
                            enable_aliasing = ?true;
                            allow_raw_access = ?false;
                        },
                    ),
                ),
                "[0xg9] Failed to create asset for content greater than 2MB",
            );

            ts_print("Created asset");

            ts_assert_or_print(
                Result.isOk(
                    await* assets.set_asset_content(
                        committer,
                        {
                            key = "/test/asset/more_than_2mb";
                            sha256 = null;
                            chunk_ids ;
                            content_encoding = "identity";
                        },
                    ),
                ),
                "[0xga] Failed to set asset content for /test/asset/more_than_2mb",
            );

            ts_print("Set asset content");

            switch(Map.get(assets_internal.assets, thash, "/test/asset/more_than_2mb")) {
                case (?asset) switch(Map.get(asset.encodings, thash, "identity")) {
                    case (?encoding) {

                        ts_print("encoding: " # debug_show ({
                            total_length = encoding.total_length;
                            content_chunks_prefix_sum = encoding.content_chunks_prefix_sum;
                            certified = encoding.certified;
                            sha256 = encoding.sha256;
                            modified = encoding.modified;
                            content_chunks = null;
                        }));

                    };
                    case (null) {
                        ts_assert(false);
                        return ts_print("Asset encoding not found");
                    };
                };
                case (null) {
                    ts_assert(false);
                    return ts_print("Asset not found");
                };
            };

            // merges and resizes chunks internally so the max size is 2MB
            // which means the chunks may not be exactly the same size as the input chunks
            // but the content and total length will be the same

            let first_returned_chunk = Blob.fromArray(Array.append(chunk_3_bytes, chunk_4_bytes));
            let second_returned_chunk = chunk_5;

            
            let sha256 = await* Assets.hash_chunks([chunk_3, chunk_4, chunk_5]);

            let actual_asset_details = assets.get({ key = "/test/asset/more_than_2mb"; accept_encodings = ["identity"] });
            let expected_asset_details : Result<Assets.EncodedAsset, Text> = #ok({
                content = first_returned_chunk;
                content_type = "text/plain";
                content_encoding = "identity";
                total_length = first_returned_chunk.size() + second_returned_chunk.size();
                sha256 = ?sha256;
            });

            func exclude_content(encoded_asset : Assets.EncodedAsset) : Assets.EncodedAsset {
                {
                    encoded_asset with content : Blob = "Content too large to display"
                }
            };

            ts_assert_or_print(
                actual_asset_details == expected_asset_details,
                "[0xeq] Failed to match asset content (actual vs expected): " # debug_show (Result.mapOk(actual_asset_details, exclude_content), Result.mapOk(expected_asset_details, exclude_content)),
            );

            // without sha256 hash
            ts_assert_or_print(
                assets.get_chunk({
                    key = "/test/asset/more_than_2mb";
                    content_encoding = "identity";
                    index = 1;
                    sha256 = null;
                }) == #ok({
                    content = second_returned_chunk;
                }),
                "[0xer] Failed to get chunk without sha256 hash",
            );

            // with correct sha256 hash
             ts_assert_or_print(
                assets.get_chunk({
                    key = "/test/asset/more_than_2mb";
                    content_encoding = "identity";
                    index = 1;
                    sha256 = ?sha256;
                }) == #ok({
                    content = second_returned_chunk;
                }),
                "[0xes] Failed to get chunk with correct sha256 hash",
            );

            // with incorrect sha256 hash
            ts_assert_or_print(
                Result.isErr(
                    assets.get_chunk({
                        key = "/test/asset/more_than_2mb";
                        content_encoding = "identity";
                        index = 1;
                        sha256 = ?Blob.fromArray([0x62, 0x62, 0x62]);
                    })
                ),
                "Should not pass with incorrect sha256 hash",
            );
            
            ts_assert_or_print(
                Result.isErr(
                    assets.get_chunk({
                        key = "/test/asset/more_than_2mb";
                        content_encoding = "identity";
                        index = 2;
                        sha256 = null;
                    })
                ),
                "Should not pass with invalid index",
            );
        }
    );

    suite.add(
        "unset asset content to remove specific content encoding",
         func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 

            let batch_id = test_create_asset_batch_id;
            let chunk_0: Blob = "pretend ";
            let chunk_1 : Blob= "this is ";
            let chunk_2 : Blob= "gzip encoded";

            let chunk_ids = Buffer.Buffer<Nat>(8);

            for (chunk in [chunk_0, chunk_1, chunk_2].vals()) {
                switch (assets.create_chunk(committer, { batch_id; content = chunk })) {
                    case (#ok({ chunk_id })) chunk_ids.add(chunk_id);
                    case (#err(msg)) {
                        ts_assert(false);
                        return ts_print("Error creating chunk: " # msg);
                    };
                };
            };

            ts_assert_or_print(
                Result.isOk(
                    await* assets.set_asset_content(
                        committer,
                        {
                            key = "/test/asset/hello";
                            sha256 = null;
                            chunk_ids = Buffer.toArray(chunk_ids);
                            content_encoding = "gzip";
                        },
                    ),
                ),
                "Failed to set asset content for /test/asset/hello",
            );

            let gzip_content : Blob = "pretend this is gzip encoded";

            ts_assert_or_print(
                assets.get({ key = "/test/asset/hello"; accept_encodings = ["gzip"] }) == #ok({
                    content = gzip_content;
                    content_type = "text/plain";
                    content_encoding = "gzip";
                    total_length = gzip_content.size();
                    sha256 = ?(Sha256.fromBlob(#sha256, gzip_content));
                }),
                "Failed to match asset content",
            );

            ts_assert_or_print(
                Result.isOk(
                    assets.unset_asset_content(
                        committer,
                        {
                            key = "/test/asset/hello";
                            content_encoding = "gzip";
                        },
                    ),
                ),
                "Failed to unset asset content for /test/asset/hello",
            );

            ts_assert_or_print(
                Result.isErr(
                    assets.get({ key = "/test/asset/hello"; accept_encodings = ["gzip"] })
                ),
                "Unexpectedly passed with unset content encoding",
            );

            let identity_encoding : Blob = "👋 Hello, World!";

            let actual_identity_encoding_details = assets.get({ key = "/test/asset/hello"; accept_encodings = ["identity"] });
            let expected_identity_encoding_details : Result<Assets.EncodedAsset, Text> = #ok({
                content = identity_encoding;
                content_type = "text/plain";
                content_encoding = "identity";
                total_length = identity_encoding.size();
                sha256 = ?(Sha256.fromBlob(#sha256, identity_encoding));
            });

            ts_assert_or_print(
                actual_identity_encoding_details == expected_identity_encoding_details,
                "Removing gzip encoding unexpectedly affected other encodings (actual vs expected): " # debug_show (actual_identity_encoding_details, expected_identity_encoding_details),
            )
        }
    );

    func blob_concat(blobs: [Blob]) : Blob {
        Blob.fromArray(
            Iter.toArray(
                Itertools.flatten(
                    Iter.map<Blob, Iter.Iter<Nat8>>(
                        blobs.vals(),
                        func(blob: Blob) : Iter.Iter<Nat8> { blob.vals() }
                    )
                ),
            )
        );
    };

    suite.add(
        "commit batch - create and edit multiple assets in a single call",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
            // create a batch with multiple assets, edit some of them and commit
            // verify that all the assets are created and edited correctly

            let batch_id = switch (assets.create_batch(committer, {})) {
                case (#ok({ batch_id })) batch_id;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating batch: " # msg);
                };
            };

            // upload chunks in parallel
            let identity_chunks : [Blob] = [
                "[",
                "{ \"name\": \"Alice\", \"age\": 25 }",
                "{ \"name\": \"Bob\", \"age\": 30 }",
                "]",
            ];

            let #ok({ chunk_ids = identity_chunk_ids}) = await* assets.create_chunks(committer, { batch_id; content = identity_chunks });

            let gzip_chunks : [Blob]= [
                "pretend ",
                "this is ",
                "gzip encoded",
            ];
            let #ok({chunk_ids = gzip_chunk_ids}) = await* assets.create_chunks(committer, { batch_id; content = gzip_chunks });

            let identity_content : Blob = blob_concat(identity_chunks);
            let gzip_content : Blob = blob_concat(gzip_chunks);
            
            let commit_args : Assets.CommitBatchArguments = {
                batch_id;
                operations = [
                    #CreateAsset({
                        key = "/test/commit/file.json";
                        content_type = "application/json";
                        max_age = null;
                        headers = null;
                        enable_aliasing = null;
                        allow_raw_access = null;
                    }),
                    #SetAssetContent({
                        key = "/test/commit/file.json";
                        content_encoding = "identity";
                        chunk_ids = identity_chunk_ids;
                        sha256 = null;
                    }),
                    #SetAssetContent({
                        key = "/test/commit/file.json";
                        content_encoding = "gzip";
                        chunk_ids = gzip_chunk_ids;
                        sha256 = null;
                    }),
                    #CreateAsset({
                        key = "/test/commit/no-data";
                        content_type = "text/plain";
                        max_age = null;
                        headers = null;
                        enable_aliasing = null;
                        allow_raw_access = null;
                    }),
                    #SetAssetProperties({
                        key = "/test/commit/no-data";
                        max_age = ??(100_000_000_000);
                        headers = ??[("content-length", "0")];
                        allow_raw_access = ??true;
                        is_aliased = ??true;
                    }),
                ];
            };

            ts_assert_or_print(
                Result.isOk(
                    await* assets.commit_batch(committer, commit_args)
                ),
                "[0xur] Failed to commit batch",
            );

            let actual_identity_encoding_details = assets.get({ key = "/test/commit/file.json"; accept_encodings = ["identity"] });
            let expected_identity_encoding_details : Result<Assets.EncodedAsset, Text> = #ok({
                content = identity_content;
                content_type = "application/json";
                content_encoding = "identity";
                total_length = identity_content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, identity_content));
            });

            ts_assert_or_print(
                actual_identity_encoding_details == expected_identity_encoding_details,
                "[0xus] Failed to match asset /test/commit/file.json (actual vs expected): " # debug_show (actual_identity_encoding_details, expected_identity_encoding_details),
            );

            let actual_gzip_encoding_details = assets.get({ key = "/test/commit/file.json"; accept_encodings = ["gzip"] });
            let expected_gzip_encoding_details : Result<Assets.EncodedAsset, Text> = #ok({
                content = gzip_content;
                content_type = "application/json";
                content_encoding = "gzip";
                total_length = gzip_content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, gzip_content));
            });

            ts_assert_or_print(
                actual_gzip_encoding_details == expected_gzip_encoding_details,
                "[0xut] Failed to match asset /test/commit/file.json (actual vs expected): " # debug_show (actual_gzip_encoding_details, expected_gzip_encoding_details),
            );

            ts_assert_or_print(
                Result.isErr(
                    assets.get({ key = "/test/commit/no-data"; accept_encodings = [] })
                ),
                "[0xuu] Unexpectledly passed even though asset has no content encoding and accept-encodings is empty",
            );

            let actual_no_data_asset_properties : Assets.AssetProperties = switch (assets.get_asset_properties("/test/commit/no-data")) {
                case (#ok(asset_properties)) asset_properties;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error retrieving asset properties: " # msg);
                };
            };

            let expected_no_data_asset_properties: Assets.AssetProperties = {
                max_age = ?100_000_000_000;
                headers = ?[("content-length", "0")];
                allow_raw_access = ?true;
                is_aliased = ?true;
            };

            ts_assert_or_print(
                actual_no_data_asset_properties == expected_no_data_asset_properties,
                "[0xuv] Failed to update asset properties: " # debug_show (actual_no_data_asset_properties, expected_no_data_asset_properties),
            );

        }
    );

    suite.add(
        "revert failed commit batch",
            func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
                // create a batch with an operation that fails, and revert the batch so none of the changes are committed

                let batch_id = switch (assets.create_batch(committer, {})) {
                    case (#ok({ batch_id })) batch_id;
                    case (#err(msg)) {
                        ts_assert(false);
                        return ts_print("Error creating batch: " # msg);
                    };
                };

                let plain_hello_content : Blob = "Not Hello, World!";
                let hello_world_gzip_content : Blob = "pretend this is gzip encoded";
                
                // upload chunks in parallel
                let #ok({chunk_ids = plain_hello_chunk_ids}) = await* assets.create_chunks(committer, { batch_id; content = [plain_hello_content] });
                let #ok({chunk_ids = gzip_hello_chunk_ids}) = await* assets.create_chunks(committer, { batch_id; content = [hello_world_gzip_content] });

                let commit_args : Assets.CommitBatchArguments = {
                    batch_id;
                    operations = [
                        // create a new asset
                        #CreateAsset({
                            key = "/test/commit/new-hello-file";
                            content_type = "text/plain";
                            max_age = null;
                            headers = null;
                            enable_aliasing = null;
                            allow_raw_access = null;
                        }),
                        #SetAssetContent({
                            key = "/test/commit/new-hello-file";
                            content_encoding = "gzip";
                            chunk_ids = gzip_hello_chunk_ids;
                            sha256 = null;
                        }),

                        // replace existing asset content with new content
                        #SetAssetContent({
                            key = "/test/asset/hello";
                            content_encoding = "identity";
                            chunk_ids = plain_hello_chunk_ids;
                            sha256 = null;
                        }),


                        // should fail because the asset hasn't been created
                        #SetAssetContent({
                            key = "/test/commit/missing-file";
                            content_encoding = "gzip";
                            chunk_ids = gzip_hello_chunk_ids;
                            sha256 = null;
                        }),
                    ];
                };

                ts_assert_or_print(
                    Result.isErr(
                        await* assets.commit_batch(committer, commit_args)
                    ),
                    "[0xv1] Failed to revert failed commit batch",
                );

                ts_assert_or_print(
                    Result.isErr(
                        assets.get({ key = "/test/commit/new-hello-file"; accept_encodings = ["gzip"] })
                    ),
                    "[0xv2] Failed to revert changes in batch and created /test/commit/new-hello-file",
                );

                
                let actual_hello_asset_details = assets.get({ key = "/test/asset/hello"; accept_encodings = ["identity"] });

                let expected_hello_content: Blob = "👋 Hello, World!";

                let expected_hello_asset_details = #ok({
                    content = expected_hello_content;
                    content_type = "text/plain";
                    content_encoding = "identity";
                    total_length = expected_hello_content.size();
                    sha256 = ?(Sha256.fromBlob(#sha256, expected_hello_content));
                });


                ts_assert_or_print(
                    actual_hello_asset_details == expected_hello_asset_details,
                    "[0xv3] Failed to revert changes to /test/asset/hello",
                );
            }
    );

    suite.add(
        "propose and commit batch",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
            // create a batch with multiple assets, propose the batch, edit some of them and commit
            // verify that all the assets are created and edited correctly

            let batch_id = switch (assets.create_batch(preparer, {})) {
                case (#ok({ batch_id })) batch_id;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating batch: " # msg);
                };
            };

            let chunks : [Blob]= ["propose_commit_batch test chunk 1.\n", "propose_commit_batch test chunk 2.\n", "propose_commit_batch test chunk 3.\n"];
            let #ok({chunk_ids}) = await* assets.create_chunks(preparer, { batch_id; content = chunks });

            let commit_args : Assets.CommitBatchArguments = {
                batch_id;
                operations = [
                    #CreateAsset({
                        key = "/test/propose_commit/file.txt";
                        content_type = "text/plain";
                        max_age = null;
                        headers = null;
                        enable_aliasing = null;
                        allow_raw_access = null;
                    }),
                    #SetAssetContent({
                        key = "/test/propose_commit/file.txt";
                        content_encoding = "identity";
                        chunk_ids;
                        sha256 = null;
                    }),
                ];
            };

            ts_assert_or_print(
                Result.isOk(
                    assets.propose_commit_batch(preparer, commit_args)
                ),
                "[0xv4] Failed to propose commit batch",
            );

            ts_assert_or_print(
                Result.isErr(
                    assets.propose_commit_batch(preparer, {commit_args with operations = [commit_args.operations[1]]})
                ),
                "Should only be able to propose a batch once",
            );

            ts_assert_or_print(
                Result.isErr(
                    assets.create_chunk(preparer, { batch_id; content = "propose_commit_batch test chunk 4.\n" })
                ),
                "Should not be able to create chunks after proposing a batch",
            );

            let computed_evidence = switch(await* assets.compute_evidence(preparer, { batch_id; max_iterations = null })) {
                case (#ok(?evidence)) evidence;
                case (#ok(null)) {
                    ts_assert(false);
                    return ts_print("Error computing evidence: evidence is null");
                };
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error computing evidence: " # msg);
                };
            };

            ts_assert_or_print(
                Result.isOk(
                    await* assets.commit_proposed_batch(committer, {batch_id; evidence = computed_evidence})
                ),
                "[0xv5] Failed to commit batch",
            );

            ts_assert_or_print(
                Result.isErr(
                    await* assets.commit_proposed_batch(committer, {batch_id; evidence = computed_evidence})
                ),
                "Should not be able to commit a proposed batch that has already been committed",
            );

            let actual_asset_details = assets.get({ key = "/test/propose_commit/file.txt"; accept_encodings = ["identity"] });

            let expected_content : Blob = blob_concat(chunks);  
            let expected_asset_details : Result<Assets.EncodedAsset, Text> = #ok({
                content = expected_content;
                content_type = "text/plain";
                content_encoding = "identity";
                total_length = expected_content.size();
                sha256 = ?(Sha256.fromBlob(#sha256, expected_content));
            });

            ts_assert_or_print(
                actual_asset_details == expected_asset_details,
                "[0xv6] Failed to match asset content (actual vs expected): " # debug_show (actual_asset_details, expected_asset_details),
            );

            ts_assert_or_print(
                Option.isNull(Map.get(assets_internal.batches, nhash, batch_id)),
                "[0xv7] Proposed batch should be removed after committing",
            )
         }
    );

    func validate_chunks_range(key: Text, content_encoding: Text, chunks: [Blob], start: Nat, len: Nat) :  async () {
        Debug.print("Validating chunks range: " # debug_show start # " to " # debug_show Nat.min(chunks.size(), start + len));
        for (i in Itertools.range(start, Nat.min(chunks.size(), start + len))){
            let expected = chunks.get(i);
            
            let #ok({content = actual_chunk}) = assets.get_chunk({
                key;
                content_encoding;
                index = i;
                sha256 = null;
            });

            assert actual_chunk == expected;
        };
    };

    func validate_chunks(key: Text, content_encoding: Text, chunks: [Blob]) : async* (){
        var start = 0;
        let len = 50;

        let async_buffer = Buffer.Buffer<(async ())>((chunks.size() + 1) / len);

        while (start < chunks.size()){
            async_buffer.add(validate_chunks_range(key, content_encoding, chunks, start, len));
            start += len;
        };

        for (async_task in async_buffer.vals()){
            await async_task;
        };

    };

    suite.add(
        "create and store a large asset - 256MB",
        func ({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
            // create a batch with a single asset that is 1GB in size
            // verify that the asset is created correctly
            // verify each chunk

            let batch_id = switch (assets.create_batch(committer, {})) {
                case (#ok({ batch_id })) batch_id;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error creating batch: " # msg);
                };
            };

            let chunk_0 = chunk_2mb_blob;

            let #ok({chunk_id}) = (assets.create_chunk(committer, { batch_id; content = chunk_0}));

            let num_chunks = 128; // 128 * 2MB = 256MB

            let chunk_ids_for_200mb_file = Array.tabulate(num_chunks, func(_: Nat): Nat = chunk_id);

            let commit_args : Assets.CommitBatchArguments = {
                batch_id;
                operations = [
                    #CreateAsset({
                        key = "/test/commit/200mb-file";
                        content_type = "text/plain";
                        max_age = null;
                        headers = null;
                        enable_aliasing = null;
                        allow_raw_access = null;
                    }),
                    #SetAssetContent({
                        key = "/test/commit/200mb-file";
                        content_encoding = "identity";
                        chunk_ids = chunk_ids_for_200mb_file;
                        sha256 = null;
                    }),
                ];
            };


            ts_assert_or_print(
                Result.isOk(
                    await* assets.commit_batch(committer, commit_args)
                ),
                "[0xv4] Failed to commit batch",
            );
            

            let chunks = Array.tabulate(num_chunks, func(_: Nat): Blob = chunk_0);

            assert assets.get({ key = "/test/commit/200mb-file"; accept_encodings = ["identity"] }) == #ok({
                content = chunk_0;
                content_type = "text/plain";
                content_encoding = "identity";
                total_length = chunk_0.size() * num_chunks;
                sha256 = ?(await* Assets.hash_chunks(chunks))
            });

        }
    );


    suite.add(
        "list assets",
       func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
            // list and verify all the assets we created during the tests

            let assets_list = assets.list({});
            let asset_keys : [Text]= Array.map(assets_list, func(asset: Assets.AssetDetails): Text = asset.key);

            let expected_assets : [Text]= [
                "/test/asset/hello",
                "/test/asset/more_than_2mb",
                "/test/commit/file.json",
                "/test/commit/no-data",
                "/test/commit/200mb-file",
                "/test/propose_commit/file.txt",
            ];

            for (expected_asset in expected_assets.vals()) {
                ts_assert_or_print(
                    exists_in<Text>(asset_keys, Text.equal, expected_asset),
                    "Failed to find " # expected_asset # " in the list of assets",
                );
            };   
        }
    );

    suite.add_query(
        "retrieve asset via http_request",
        func({ ts_assert; ts_print; ts_assert_or_print} : CanisterTests.TestTools) : () {

            let http_request : Assets.HttpRequest = {
                method = "GET";
                url = "/test/asset/hello";
                headers = [];
                body : Blob = "";
                certificate_version = ?2;
            };

            let actual_http_response = switch(assets.http_request(http_request)){
                case (#ok(response)) response;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error making http request: " # msg);
                };
            };

            ts_assert_or_print(
                actual_http_response.status_code == 200,
                "[0xv8] Failed with status_code: " # debug_show actual_http_response.status_code,
            );

            ts_assert_or_print(
                actual_http_response.body == "👋 Hello, World!",
                "[0xv9] Failed to match asset content (actual vs expected): " # debug_show (actual_http_response.body, "👋 Hello, World!"),
            );

            func tuple_equal(a: (Text, Text), b: (Text, Text)) : Bool {
                (Text.toLowercase(a.0) == Text.toLowercase(b.0)) and (Text.toLowercase(a.1) == Text.toLowercase(b.1));
            };

            func tuple_key_equal(a: (Text, Text), b: (Text, Text)) : Bool {
                Text.toLowercase(a.0) == Text.toLowercase(b.0);
            };
            
            ts_assert_or_print(
                exists_in<(Text, Text)>(actual_http_response.headers, tuple_equal, ("content-type", "text/plain")),
                "[0xva] Failed to match expected headers",
            );

            ts_assert_or_print(
                exists_in<(Text, Text)>(actual_http_response.headers, tuple_key_equal, ("IC-Certificate", "")) and 
                exists_in<(Text, Text)>(actual_http_response.headers, tuple_key_equal, ("IC-CertificateExpression", "")),
                "[0xvb] Missing certificate headers",
            )
        },
    );

    suite.add(
        "supports fallback - store index.html files",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
            // verify that the canister supports the fallback method
            assert Result.isOk(
                assets.store(
                    committer,
                    {
                        key = "/test/fallback/index.html";
                        content = "fallback test";
                        content_type = "text/plain";
                        content_encoding = "identity";
                        sha256 = null;
                        is_aliased = null;
                    },
                )
            );

            assert Result.isOk(
                assets.store(
                    committer,
                    {
                        key = "/index.html";
                        content = "fallback test";
                        content_type = "text/plain";
                        content_encoding = "identity";
                        sha256 = null;
                        is_aliased = null;
                    },
                )
            );
        }
    );

    suite.add_query(
        "supports fallback - retrieve non-existent asset with fallback via http_request",
        func({ ts_assert; ts_print; ts_assert_or_print} : CanisterTests.TestTools) : () {

            var http_request : Assets.HttpRequest = {
                method = "GET";
                url = "/test/fallback/non-existent.html";
                headers = [];
                body : Blob = "";
                certificate_version = ?2;
            };

            var http_response = switch(assets.http_request(http_request)){
                case (#ok(response)) response;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error making http request: " # msg);
                };
            };

            func tuple_equal(a: (Text, Text), b: (Text, Text)) : Bool {
                if (b.1 == "" or a.1 == "") {
                    Text.toLowercase(a.0) == Text.toLowercase(b.0);
                } else {
                    (Text.toLowercase(a.0) == Text.toLowercase(b.0)) and (Text.toLowercase(a.1) == Text.toLowercase(b.1));
                };
            };

            func is_fallback_response(http_response: Assets.HttpResponse) : Bool {
                http_response.status_code == 200 and http_response.body == "fallback test" and exists_in<(Text, Text)>(http_response.headers, tuple_equal, ("content-type", "text/plain")) and
                exists_in<(Text, Text)>(http_response.headers, tuple_equal, ("IC-Certificate", "")) and 
                exists_in<(Text, Text)>(http_response.headers, tuple_equal, ("IC-CertificateExpression", ""));
            };
            
            ts_assert_or_print(
                is_fallback_response(http_response),
                "[0xvc] \'/test/fallback/non-existent.html\' should return fallback response",
            );

            http_request := { http_request with url = "test/fallback/unknown/path/non-existent.txt" };
            http_response := switch(assets.http_request(http_request)){
                case (#ok(response)) response;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error making http request: " # msg);
                };
            };
            ts_assert_or_print(
                is_fallback_response(http_response) ,
                "[0xvd] \'/test/fallback/unknown/path/non-existent.txt\' should return fallback response",
            );

            http_request := { http_request with url = "/non-existent.png" };
            http_response := switch(assets.http_request(http_request)){
                case (#ok(response)) response;
                case (#err(msg)) {
                    ts_assert(false);
                    return ts_print("Error making http request: " # msg);
                };
            };
            ts_assert_or_print(
                is_fallback_response(http_response),
                "[0xve] \'/non-existent.png\' should return fallback response",
            );

        },
    );

    suite.add(
        "delete assets",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () { 
            // choose any of the existing assets to delete
            // once deleted, verify that the asset is no longer present
            // by calling get, list_assets and all other relevant functions

            let assets_to_delete = [
                "/test/asset/more_than_2mb",
                "/test/commit/file.json",
                "/test/commit/no-data",
                "/test/commit/200mb-file",
                "/test/propose_commit/file.txt",
                "/test/asset/hello",
            ];

            for (asset in assets_to_delete.vals()) {
                ts_assert_or_print(
                    Result.isOk(
                        assets.delete_asset(committer, { key = asset })
                    ),
                    "Failed to delete asset: " # asset,
                );

                // ts_print("certified_endpoints: " # debug_show(Iter.toArray(get_certified_endpoints())));

                ts_assert_or_print(
                    not Itertools.any(
                        get_certified_endpoints(),
                        func(asset_details: CertifiedAssets.EndpointRecord): Bool {
                            asset_details.url == asset
                            and asset_details.status == 200;
                        }
                    ),
                    "Certified endpoint for " # asset # " should be removed after deleting the asset",
                );
            };
        }
    );


    suite.add(
        "owner revokes permissions",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {

            ts_assert_or_print(
                Result.isOk(
                    await* assets.revoke_permission(
                        owner,
                        {
                            permission = #Prepare;
                            of_principal = preparer;
                        },
                    )
                ),
                "[0xya] Owner failed to revoke Prepare permission",
            );

            let prepare_principals = assets.list_permitted({ permission = #Prepare });

            ts_assert_or_print(
                (prepare_principals) == [],
                "[0xyb] Prepare permission should be empty but got: " # debug_show(Array.map(prepare_principals, get_principal_tag)),
            );

            ts_assert_or_print(
                Result.isOk(
                    await* assets.revoke_permission(
                        owner,
                        {
                            permission = #Commit;
                            of_principal = committer;
                        },
                    )
                ),
                "[0xyc] Owner failed to revoke Commit permission",
            );

            let commit_principals = assets.list_permitted({ permission = #Commit });

            ts_assert_or_print(commit_principals == [owner, canister_id],
                "[0xyd] Only owner should have Commit permission now but got: " # debug_show(Array.map(commit_principals, get_principal_tag)),
            );

            

            ts_assert_or_print(
                Result.isOk(
                    await* assets.revoke_permission(
                        owner,
                        {
                            permission = #ManagePermissions;
                            of_principal = manager;
                        },
                    )
                ),
                "[0xye] Owner failed to revoke ManagePermissions permission",
            );

            let manage_permissions_principals = assets.list_permitted({ permission = #ManagePermissions });

            ts_assert_or_print(manage_permissions_principals == [], "[0xyf] ManagePermissions permission should be empty but got: " # debug_show(Array.map(manage_permissions_principals, get_principal_tag)));
        },
    );

    suite.add(
        "take ownership",
        func({ ts_assert; ts_print; ts_assert_or_print } : CanisterTests.TestTools) : async () {
            // A controller takes ownership of the canister

            for (none_controller in [no_permission, preparer, committer, manager].vals()) {
                ts_assert(
                    Result.isErr(
                        await* assets.take_ownership(none_controller)
                    )
                );
            };

            ts_assert(Result.isOk(await* assets.take_ownership(controller)));

            ts_assert(assets.list_permitted({ permission = #ManagePermissions }) == []);
            ts_assert(assets.list_permitted({ permission = #Prepare }) == []);
            ts_assert(assets.list_permitted({ permission = #Commit }) == [controller]);

        },
    );
};
