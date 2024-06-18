import Debug "mo:base/Debug";
import Iter "mo:base/Iter";
import Principal "mo:base/Principal";
import None "mo:base/None";
import Cycles "mo:base/ExperimentalCycles";
import Buffer "mo:base/Buffer";
import Option "mo:base/Option";
import Map "mo:map/Map";

import { test; suite } "mo:test/async";
import Itertools "mo:itertools/Iter";

import BaseAsset "../src/BaseAsset";
import Assets "../src";
import Migrations "../src/Migrations";

import AssetsCanister "../src/Canister";

actor class() = this_canister{

    type Buffer<A> = Buffer.Buffer<A>;
    type Map<K, V> = Map.Map<K, V>;

    // let canister_id = Principal.fromText("r7inp-6aaaa-aaaaa-aaabq-cai");
    // let owner = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");
    let caller = Principal.fromText("tde7l-3qaaa-aaaah-qansa-cai");
    let preparer = Principal.fromText("rrkah-fqaaa-aaaaa-aaaaq-cai");

    // let test_data = Map.new<Text, Map<Assets.Key, Assets.AssetDetails>>();

    // let test1 = Map.new<Assets.Key, Assets.SharedAsset>();
    // Map.put(test1, "/hello", {
    //     key = "/hello";
    //     encodings: [{
    //         modified = 0;
    //         content_encoding = "identity";
    //     }];
    //     content_type = "text/plain";
    //     headers = [];
    //     is_aliased = ?true;
    //     max_age = null;
    //     allow_raw_access = null;
    // });

    // Map.put(test_data, "test1", Map.new<Assets.Key, Assets.AssetDetails>());

    func permissions(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {

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

    };

    let hello_world_gzip : BaseAsset.StoreArgs = {
        key = "/hello";
        // "Hello, World!"
        content = "\1f\8b\08\00\00\00\00\00\00\03\f3\48\cd\c9\c9\d7\51\08\cf\2f\ca\49\51\e4\02\00\84\9e\e8\b4\0e\00\00\00";
        sha256 = null;
        content_type = "text/plain";
        content_encoding = "gzip";
        is_aliased = ?true;
    };

    let hello_world_identity : BaseAsset.StoreArgs = {
        key = "/hello";
        content = "Hello, World!";
        sha256 = null;
        content_type = "text/plain";
        content_encoding = "identity";
        is_aliased = ?true;
    };

    func store_and_get_asset(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {

        let permission_request : Assets.GrantPermission = {
            permission = #ManagePermissions;
            to_principal = caller;
        };

        await asset.grant_permission(permission_request);

        await asset.store(hello_world_gzip);

        let get_args : Assets.GetArgs = {
            key = "/hello";
            accept_encodings = ["gzip"];
        };

        let result = await asset.get(get_args);

        assert result.content == hello_world_gzip.content;
        assert result.content_type == hello_world_gzip.content_type;
        assert result.content_encoding == hello_world_gzip.content_encoding;
        assert result.total_length == hello_world_gzip.content.size();

        // let asset_properties = await asset.get_asset_properties("/hello");

        // assert asset_properties.max_age == null;
        // assert asset_properties.headers == null;
        // assert asset_properties.allow_raw_access == null;
        // assert asset_properties.is_aliased == ?true;

    };

    func get_asset_from_aliases(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {
        let alias1 : Assets.GetArgs = {
            key = "/hello.html";
            accept_encodings = ["gzip"];
        };

        let result1 = await asset.get(alias1);

        assert result1.content == hello_world_gzip.content;
        assert result1.content_type == hello_world_gzip.content_type;
        assert result1.content_encoding == hello_world_gzip.content_encoding;
        assert result1.total_length == hello_world_gzip.content.size();

        let result1_properties = await asset.get_asset_properties("/hello.html");

        assert result1_properties.max_age == null;
        assert result1_properties.headers == null;
        assert result1_properties.allow_raw_access == null;
        assert result1_properties.is_aliased == ?true;

        let alias2 : Assets.GetArgs = {
            key = "/hello/index.html";
            accept_encodings = ["gzip"];
        };

        let result2 = await asset.get(alias2);

        assert result2.content == hello_world_gzip.content;
        assert result2.content_type == hello_world_gzip.content_type;
        assert result2.content_encoding == hello_world_gzip.content_encoding;
        assert result2.total_length == hello_world_gzip.content.size();

        let result2_properties = await asset.get_asset_properties("/hello/index.html");

        assert result2_properties.max_age == null;
        assert result2_properties.headers == null;
        assert result2_properties.allow_raw_access == null;
        assert result2_properties.is_aliased == ?true;

    };

    func delete_hello_world_asset(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {

        await asset.delete_asset({ key = "/hello" });

        let get_args2 : Assets.GetArgs = {
            key = "/hello";
            accept_encodings = ["gzip"];
        };

        // let res = await asset.get(get_args2); // will fail
        // Debug.print("deleted result: " # debug_show res);

    };

    func configure(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {
        var config = await asset.get_configuration();

        assert config.max_batches == null;
        assert config.max_chunks == null;
        assert config.max_bytes == null;

        await asset.configure({
            max_batches = ??100;
            max_chunks = ??100_000;
            max_bytes = ??100_000_000_000;
        });

        config := await asset.get_configuration();

        assert config.max_batches == ?100;
        assert config.max_chunks == ?100_000;
        assert config.max_bytes == ?100_000_000_000;

    };

    let hello_world_identity_chunks : [Blob] = ["H", "e", "l", "l", "o", ",", " ", "W", "o", "r", "l", "d", "!"];

    func upload_chunks(asset : AssetsCanister.AssetsCanister, batch_id : Assets.BatchId, chunks : [Blob]) : async* [Assets.ChunkId] {
        let async_chunks = Buffer.Buffer<async Assets.CreateChunkResponse>(8);

        for (chunk in chunks.vals()) {
            async_chunks.add(asset.create_chunk({ batch_id = batch_id; content = chunk }));
        };

        let chunk_ids = Buffer.Buffer<Assets.ChunkId>(8);

        for (async_chunk in async_chunks.vals()) {
            let { chunk_id } = await async_chunk;
            chunk_ids.add(chunk_id);
        };

        return Buffer.toArray(chunk_ids);
    };

    func create_and_commit_batch(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {
        // create batch
        let { batch_id } = await asset.create_batch({});

        // upload chunks in parallel
        let chunk_ids = await* upload_chunks(asset, batch_id, hello_world_identity_chunks);

        let commit_args : Assets.CommitBatchArguments = {
            batch_id;
            operations = [
                #CreateAsset({
                    key = "/hello";
                    content_type = "text/plain";
                    max_age = ?100_000_000_000;
                    headers = ?[("content-length", debug_show (hello_world_identity_chunks.size()))];
                    enable_aliasing = ?true;
                    allow_raw_access = ?true;
                }),
                #SetAssetContent({
                    key = "/hello";
                    content_encoding = "identity";
                    chunk_ids;
                    sha256 = null;
                }),
            ];
        };

        await asset.commit_batch(commit_args);

        let get_args : Assets.GetArgs = {
            key = "/hello";
            accept_encodings = ["gzip"];
            batch_id = batch_id;
        };

        let result = await asset.get(get_args);

        assert result.content == hello_world_gzip.content;
        assert result.content_type == hello_world_gzip.content_type;
        assert result.content_encoding == hello_world_gzip.content_encoding;
        assert result.total_length == hello_world_gzip.content.size();

        let result1_properties = await asset.get_asset_properties("/hello");

        assert result1_properties.max_age == ?100_000_000_000;
        assert result1_properties.headers == ?[("content-length", debug_show (hello_world_identity_chunks.size()))];
        assert result1_properties.allow_raw_access == ?true;
        assert result1_properties.is_aliased == ?true;

    };

    func get_asset_chunks(asset : AssetsCanister.AssetsCanister, owner : Principal, key : Assets.Key, chunks : [Blob]) : async* () {

        for (i in Iter.range(0, hello_world_identity_chunks.size() - 1)) {
            let get_chunk_args : Assets.GetChunkArgs = {
                index = i;
                key = "/hello";
                content_encoding = "identity";
                sha256 = null;
            };

            let result = await asset.get_chunk(get_chunk_args);
            assert result.content == hello_world_identity_chunks[i];
        };
    };

    func propose_and_commit_batch(asset : AssetsCanister.AssetsCanister, owner : Principal) : async* () {
        await asset.revoke_permission({
            permission = #Commit;
            of_principal = owner;
        });
        await asset.grant_permission({
            permission = #Prepare;
            to_principal = owner;
        });

        // create batch
        let { batch_id } = await asset.create_batch({});

        // upload chunks in parallel
        let chunk_ids = await* upload_chunks(asset, batch_id, hello_world_identity_chunks);

        let commit_args : Assets.CommitBatchArguments = {
            batch_id;
            operations = [
                #CreateAsset({
                    key = "/hello";
                    content_type = "text/plain";
                    max_age = null;
                    headers = null;
                    enable_aliasing = ?true;
                    allow_raw_access = null;
                }),
                #SetAssetContent({
                    key = "/hello";
                    content_encoding = "identity";
                    chunk_ids;
                    sha256 = null;
                }),
            ];
        };

        await asset.propose_commit_batch(commit_args);
        let opt_evidence_blob = await asset.compute_evidence({
            batch_id;
            max_iterations = null;
        });
        assert Option.isSome(opt_evidence_blob);

        await asset.grant_permission({
            permission = #Commit;
            to_principal = owner;
        });

        let ?evidence = opt_evidence_blob else Debug.trap("evidence is null");
        await asset.commit_proposed_batch({ batch_id; evidence });

        let get_args : Assets.GetArgs = {
            key = "/hello";
            accept_encodings = ["gzip"];
            batch_id = batch_id;
        };

        let result = await asset.get(get_args);

        assert result.content == hello_world_gzip.content;
        assert result.content_type == hello_world_gzip.content_type;
        assert result.content_encoding == hello_world_gzip.content_encoding;
        assert result.total_length == hello_world_gzip.content.size();

    };

    // public func propose_another_batch_before_previous_is_committed
    var asset_canister_id : ?Principal = null;

    public query func asset_id() : async ?Principal { asset_canister_id };

    public func run_tests() : async () {
        Cycles.add<system>(200_000_000_000);
        let asset = await AssetsCanister.AssetsCanister ( #Init({}));
        await asset.init();
        asset_canister_id := ?Principal.fromActor(asset);
        let authorized = await asset.list_authorized();
        let owner = authorized[0];

        await suite(
            "BaseAsset Service Test",
            func() : async () {

                await test(
                    "permissions",
                    func() : async () { await* permissions(asset, owner) },
                );

                await test(
                    "store and get asset",
                    func() : async () {
                        await* store_and_get_asset(asset, owner);
                    },
                );

                await test(
                    "get asset from aliases",
                    func() : async () {
                        await* get_asset_from_aliases(asset, owner);
                    },
                );

                await test(
                    "list assets",
                    func() : async () {
                        let assets = await asset.list({});
                        let first = assets[0];

                        assert first.key == "/hello";
                        assert first.content_type == "text/plain";

                        let first_encoding = first.encodings[0];
                        assert first_encoding.content_encoding == "gzip";
                        assert first_encoding.length == hello_world_gzip.content.size();

                    },
                );

                await test(
                    "delete asset",
                    func() : async () {
                        await* delete_hello_world_asset(asset, owner);
                    },
                );

                await test(
                    "configure",
                    func() : async () {
                        await* configure(asset, owner);
                    },
                );

                await test(
                    "create and commit batch",
                    func() : async () {
                        await* create_and_commit_batch(asset, owner);
                    },
                );

                await test(
                    "get asset chunks",
                    func() : async () {
                        await* get_asset_chunks(asset, owner, "/hello", hello_world_identity_chunks);
                        await* get_asset_chunks(asset, owner, "/hello.html", hello_world_identity_chunks);
                        await* get_asset_chunks(asset, owner, "/hello/index.html", hello_world_identity_chunks);
                    },
                );

                await test(
                    "propose and commit batch",
                    func() : async () {
                        await* propose_and_commit_batch(asset, owner);
                    },
                );

            },
        );
    };

};
