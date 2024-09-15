import Cycles "mo:base/ExperimentalCycles";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Buffer "mo:base/Buffer";
import Text "mo:base/Text";
import Blob "mo:base/Blob";
import Iter "mo:base/Iter";
import Nat8 "mo:base/Nat8";
import Nat32 "mo:base/Nat32";
import Char "mo:base/Char";
import Option "mo:base/Option";
import Error "mo:base/Error";
import Principal "mo:base/Principal";

import Map "mo:map/Map";
import Vector "mo:vector";
import HttpParser "mo:http-parser";
import Itertools "mo:itertools/Iter";
// import Web "mo:web-io";

import Assets "../src";

shared ({ caller = owner }) actor class () = this_canister {

    type File = {
        content_type : Text;
        chunk_ids : Buffer.Buffer<Nat>;
        exists : Bool;
        var is_committed : Bool;
    };

    let { nhash; thash } = Map;

    type Map<K, V> = Map.Map<K, V>;
    let raw_uploads : Map<Nat, Map<Nat, Blob>> = Map.new();
    let current_uploads : Map<Nat, Map<Text, File>> = Map.new();

    stable var assets_sstore = Assets.init_stable_store(owner);
    assets_sstore := Assets.migrate(assets_sstore);
    let assets = Assets.Assets(assets_sstore);

    public query func http_request_streaming_callback(token_blob : Assets.StreamingToken) : async (Assets.StreamingCallbackResponse) {
        assets.http_request_streaming_callback(token_blob);
    };

    // Acts as the initialization function for the canister.
    // > Warning: using the system timer will cause the Timer.mo library to not function properly.
    system func timer(setGlobalTimer : Nat64 -> ()) : async () {
        let id = _canister_id();
        assets.set_canister_id(id);
        assets.set_streaming_callback(http_request_streaming_callback);

        assert assets.get_streaming_callback() == ?http_request_streaming_callback;
        await* update_homepage();
    };

    func create_batch() : (Assets.BatchId) {
        let { batch_id } = assets.create_batch(owner, {});
        let new_batch = Map.new<Text, File>();
        ignore Map.put(current_uploads, nhash, batch_id, new_batch);
        batch_id;
    };

    func upload_chunks(batch_id : Assets.BatchId, chunks : [Blob]) : [Assets.ChunkId] {
        // let async_chunks = Buffer.Buffer<async Assets.CreateChunkResponse>(8);
        let chunk_ids = Buffer.Buffer<Assets.ChunkId>(8);

        for (chunk in chunks.vals()) {
            let { chunk_id } = assets.create_chunk(owner, { batch_id = batch_id; content = chunk });
            chunk_ids.add(chunk_id);
        };

        // for (async_chunk in async_chunks.vals()) {
        //     let { chunk_id } = await async_chunk;
        //     chunk_ids.add(chunk_id);
        // };

        return Buffer.toArray(chunk_ids);
    };

    func upload(batch_id : Nat, file_name : Text, content_type : Text, chunks : [Blob]) : async* () {
        let ?batch = Map.get(current_uploads, nhash, batch_id) else Debug.trap("Batch not found");
        let file = switch (Map.get(batch, thash, file_name)) {
            case (?file) file;
            case (null) {

                let file : File = {
                    content_type;
                    chunk_ids = Buffer.Buffer<Nat>(8);
                    exists = assets.exists(file_name);
                    var is_committed = false;
                };

                ignore Map.put(batch, thash, file_name, file);
                file;
            };
        };

        let chunk_ids = upload_chunks(batch_id, chunks);
        for (chunk_id in chunk_ids.vals()) {
            file.chunk_ids.add(chunk_id);
        };

    };

    func commit_batch(batch_id : Assets.BatchId) : async* () {
        let ?batch = Map.remove(current_uploads, nhash, batch_id) else Debug.trap("Batch not found");
        let operations = Buffer.Buffer<Assets.BatchOperationKind>(8);

        for ((file_name, file) in Map.entries(batch)) {
            let { content_type; chunk_ids; exists } = file;

            if (not exists) {
                operations.add(
                    #CreateAsset({
                        key = file_name;
                        content_type;
                        max_age = ?100_000_000_000;
                        headers = ?[];
                        enable_aliasing = if (file_name == "/homepage") ?true else ?false;
                        allow_raw_access = ?true;
                    }),

                );
            };

            operations.add(#SetAssetContent({ key = file_name; content_encoding = "identity"; chunk_ids = Buffer.toArray(chunk_ids); sha256 = null }));
        };

        // Debug.print("Committing batch: " # debug_show batch_id);
        // Debug.print("Operations: " # debug_show Buffer.toArray(operations));

        await* assets.commit_batch(
            owner,
            {
                batch_id;
                operations = Buffer.toArray(operations);
            },
        );

    };

    func homepage(files : [Assets.AssetDetails]) : Text {
        let files_list = Array.map(
            files,
            func(file : Assets.AssetDetails) : Text {
                let encoding = file.encodings[0];
                // Debug.print("homepage: " # debug_show (file.key, HttpParser.decodeURIComponent(HttpParser.encodeURI(file.key))));

                "<li style=\"display:\"flex\"; flex-direction:\"row\" align-items: \"space-between\"; width=\"1vw\" \">
                    <a href=\"" # HttpParser.encodeURI(file.key) # "\">" # file.key # "</a>
                    <span> - [" # file.content_type # "]      </span>
                    <span> -  <b>" # debug_show (encoding.length) # "</b> Bytes      </span>
                    <button>Delete</button>
                </li>";
            },
        );

        "
            <!DOCTYPE html>
            <html lang=\"en\">
            <head>
                <meta charset=\"UTF-8\">
                <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
                <title>Assets Canister Example</title>
            </head>
            <body>
                <h1>Assets Canister Example</h1>

                <input type=\"file\" class=\"file-input\" multiple>
                <input type=\"text\" class=\"file-name-input\" placeholder=\"add files to directory\">
                <button class=\"upload-button\">Upload</button>

                <h2>Files</h2>
                <ul class=\"item-list\">
                    " # Text.join("\n", files_list.vals()) # "
                </ul>

                <script>
                    document.querySelector('.upload-button').addEventListener('click', uploadFiles);

                    const MAX_CHUNK_SIZE = 1_887_436; // should be 2mb but is 1.8mb to account for the other data in the request like method and headers

                    async function uploadFiles() {
                        const fileInput = document.querySelector('.file-input');
                        const file_name_input = document.querySelector('.file-name-input');
                        const files = fileInput.files;
                        let prefix = file_name_input.value;

                        if (files.length > 0) {

                            const convert_to_bytes = async (formData) => {
                                return new Promise((resolve, reject) => {
                                    const reader = new FileReader();
                                    reader.readAsArrayBuffer(formData);
                                    reader.onloadend = () => {
                                        const array_buffer = reader.result;
                                        const byte_array = new Uint8Array(array_buffer);
                                        resolve(byte_array);
                                    };
                                });
                            };

                            const upload_chunks = async (batch_id, filename, content_type, chunks, start_id = 0) => {
                                return new Promise(async (resolve, reject) => {
                                    if (start_id >= chunks.length) return resolve();
                                    let chunk_requests = [];
                                    for (let chunk_id = start_id; chunk_id < chunks.length; chunk_id += 1) {
                                        let req = fetch(`/upload/${batch_id}/${chunk_id}?content-type=${content_type}&filename=${filename}&total_chunks=${chunks.length}`, {
                                            method: 'PUT',
                                            body: chunks[chunk_id]
                                        });

                                        chunk_requests.push(req);
                                    }

                                    Promise.all(chunk_requests)
                                        .then(() => resolve());
                                });
                            };

                            let batch_id = await fetch(`/upload`, { // should add number of files
                                method: 'POST',
                            })
                            .then((response) => response.text())
                            .then((text) => new Number(text));
                            console.log({ batch_id })

                            let prefixed_file_names = []
                            for (let i = 0; i < files.length; i++) {
                                const file = files[i];
                                let prefixedFileName = file.name;
                                prefix = '/' + prefix.split('/').filter((t) => t !== '').join('/')
                                if (prefix !== \"\") prefixedFileName= `${prefix}/${file.name}`;

                                console.log({prefixedFileName, prefix, filename: file.name})

                                const prefixedFile = new File([file], prefixedFileName, { type: file.type });
                                prefixed_file_names.push(prefixedFileName);

                                const file_bytes = await convert_to_bytes(prefixedFile);

                                const file_bytes_chunks = [];
                                for (let i = 0; i < file_bytes.length; i += MAX_CHUNK_SIZE) {
                                    file_bytes_chunks.push(file_bytes.slice(i, i + MAX_CHUNK_SIZE));
                                }

                                await upload_chunks(batch_id, prefixedFileName, file.type, file_bytes_chunks);
                            }

                            for (prefixed_file_name of prefixed_file_names) {
                                console.log(\"about to commit \", prefixed_file_name);
                                await fetch(`/upload/commit/${batch_id}?filename=${prefixed_file_name}`, {
                                    method: 'POST',
                                })
                                .catch(response => alert('Error:', response));
                            }

                            window.location.reload()

                        } else {
                            alert('Please select a file or directory to upload.');
                        }
                    }

                    function deleteFile(filename){
                        fetch(`/upload?filename=${filename}`, {
                            method: 'DELETE',
                        })
                        .then(response => response.text())
                        .then(data => {
                            console.log('Success:', data);
                            window.location.reload()
                        })
                        .catch(response => {
                            console.response('Error:', response);
                        });
                    }

                    for (const list_item of document.querySelectorAll('li')) {
                        let key = list_item.querySelector('a').textContent;
                        let button = list_item.querySelector('button');

                        button.addEventListener('click', () => {
                            console.log(\"about to delete \", key);
                            deleteFile(key);
                        })
                    }
                </script>
            </body>
            </html>
        ";
    };

    func update_homepage() : async* () {
        let files = (assets.list({}));
        let root = homepage(files);

        let batch_id = create_batch();
        await* upload(batch_id, "/homepage", "text/html", [Text.encodeUtf8(root)]);
        await* commit_batch(batch_id);
    };

    public composite query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
        assert ?_canister_id() == assets.get_canister_id();
        assert assets.get_streaming_callback() == ?http_request_streaming_callback;
        // Debug.print("request before parsing: " # debug_show { request with body = "" });

        if (request.method == "POST" or request.method == "PUT" or request.method == "DELETE") {
            return {
                upgrade = ?true;
                headers = [];
                status_code = 200;
                body = "";
                streaming_strategy = null;
            };
        };

        let url = HttpParser.URL(request.url, HttpParser.Headers([]));

        assets.http_request({ request with url = url.original });
    };

    func response(status_code : Nat16, message : Text) : Assets.HttpResponse {
        {
            status_code;
            body = Text.encodeUtf8(message);
            headers = [];
            streaming_strategy = null;
            upgrade = null;
        };
    };

    public func http_request_update(_request : Assets.HttpRequest) : async Assets.HttpResponse {
        try {
            let url = HttpParser.URL(_request.url, HttpParser.Headers([]));
            // Debug.print("request after parsing: " # debug_show { url = url.original });

            if (_request.method == "POST" and url.path.array[0] == "upload" and url.path.array.size() == 1) {
                // Debug.print("new request: creating batch");
                let batch_id = create_batch();
                // Debug.print("Create Upload batch: " # debug_show batch_id);
                return response(200, debug_show batch_id);

            } else if (_request.method == "PUT" and url.path.array[0] == "upload") {
                // Debug.print("uploading chunks: " # debug_show url.original);

                let batch_id = nat_from_text(url.path.array[1]);
                let chunk_id = nat_from_text(url.path.array[2]);

                let ?filename = url.queryObj.get("filename");

                // Debug.print("upload chunks: " # debug_show ({ batch_id; filename; chunk_id }));

                let files_batch = switch (Map.get(current_uploads, nhash, batch_id)) {
                    case (?files_batch) { files_batch };
                    case (null) {
                        let files_batch = Map.new<Text, File>();
                        ignore Map.put(current_uploads, nhash, batch_id, files_batch);
                        files_batch;
                    };
                };

                let file = switch (Map.get(files_batch, thash, filename)) {
                    case (?file) file;
                    case (null) {
                        let ?content_type = url.queryObj.get("content-type");
                        // Debug.print("Create new file: " # debug_show ({ batch_id; filename; content_type }));
                        let ?total_chunks_text = (url.queryObj.get("total_chunks"));
                        let total_chunks = nat_from_text(total_chunks_text);

                        let file : File = {
                            content_type;
                            chunk_ids = Buffer.fromArray<Nat>(
                                Array.tabulate<Nat>(
                                    total_chunks,
                                    func(i : Nat) : Nat { 0 },
                                )
                            );
                            exists = assets.exists(filename);
                            var is_committed = false;
                        };

                        ignore Map.put(files_batch, thash, filename, file);
                        file;

                    };
                };

                let uploaded_chunk_ids = upload_chunks(batch_id, [_request.body]);

                // for (uploaded_chunk_id in uploaded_chunk_ids.vals()) {
                //     file.chunk_ids.add(uploaded_chunk_id);
                // };

                file.chunk_ids.put(chunk_id, uploaded_chunk_ids[0]);

                return response(200, "Successfully uploaded chunk " # debug_show ({ batch_id; filename; chunk_id }));

            } else if (_request.method == "POST" and url.path.array[0] == "upload" and url.path.array[1] == "commit") {
                let parts = url.path.array;
                // Debug.print("committing: " # debug_show parts);

                let batch_id = nat_from_text(parts[2]);
                let ?filename = url.queryObj.get("filename");

                // Debug.print("Commit Upload batch: " # debug_show batch_id);

                let ?files_batch = Map.get(current_uploads, nhash, batch_id) else return response(404, "Batch not found");
                let ?file = Map.get(files_batch, thash, filename) else return response(404, "File not found");

                file.is_committed := true;

                if (Itertools.all(Map.vals(files_batch), func(file : File) : Bool { file.is_committed })) {

                    // Debug.print("Committing batch: " # debug_show batch_id);
                    await* commit_batch(batch_id);
                    await* update_homepage();
                };

                ignore Map.remove(current_uploads, nhash, batch_id);

                return response(200, debug_show batch_id);

            } else if (_request.method == "DELETE" and url.path.array[0] == "upload") {

                let ?key = url.queryObj.get("filename");

                // Debug.print("deleting: " # debug_show key);
                assets.delete_asset(owner, { key });
                await* update_homepage();

                return response(200, "Successfully deleted " # debug_show key);
            };

            return response(404, "Not found");
        } catch (e) {
            Debug.print("this might be a trap!");
            Debug.print("Error: " # debug_show (Error.code(e), Error.message(e)));
            // return response(500, "Internal Server Error");
            throw e;
        };
    };

    func _canister_id() : Principal {
        Principal.fromActor(this_canister);
    };

    func nat_from_text(text : Text) : Nat {
        var n : Nat = 0;

        for (c in text.chars()) {
            if (Char.isDigit(c)) {
                n := n * 10 + Nat32.toNat(Char.toNat32(c) - Char.toNat32('0'));
            } else {
                Debug.trap("Invalid character in number: " # Char.toText(c));
            };
        };

        return n;
    };

};
