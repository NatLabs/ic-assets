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
// import Vector "mo:vector";
import HttpParser "mo:http-parser";
import Itertools "mo:itertools/Iter";
// import Web "mo:web-io";

import Assets "../src";

shared ({ caller = owner }) actor class () = this_canister {

    stable var assets_sstore = Assets.init_stable_store(owner);
    assets_sstore := Assets.migrate(assets_sstore);
    let assets = Assets.Assets(assets_sstore);

    public query func http_request_streaming_callback(token : Assets.StreamingToken) : async (Assets.StreamingCallbackResponse) {
        assets.http_request_streaming_callback(token);
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

    type File = {
        content_type : Text;
        chunk_ids : Buffer.Buffer<Nat>;
        exists : Bool;
        var is_committed : Bool;
    };

    let { nhash; thash } = Map;

    type Map<K, V> = Map.Map<K, V>;
    let raw_uploads : Map<Nat, Map<Nat, Blob>> = Map.new();
    stable let current_uploads : Map<Nat, Map<Text, File>> = Map.new();

    let router = Web.Router();

    public composite query func http_request(request : Assets.HttpRequest) : async Assets.HttpResponse {
        assert ?_canister_id() == assets.get_canister_id();
        assert assets.get_streaming_callback() == ?http_request_streaming_callback;

        router.process_request(request, null);
    };

    public func http_request_update(request : Assets.HttpRequest) : async Assets.HttpResponse {
        router.process_request_update(request, null);
    };

    router.post(
        "/upload",
        func(
            req : Web.Request,
            res : Web.ResponseBuilder,
        ) {
            let batch_id = create_batch();
            Debug.print("Create Upload batch: " # debug_show batch_id);
            res.status(200).text(debug_show batch_id);
        },
    );

    router.put(
        "/upload/:batch_id/:filename/:chunk_id",
        func(req : Web.Request, res : Web.ResponseBuilder) = do ? {
            let batch_id = nat_from_text(req.params.get("batch_id")!);
            let filename = req.params.get("filename")!;
            let chunk_id = nat_from_text(req.params.get("chunk_id")!);

            Debug.print("upload chunks: " # debug_show ({ batch_id; filename; chunk_id }));

            let ?files_batch = switch (Map.get(current_uploads, nhash, batch_id)) {
                case (?files_batch) {
                    if (chunk_id == 0) {
                        files_batch.is_committed := false;
                    };

                    files_batch;
                };
                case (null) {
                    let ?content_type = req.headers.get("content-type") else return res.status(404).text("Content-Type not found");

                    let file : File = {
                        content_type;
                        chunk_ids = Buffer.Buffer<Nat>(8);
                        exists = assets.exists(filename);
                        var is_committed = false;
                    };

                    let files_batch = Map.new<Text, File>();
                    ignore Map.put(files_batch, thash, filename, file);
                    ignore Map.put(current_uploads, nhash, batch_id, files_batch);

                };
            };

            let ?file = Map.get(files_batch, thash, filename) else return res.status(404).text("File not found");

            let uploaded_chunk_ids = upload_chunks(batch_id, [req.body]);

            for (uploaded_chunk_id in uploaded_chunk_ids.vals()) {
                file.chunk_ids.add(uploaded_chunk_id);
            };

            res.status(200).text("Successfully uploaded chunk " # debug_show ({ batch_id; filename; chunk_id }));

        },
    );

    router.post(
        "/upload/commit/:batch_id/:filename",
        func(req : Web.Request, res : Web.ResponseBuilder) {

            let batch_id_text = req.params.get("batch_id") else return res.status(404).text("no batch id");
            let batch_id = nat_from_text(batch_id_text);

            let filename = req.params.get("filename") else return res.status(404).text("no filename");

            let ?files_batch = Map.get(current_uploads, nhash, batch_id) else return res.status(404).text("Batch not found");
            let ?file = Map.get(files_batch, thash, filename) else return res.status(404).text("File not found");

            file.is_committed := true;

            if (Itertools.all(files_batch.vals(), func(file) : Bool { file.is_committed })) {

                Debug.print("Committing batch: " # debug_show batch_id);
                await* commit_batch(batch_id);
                await* update_homepage();
            };

            ignore Map.remove(current_uploads, nhash, batch_id);

            res.status(200).text(debug_show batch_id);

        },
    );

    router.delete(
        "/upload/:key",
        func(req : Web.Request, res : Web.ResponseBuilder) {
            let ?key = req.params.get("key") else return res.status(404).text("no key");

            Debug.print("deleting: " # debug_show key);
            assets.delete_asset(owner, { key });
            await* update_homepage();

            return response(200, "Successfully deleted " # debug_show key);

        },
    );

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
                    is_committed = false;
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

        Debug.print("Committing batch: " # debug_show batch_id);
        Debug.print("Operations: " # debug_show Buffer.toArray(operations));

        await* assets.commit_batch(
            owner,
            {
                batch_id;
                operations = Buffer.toArray(operations);
            },
        );

    };

    func update_homepage() : async* () {
        let files = (assets.list({}));
        let root = homepage(files);

        let batch_id = create_batch();
        await* upload(batch_id, "/homepage", "text/html", [Text.encodeUtf8(root)]);
        await* commit_batch(batch_id);
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

    func homepage(files : [Assets.AssetDetails]) : Text {
        let files_list = Array.map(
            files,
            func(file : Assets.AssetDetails) : Text {
                let encoding = file.encodings[0];

                "<li style=\"display:\"flex\"; flex-direction:\"row\" align-items: \"space-between\"; width=\"1vw\" \">
                    <a href=\"" # file.key # "\">" # file.key # "</a>
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
                    document.querySelector('.upload-button').addEventListener('click', uploadFile);

                    async function uploadFile() {
                        const fileInput = document.querySelector('.file-input');
                        const file_name_input = document.querySelector('.file-name-input');
                        const files = fileInput.files;
                        let prefix = file_name_input.value;

                        if (files.length > 0) {
                            const formData = new FormData();
                            for (let i = 0; i < files.length; i++) {
                                const file = files[i];
                                let prefixedFileName = file.name;
                                prefix = '/' + prefix.split('/').filter((t) => t !== '').join('/')
                                if (prefix !== \"\") prefixedFileName= `${prefix}/${file.name}`;

                                const prefixedFile = new File([file], prefixedFileName, { type: file.type });
                                formData.append('files[]', prefixedFile);
                            }

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

                            const upload_chunks = async (batch_id, chunks, start_id) => {
                                return new Promise(async (resolve, reject) => {
                                    if (start_id >= chunks.length) return resolve();
                                    let chunk_requests = [];
                                    for (let chunk_id = start_id; chunk_id < chunks.length; chunk_id += 1) {
                                        let req = fetch(`/upload/${batch_id}/${chunk_id}`, {
                                            method: 'PUT',
                                            body: chunks[chunk_id]
                                        });

                                        chunk_requests.push(req);
                                    }

                                    Promise.all(chunk_requests)
                                        .then(() => resolve());
                                });
                            };

                            const form_data_to_blob = async (formData) => {
                                const boundary = '----WebKitFormBoundary' + Math.random().toString(36).substr(2);

                                // Convert FormData to an array of form-data parts
                                const formDataParts = [];
                                for (const [name, value] of formData.entries()) {
                                    if (value instanceof File) {
                                        formDataParts.push(
                                            `--${boundary}\\r\\n` +
                                            `Content-Disposition: form-data; name=\"${name}\"; filename=\"${value.name}\"\\r\\n` +
                                            `Content-Type: ${value.type}\\r\\n\\r\\n`
                                        );

                                        const array_buffer = await convert_to_bytes(value);
                                        const uint8_array = new Uint8Array(array_buffer);
                                        formDataParts.push(uint8_array);
                                        formDataParts.push('\\r\\n');

                                    } else {
                                        formDataParts.push(
                                            `--${boundary}\\r\\n` +
                                            `Content-Disposition: form-data; name=\"${name}\"\\r\\n\\r\\n`
                                        );

                                        const array_buffer = await readFileAsArrayBuffer(value);
                                        const uint8_array = new Uint8Array(array_buffer);
                                        formDataParts.push(uint8_array);
                                        formDataParts.push(`\\r\\n`);
                                    }
                                }
                                formDataParts.push(`--${boundary}--\\r\\n`);

                                // Create a Blob from the form-data parts
                                let blob =  new Blob(formDataParts, { type: 'multipart/form-data; boundary=' + boundary });
                                return await convert_to_bytes(blob);
                            }

                            let form_data_array = await form_data_to_blob(formData);

                            console.log({form_data_array})
                            let chunks = [];
                            let chunk_size = (1024 ** 2) * 1.5;
                            for (let i = 0; i < form_data_array.length; i += i + chunk_size) {
                                chunks.push(form_data_array.slice(i, i + chunk_size));
                            }

                            console.log({chunks})

                            let response = await fetch('/upload', {
                                method: 'POST',
                                body: chunks[0]
                            });

                            let response_text = await response.text();
                            console.log({ response_text })
                            let batch_id = new Number(response_text);
                            console.log({ batch_id })

                            await upload_chunks(batch_id, chunks, 1);

                            await fetch('/upload/commit', {
                                method: 'POST',
                                body: batch_id
                            })
                            .then(_x => window.location.reload())
                            .catch(response => alert('Error:', response));

                        } else {
                            alert('Please select a file or directory to upload.');
                        }
                    }

                    function deleteFile(key){
                        fetch('/upload', {
                            method: 'DELETE',
                            body: key
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

};
