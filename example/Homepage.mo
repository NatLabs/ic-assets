import Array "mo:base/Array";
import Text "mo:base/Text";

import HttpParser "mo:http-parser";

import Assets "../src";

module {
    public func homepage(files : [Assets.AssetDetails]) : Text {
        let files_list = Array.map(
            files,
            func(file : Assets.AssetDetails) : Text {
                let encoding = file.encodings[0];
                // Debug.print("homepage: " # debug_show (file.key, HttpParser.decodeURIComponent(HttpParser.encodeURI(file.key))));

                "<li style=\"display:\"flex\"; flex-direction:\"row\" align-items: \"space-between\"; width=\"1vw\" \">
                    <a href=\"" # HttpParser.encodeURI(file.key) # "\">" # file.key # "</a>
                    <span> - [" # file.content_type # "]      </span>
                    <span> -  <b>" # debug_show (encoding.length) # "</b> Bytes      </span>
                    " # (
                    if (file.key == "/homepage" or file.key == "/fallback/index.html") {
                        "<button disabled>Delete</button>";
                    } else { "<button>Delete</button>" }
                ) # "
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

                    const MAX_CHUNK_SIZE = 1_887_436; // should be 2mb but is 1.8mb to account for the additional data in the request like the method, headers, query params, etc.
                    const BATCH_SIZE = 10;

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

                            const run_batch = async (batch_id, filename, content_type, chunks, start_id, total_chunks) => {
                                return new Promise(async (resolve, reject) => {
                                    let chunk_requests = [];
                                    for (let i = 0; i < chunks.length; i += 1) {
                                        let req = fetch(`/upload/${batch_id}/${start_id + i}?content-type=${content_type}&filename=${filename}&total_chunks=${total_chunks}`, {
                                            method: 'PUT',
                                            body: chunks[i]
                                        });

                                        chunk_requests.push(req);
                                    }

                                    Promise.all(chunk_requests)
                                        .then(() => resolve());
                                });
                            };


                            const upload_chunks = async (batch_id, filename, content_type, chunks) => {
                                return new Promise(async (resolve, reject) => {

                                    let start_id = 0;


                                    while (start_id < chunks.length) {
                                        await run_batch(batch_id, filename, content_type, chunks.slice(start_id, start_id + BATCH_SIZE), start_id, chunks.length);
                                        start_id += BATCH_SIZE;
                                    }

                                    resolve();

                                })

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
};
