import Buffer "mo:base/Buffer";
let hello_file = Assets.StoreArgs {
    key = "/assets/hello.txt";
    content_type = "text/plain";
    content = "Hello, World!";
    sha256 = null;
    content_encoding = "identity";
    is_aliased = ?true;
};

let numbers_file = Assets.StoreArgs {
    key = "/assets/numbers.txt";
    content_type = "text/plain";
    content = "1, 2, 3, 4, 5";
    sha256 = null;
    content_encoding = "identity";
    is_aliased = ?true;
};

let batch_id = await assets.create_batch();

let hello_chunks = Assets.split_into_chunks(hello_file.content);
let hello_chunk_ids_in_order = Buffer.Buffer(hello_chunks.size());

for (chunk in hello_chunks.vals()) {
    let chunk_id = await assets.create_chunk(batch_id, chunk);
    hello_chunk_ids_in_order.add(chunk_id);
};

let numbers_chunks = Assets.split_into_chunks(numbers_file.content);
let numbers_chunk_ids_in_order = Buffer.Buffer(numbers_chunks.size());

for (chunk in numbers_chunks.vals()) {
    let chunk_id = await assets.create_chunk(batch_id, chunk);
    numbers_chunk_ids_in_order.add(chunk_id);
};

let create_hello_file_args = {
    key = hello_file.key;
    content_type = hello_file.content_type;
    max_age = null;
    headers = null;
    enable_aliasing = null;
    allow_raw_access = ?false;
};

let set_hello_file_content_args = {
    key = hello_file.key;
    content_encoding = hello_file.content_encoding;
    chunk_ids = Buffer.toArray(hello_chunk_ids_in_order);
    sha256 = null;
};

let create_numbers_file_args = {
    key = numbers_file.key;
    content_type = numbers_file.content_type;
    max_age = null;
    headers = null;
    enable_aliasing = null;
    allow_raw_access = ?false;
};

let set_numbers_file_content_args = {
    key = numbers_file.key;
    content_encoding = numbers_file.content_encoding;
    chunk_ids = Buffer.toArray(numbers_chunk_ids_in_order);
    sha256 = null;
};

let operations = [
    #CreateAssetArguments(create_hello_file_args),
    #SetAssetContentArguments(set_hello_file_content_args),
    #CreateAssetArguments(create_numbers_file_args),
    #SetAssetContentArguments(set_numbers_file_content_args),
];

await assets.commit_batch(batch_id, operations);
