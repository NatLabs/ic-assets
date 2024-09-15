document.querySelector(".upload-button").addEventListener("click", uploadFile);

const MAX_CHUNK_SIZE = 1024 * 1024 * 2;

async function uploadFile() {
  const fileInput = document.querySelector(".file-input");
  const file_name_input = document.querySelector(".file-name-input");
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

    const upload_chunks = async (
      batch_id,
      filename,
      content_type,
      chunks,
      start_id = 0
    ) => {
      return new Promise(async (resolve, reject) => {
        if (start_id >= chunks.length) return resolve();
        let chunk_requests = [];
        for (let chunk_id = start_id; chunk_id < chunks.length; chunk_id += 1) {
          let req = fetch(
            `/upload/${batch_id}/${filename}/${chunk_id}?content-type=${content_type}`,
            {
              method: "PUT",
              body: chunks[chunk_id],
            }
          );

          chunk_requests.push(req);
        }

        Promise.all(chunk_requests).then(() => resolve());
      });
    };

    let batch_id = await fetch("/upload", {
      method: "POST",
    })
      .then((response) => response.text())
      .then((text) => new Number(text));

    console.log({ batch_id });

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      let prefixedFileName = file.name;
      prefix =
        "/" +
        prefix
          .split("/")
          .filter((t) => t !== "")
          .join("/");
      if (prefix !== "") prefixedFileName = `${prefix}/${file.name}`;

      console.log({ prefixedFileName, prefix, filename: file.name });

      const prefixedFile = new File([file], prefixedFileName, {
        type: file.type,
      });

      const file_bytes = await convert_to_bytes(prefixedFile);

      const file_bytes_chunks = [];
      for (let i = 0; i < file_bytes.length; i += MAX_CHUNK_SIZE) {
        file_bytes_chunks.push(file_bytes.slice(i, i + MAX_CHUNK_SIZE));
      }

      await upload_chunks(
        batch_id,
        prefixedFileName,
        file.type,
        file_bytes_chunks
      );

      await fetch(`/upload/commit/${batch_id}/${file.name}`, {
        method: "POST",
        body: batch_id,
      })
        .then((_x) => window.location.reload())
        .catch((response) => alert("Error:", response));
    }
  } else {
    alert("Please select a file or directory to upload.");
  }
}

function deleteFile(key) {
  fetch(`/upload/${key}`, {
    method: "DELETE",
  })
    .then((response) => response.text())
    .then((data) => {
      console.log("Success:", data);
      window.location.reload();
    })
    .catch((response) => {
      console.response("Error:", response);
    });
}

for (const list_item of document.querySelectorAll("li")) {
  let key = list_item.querySelector("a").textContent;
  let button = list_item.querySelector("button");

  button.addEventListener("click", () => {
    console.log("about to delete ", key);
    deleteFile(key);
  });
}
