const EMPTY_FILE_LABEL = "No file selected";
const MARGIN_PROPERTIES = [
  "marginTop",
  "marginRight",
  "marginBottom",
  "marginLeft",
];

function selectedFileLabel(input) {
  const files = Array.from(input.files || []);
  if (!files.length) return EMPTY_FILE_LABEL;
  if (files.length === 1) return files[0].name;
  return `${files.length} files selected`;
}

function transferExternalSpacing(input, wrapper) {
  const styles = window.getComputedStyle(input);
  MARGIN_PROPERTIES.forEach((property) => {
    wrapper.style[property] = styles[property];
    input.style[property] = "0";
  });
}

function enhanceFileInput(input) {
  if (
    input.hidden ||
    input.hasAttribute("data-file-dropzone-input") ||
    input.getAttribute("data-file-input-enhanced") === "true"
  ) return;

  const parent = input.parentNode;
  if (!parent) return;

  const wrapper = document.createElement("span");
  wrapper.className = "lt-file-input";
  transferExternalSpacing(input, wrapper);

  const button = document.createElement("span");
  button.className = "lt-file-input__button";
  button.setAttribute("aria-hidden", "true");
  button.textContent = input.multiple ? "Choose files" : "Choose file";

  const filename = document.createElement("span");
  filename.className = "lt-file-input__name";
  filename.setAttribute("aria-live", "polite");

  parent.insertBefore(wrapper, input);
  wrapper.append(button, filename, input);
  input.classList.add("lt-file-input__native");
  input.setAttribute("data-file-input-enhanced", "true");

  const refresh = () => {
    filename.textContent = selectedFileLabel(input);
  };

  input.addEventListener("change", refresh);
  if (input.form) {
    input.form.addEventListener("reset", () => window.setTimeout(refresh, 0));
  }
  refresh();
}

function formatFileSize(bytes) {
  const kb = Number(bytes || 0) / 1024;
  return `${kb < 10 ? kb.toFixed(1) : Math.round(kb)} KB`;
}

function fileKey(file) {
  return `${file.name}:${file.size}:${file.lastModified}`;
}

function assignFiles(input, files) {
  const transfer = new DataTransfer();
  files.forEach((file) => transfer.items.add(file));
  input.files = transfer.files;
}

function initializeFileStaging(form) {
  if (!form || form.dataset.fileStagingEnhanced === "true") return;

  const input = form.querySelector("[data-file-dropzone-input]");
  const dropzone = form.querySelector("[data-file-dropzone]");
  const staging = form.querySelector("[data-file-staging]");
  const list = form.querySelector("[data-file-staging-list]");
  const count = form.querySelector("[data-file-count]");
  const emptyState = form.querySelector("[data-file-empty-state]");
  const submit = form.querySelector('button[type="submit"]');

  if (!input || !dropzone || !staging || !list) return;

  let files = [];

  const render = () => {
    list.replaceChildren();
    staging.hidden = files.length === 0;
    if (emptyState) emptyState.hidden = files.length > 0;
    if (submit) submit.disabled = files.length === 0;

    if (count) {
      count.textContent = `${files.length} file${files.length === 1 ? "" : "s"}`;
    }

    files.forEach((file) => {
      const item = document.createElement("li");
      item.className = "lt-upload-staging__item";

      const info = document.createElement("div");
      info.className = "lt-upload-staging__info";

      const name = document.createElement("span");
      name.className = "lt-upload-staging__name";
      name.textContent = file.name;
      name.title = file.name;

      const size = document.createElement("span");
      size.className = "lt-upload-staging__size";
      size.textContent = formatFileSize(file.size);

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "lt-upload-staging__remove";
      remove.setAttribute("aria-label", `Remove ${file.name}`);
      remove.innerHTML = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>';

      remove.addEventListener("click", () => {
        const key = fileKey(file);
        files = files.filter((candidate) => fileKey(candidate) !== key);
        assignFiles(input, files);
        render();
      });

      info.append(name, size);
      item.append(info, remove);
      list.appendChild(item);
    });
  };

  const mergeFiles = (incoming) => {
    const byKey = new Map(files.map((file) => [fileKey(file), file]));
    Array.from(incoming || []).forEach((file) => byKey.set(fileKey(file), file));
    files = Array.from(byKey.values());
    assignFiles(input, files);
    render();
  };

  input.addEventListener("change", () => mergeFiles(input.files));

  dropzone.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    input.click();
  });

  ["dragenter", "dragover"].forEach((type) => {
    dropzone.addEventListener(type, (event) => {
      event.preventDefault();
      dropzone.classList.add("is-dragging");
    });
  });

  ["dragleave", "drop"].forEach((type) => {
    dropzone.addEventListener(type, () => dropzone.classList.remove("is-dragging"));
  });

  dropzone.addEventListener("drop", (event) => {
    event.preventDefault();
    mergeFiles(event.dataTransfer?.files);
  });

  form.addEventListener("reset", () => {
    files = [];
    window.setTimeout(render, 0);
  });

  form.dataset.fileStagingEnhanced = "true";
  render();
}

function initializeFileStagingForms(root = document) {
  if (!root) return;

  if (root.matches?.("[data-file-staging-form]")) {
    initializeFileStaging(root);
  }
  root.querySelectorAll?.("[data-file-staging-form]").forEach(initializeFileStaging);
}

function initializeFileInputs(root = document) {
  if (!root) return;

  if (root.matches?.('input[type="file"]')) {
    enhanceFileInput(root);
    return;
  }

  root.querySelectorAll?.('input[type="file"]').forEach(enhanceFileInput);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    initializeFileInputs();
    initializeFileStagingForms();
  });
} else {
  initializeFileInputs();
  initializeFileStagingForms();
}

document.addEventListener("htmx:load", (event) => {
  const root = event.detail?.elt || event.target;
  initializeFileInputs(root);
  initializeFileStagingForms(root);
});

export {
  enhanceFileInput,
  initializeFileInputs,
  initializeFileStagingForms,
  selectedFileLabel,
};
