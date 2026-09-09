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
  if (input.getAttribute("data-file-input-enhanced") === "true") return;

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

function initializeFileInputs(root = document) {
  if (!root) return;

  if (root.matches?.('input[type="file"]')) {
    enhanceFileInput(root);
    return;
  }

  root.querySelectorAll?.('input[type="file"]').forEach(enhanceFileInput);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => initializeFileInputs());
} else {
  initializeFileInputs();
}

document.addEventListener("htmx:load", (event) => {
  initializeFileInputs(event.detail?.elt || event.target);
});

export { enhanceFileInput, initializeFileInputs, selectedFileLabel };
