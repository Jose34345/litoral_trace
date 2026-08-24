const EMPTY_FILE_LABEL = "Ningún archivo seleccionado";
const MARGIN_PROPERTIES = ["marginTop", "marginRight", "marginBottom", "marginLeft"];

function selectedFileLabel(input) {
    const files = Array.from(input.files || []);
    if (!files.length) return EMPTY_FILE_LABEL;
    if (files.length === 1) return files[0].name;
    return `${files.length} archivos seleccionados`;
}

function transferExternalSpacing(input, wrapper) {
    const computed = window.getComputedStyle(input);
    MARGIN_PROPERTIES.forEach((property) => {
        wrapper.style[property] = computed[property];
    });

    // The native input becomes a transparent absolute overlay. Keeping its
    // previous utility margin would move the real click target away from the
    // visible picker, so external spacing belongs exclusively to the wrapper.
    input.style.margin = "0";
}

function enhanceFileInput(input) {
    if (!input || input.dataset.fileInputEnhanced === "true") return;

    const parent = input.parentNode;
    if (!parent) return;

    const wrapper = document.createElement("div");
    wrapper.className = "lt-file-input";
    transferExternalSpacing(input, wrapper);

    const button = document.createElement("span");
    button.className = "lt-file-input__button";
    button.setAttribute("aria-hidden", "true");
    button.textContent = input.multiple ? "Seleccionar archivos" : "Seleccionar archivo";

    const filename = document.createElement("span");
    filename.className = "lt-file-input__name";
    filename.setAttribute("aria-live", "polite");
    filename.textContent = selectedFileLabel(input);

    parent.insertBefore(wrapper, input);
    wrapper.append(button, filename, input);
    input.classList.add("lt-file-input__native");
    input.dataset.fileInputEnhanced = "true";

    const refreshLabel = () => {
        filename.textContent = selectedFileLabel(input);
    };
    input.addEventListener("change", refreshLabel);
    input.form?.addEventListener("reset", () => window.setTimeout(refreshLabel, 0));
}

function initializeFileInputs(root = document) {
    if (root?.matches?.('input[type="file"]')) enhanceFileInput(root);
    root?.querySelectorAll?.('input[type="file"]:not([data-file-input-enhanced="true"])')
        ?.forEach(enhanceFileInput);
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => initializeFileInputs(document), { once: true });
} else {
    initializeFileInputs(document);
}

document.body?.addEventListener("htmx:load", (event) => {
    initializeFileInputs(event.target || document);
});

export { enhanceFileInput, initializeFileInputs, selectedFileLabel, transferExternalSpacing };