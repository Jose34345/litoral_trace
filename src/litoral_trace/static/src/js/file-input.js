const EMPTY_FILE_LABEL = "Ningún archivo seleccionado";

function selectedFileLabel(input) {
    const files = Array.from(input.files || []);
    if (!files.length) return EMPTY_FILE_LABEL;
    if (files.length === 1) return files[0].name;
    return `${files.length} archivos seleccionados`;
}

function enhanceFileInput(input) {
    if (!input || input.dataset.fileInputEnhanced === "true") return;
    input.dataset.fileInputEnhanced = "true";

    const wrapper = document.createElement("div");
    wrapper.className = "lt-file-input";

    const button = document.createElement("span");
    button.className = "lt-file-input__button";
    button.setAttribute("aria-hidden", "true");
    button.textContent = input.multiple ? "Seleccionar archivos" : "Seleccionar archivo";

    const filename = document.createElement("span");
    filename.className = "lt-file-input__name";
    filename.setAttribute("aria-live", "polite");
    filename.textContent = selectedFileLabel(input);

    const parent = input.parentNode;
    if (!parent) return;
    parent.insertBefore(wrapper, input);
    wrapper.append(button, filename, input);
    input.classList.add("lt-file-input__native");

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

export { enhanceFileInput, initializeFileInputs, selectedFileLabel };