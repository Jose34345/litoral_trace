const DIALOG_SELECTOR = "[data-lt-dialog]";
const OPEN_SELECTOR = "[data-lt-dialog-open]";
const CLOSE_SELECTOR = "[data-lt-dialog-close]";

const previousFocus = new WeakMap();

function resolveDialog(targetId) {
  if (!targetId) {
    return null;
  }

  const element = document.getElementById(targetId);
  return element instanceof HTMLDialogElement ? element : null;
}

function openDialog(dialog, trigger) {
  if (!dialog || dialog.open) {
    return;
  }

  if (trigger instanceof HTMLElement) {
    previousFocus.set(dialog, trigger);
  }

  dialog.showModal();
}

function closeDialog(dialog) {
  if (!dialog || !dialog.open) {
    return;
  }

  dialog.close();
}

function restoreDialogFocus(dialog) {
  const trigger = previousFocus.get(dialog);
  previousFocus.delete(dialog);

  if (trigger instanceof HTMLElement && document.contains(trigger)) {
    trigger.focus({ preventScroll: true });
  }
}

function installDialogController() {
  document.addEventListener("click", (event) => {
    const openTrigger = event.target.closest(OPEN_SELECTOR);

    if (openTrigger) {
      const targetId = openTrigger.getAttribute("data-lt-dialog-open");
      const dialog = resolveDialog(targetId);

      if (dialog) {
        event.preventDefault();
        openDialog(dialog, openTrigger);
      }
      return;
    }

    const closeTrigger = event.target.closest(CLOSE_SELECTOR);

    if (closeTrigger) {
      const dialog = closeTrigger.closest(DIALOG_SELECTOR);

      if (dialog instanceof HTMLDialogElement) {
        event.preventDefault();
        closeDialog(dialog);
      }
      return;
    }

    const dialog = event.target.closest(DIALOG_SELECTOR);

    if (dialog instanceof HTMLDialogElement && event.target === dialog) {
      closeDialog(dialog);
    }
  });

  document.querySelectorAll(DIALOG_SELECTOR).forEach((dialog) => {
    if (!(dialog instanceof HTMLDialogElement)) {
      return;
    }

    dialog.addEventListener("close", () => restoreDialogFocus(dialog));
  });
}

installDialogController();
