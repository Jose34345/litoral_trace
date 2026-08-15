import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, "..");

const source = resolve(
  repositoryRoot,
  "node_modules",
  "htmx.org",
  "dist",
  "htmx.min.js",
);

const destinationDirectory = resolve(
  repositoryRoot,
  "src",
  "litoral_trace",
  "static",
  "vendor",
  "htmx",
);

const destination = resolve(destinationDirectory, "htmx.min.js");

await mkdir(destinationDirectory, { recursive: true });
await copyFile(source, destination);

console.log("Copied vendored HTMX asset.");
