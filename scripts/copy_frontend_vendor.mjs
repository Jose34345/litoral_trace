import {
  copyFile,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";

import {
  dirname,
  resolve,
} from "node:path";

import {
  fileURLToPath,
} from "node:url";


const scriptDirectory = dirname(
  fileURLToPath(import.meta.url),
);

const repositoryRoot = resolve(
  scriptDirectory,
  "..",
);

const nodeModules = resolve(
  repositoryRoot,
  "node_modules",
);

const vendorRoot = resolve(
  repositoryRoot,
  "src",
  "litoral_trace",
  "static",
  "vendor",
);


async function copyTextFile(
  source,
  destination,
) {
  const content = await readFile(
    source,
    "utf8",
  );

  await writeFile(
    destination,
    content.replace(/\r\n?/g, "\n"),
    "utf8",
  );
}


async function copyDirectory(
  source,
  destination,
) {
  await mkdir(
    destination,
    {
      recursive: true,
    },
  );

  const entries = await readdir(
    source,
    {
      withFileTypes: true,
    },
  );

  for (const entry of entries) {
    const sourcePath = resolve(
      source,
      entry.name,
    );

    const destinationPath = resolve(
      destination,
      entry.name,
    );

    if (entry.isDirectory()) {
      await copyDirectory(
        sourcePath,
        destinationPath,
      );

      continue;
    }

    if (entry.isFile()) {
      await copyFile(
        sourcePath,
        destinationPath,
      );
    }
  }
}


async function copyHtmx() {
  const destinationDirectory = resolve(
    vendorRoot,
    "htmx",
  );

  await mkdir(
    destinationDirectory,
    {
      recursive: true,
    },
  );

  await copyTextFile(
    resolve(
      nodeModules,
      "htmx.org",
      "dist",
      "htmx.min.js",
    ),
    resolve(
      destinationDirectory,
      "htmx.min.js",
    ),
  );
}


async function copyLeaflet() {
  const sourceDirectory = resolve(
    nodeModules,
    "leaflet",
    "dist",
  );

  const destinationDirectory = resolve(
    vendorRoot,
    "leaflet",
  );

  await mkdir(
    destinationDirectory,
    {
      recursive: true,
    },
  );

  await copyTextFile(
    resolve(
      sourceDirectory,
      "leaflet.css",
    ),
    resolve(
      destinationDirectory,
      "leaflet.css",
    ),
  );

  await copyTextFile(
    resolve(
      sourceDirectory,
      "leaflet.js",
    ),
    resolve(
      destinationDirectory,
      "leaflet.js",
    ),
  );

  await copyDirectory(
    resolve(
      sourceDirectory,
      "images",
    ),
    resolve(
      destinationDirectory,
      "images",
    ),
  );
}


async function copyFontAwesome() {
  const packageRoot = resolve(
    nodeModules,
    "@fortawesome",
    "fontawesome-free",
  );

  const destinationDirectory = resolve(
    vendorRoot,
    "fontawesome",
  );

  const cssDirectory = resolve(
    destinationDirectory,
    "css",
  );

  await mkdir(
    cssDirectory,
    {
      recursive: true,
    },
  );

  await copyTextFile(
    resolve(
      packageRoot,
      "css",
      "all.min.css",
    ),
    resolve(
      cssDirectory,
      "all.min.css",
    ),
  );

  await copyDirectory(
    resolve(
      packageRoot,
      "webfonts",
    ),
    resolve(
      destinationDirectory,
      "webfonts",
    ),
  );
}


await rm(
  vendorRoot,
  {
    recursive: true,
    force: true,
  },
);

await mkdir(
  vendorRoot,
  {
    recursive: true,
  },
);

await Promise.all([
  copyHtmx(),
  copyLeaflet(),
  copyFontAwesome(),
]);

console.log(
  "Vendored frontend runtime assets:",
);

console.log(
  "- HTMX 2.0.10",
);

console.log(
  "- Leaflet 1.9.4",
);

console.log(
  "- Font Awesome 6.5.1",
);
