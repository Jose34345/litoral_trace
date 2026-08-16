const MAP_SELECTOR = "[data-regional-map]";
const CONTROL_SELECTOR = "[data-regional-map-control]";
const CARD_SELECTOR = "[data-regional-link]";

const DEFAULT_REGION_ID = "ARG";
const FEATURE_SEPARATOR = "|";

/*
 * Presentation-only viewport for the national map.
 *
 * The official Georef dataset is preserved intact.
 * Tierra del Fuego contains remote South Atlantic /
 * Antarctic geometries which make a full GeoJSON
 * fitBounds() unsuitable for the public continental
 * navigator.
 *
 * These bounds affect viewport only.
 * They do not alter, filter, or reinterpret the
 * canonical territorial dataset.
 */
const COUNTRY_VIEWPORT_BOUNDS = [
  [-56.2, -75.2],
  [-21.0, -52.5],
];

const BASE_STYLE = {
  color: "#64748b",
  weight: 1,
  opacity: 1,
  fillColor: "#e2e8f0",
  fillOpacity: 0.82,
};

const ACTIVE_STYLE = {
  color: "#047857",
  weight: 2,
  opacity: 1,
  fillColor: "#34d399",
  fillOpacity: 0.72,
};

const HOVER_STYLE = {
  color: "#0f766e",
  weight: 2,
  opacity: 1,
  fillColor: "#a7f3d0",
  fillOpacity: 0.86,
};


function parseFeatureNames(element) {
  const raw = (
    element.dataset.georefNames
    || ""
  ).trim();

  if (!raw) {
    return [];
  }

  return raw
    .split(FEATURE_SEPARATOR)
    .map((value) => value.trim())
    .filter(Boolean);
}


function scopeFromElement(element) {
  return {
    regionId: (
      element.dataset.regionId
      || ""
    ).trim(),
    slug: (
      element.dataset.regionSlug
      || ""
    ).trim(),
    name: (
      element.dataset.regionName
      || ""
    ).trim(),
    geographicScope: (
      element.dataset.geographicScope
      || ""
    ).trim(),
    scopeKind: (
      element.dataset.scopeKind
      || ""
    ).trim(),
    georefNames: parseFeatureNames(
      element,
    ),
    includesAllProvinces: (
      element.dataset.allProvinces
      === "true"
    ),
  };
}


function installRegionalIntelligenceMap() {
  const container = document.querySelector(
    MAP_SELECTOR,
  );

  if (!container) {
    return;
  }

  const status = document.querySelector(
    "[data-regional-map-status]",
  );

  const controlsPanel = document.querySelector(
    "[data-regional-map-controls]",
  );

  const selectedName = document.querySelector(
    "[data-regional-map-selected-name]",
  );

  const selectedScope = document.querySelector(
    "[data-regional-map-selected-scope]",
  );

  const selectedLink = document.querySelector(
    "[data-regional-map-selected-link]",
  );

  const controls = Array.from(
    document.querySelectorAll(
      CONTROL_SELECTOR,
    ),
  );

  const cards = Array.from(
    document.querySelectorAll(
      CARD_SELECTOR,
    ),
  );

  function showMapStatus(message) {
    if (!status) {
      return;
    }

    status.textContent = message;
    status.hidden = false;
  }


  function hideMapStatus() {
    if (!status) {
      return;
    }

    status.hidden = true;
  }


  showMapStatus(
    "Loading territorial reference…",
  );


  if (
    typeof window.L !== "object"
    || typeof window.L.map !== "function"
  ) {
    showMapStatus(
      "Territorial visualization is unavailable. "
      + "Regional profile content remains accessible.",
    );

    return;
  }

  const geojsonUrl = (
    container.dataset.geojsonUrl
    || ""
  ).trim();

  const featureNameProperty = (
    container.dataset.featureNameProperty
    || "nombre"
  ).trim();

  const requestedInitialRegionId = (
    container.dataset.initialRegionId
    || ""
  ).trim();

  if (!geojsonUrl) {
    showMapStatus(
      "Territorial dataset is not configured. "
      + "Regional profile content remains accessible.",
    );

    return;
  }

  const scopeElements = [
    container,
    ...controls,
    ...cards,
  ];

  const scopes = new Map();

  scopeElements.forEach((element) => {
    const scope = scopeFromElement(
      element,
    );

    if (
      scope.regionId
      && !scopes.has(scope.regionId)
    ) {
      scopes.set(
        scope.regionId,
        scope,
      );
    }
  });

  const map = window.L.map(
    container,
    {
      attributionControl: false,
      zoomControl: true,
      scrollWheelZoom: false,
      doubleClickZoom: true,
      boxZoom: true,
      keyboard: true,
    },
  );

  map.setView(
    [-38.4, -63.6],
    4,
  );

  const featureLayers = new Map();

  let geojsonLayer = null;

  let activeRegionId = (
    requestedInitialRegionId
    || DEFAULT_REGION_ID
  );

  const reducedMotion = window.matchMedia(
    "(prefers-reduced-motion: reduce)",
  ).matches;


  function getFeatureName(feature) {
    return String(
      feature?.properties?.[
        featureNameProperty
      ]
      || "",
    ).trim();
  }


  function selectedFeatureNames(scope) {
    if (
      !scope
      || scope.includesAllProvinces
    ) {
      return null;
    }

    return new Set(
      scope.georefNames,
    );
  }


  function applyScopeStyles(scope) {
    const selectedNames = (
      selectedFeatureNames(scope)
    );

    featureLayers.forEach(
      (layer, featureName) => {
        const selected = (
          selectedNames === null
          || selectedNames.has(
            featureName,
          )
        );

        layer.setStyle(
          selected
            ? ACTIVE_STYLE
            : BASE_STYLE,
        );
      },
    );
  }


  function fitScope(scope) {
    if (!geojsonLayer) {
      return;
    }

    let bounds = null;

    /*
     * National scope intentionally uses a
     * continental presentation viewport.
     *
     * All 24 jurisdictional features remain
     * loaded and styled. Only camera framing
     * is constrained here.
     */
    if (
      scope?.scopeKind === "country"
      && scope.includesAllProvinces
    ) {
      bounds = window.L.latLngBounds(
        COUNTRY_VIEWPORT_BOUNDS,
      );
    } else if (
      !scope
      || scope.includesAllProvinces
    ) {
      bounds = (
        geojsonLayer.getBounds()
      );
    } else {
      const selectedLayers = (
        scope.georefNames
          .map(
            (name) => (
              featureLayers.get(name)
            ),
          )
          .filter(Boolean)
      );

      if (selectedLayers.length > 0) {
        const group = (
          window.L.featureGroup(
            selectedLayers,
          )
        );

        bounds = group.getBounds();
      }
    }

    if (
      bounds
      && bounds.isValid()
    ) {
      map.fitBounds(
        bounds,
        {
          padding: [24, 24],
          animate: !reducedMotion,
          maxZoom: (
            scope?.scopeKind === "province"
              ? 7
              : (
                scope?.scopeKind === "country"
                  ? 5
                  : 6
              )
          ),
        },
      );
    }
  }


  function updateControls(scope) {
    controls.forEach((control) => {
      const active = (
        control.dataset.regionId
        === scope.regionId
      );

      control.setAttribute(
        "aria-pressed",
        String(active),
      );

      control.classList.toggle(
        "border-emerald-500",
        active,
      );

      control.classList.toggle(
        "bg-emerald-50",
        active,
      );

      control.classList.toggle(
        "text-emerald-900",
        active,
      );

      control.classList.toggle(
        "ring-2",
        active,
      );

      control.classList.toggle(
        "ring-emerald-500/20",
        active,
      );

      control.classList.toggle(
        "shadow-sm",
        active,
      );
    });
  }


  function updateCards(scope) {
    cards.forEach((card) => {
      const active = (
        card.dataset.regionId
        === scope.regionId
      );

      card.dataset.mapActive = (
        active
          ? "true"
          : "false"
      );

      card.classList.toggle(
        "ring-2",
        active,
      );

      card.classList.toggle(
        "ring-emerald-500",
        active,
      );

      card.classList.toggle(
        "bg-emerald-50",
        active,
      );
    });
  }


  function updateSelectedSummary(scope) {
    if (selectedName) {
      selectedName.textContent = (
        scope.name
        || scope.regionId
      );
    }

    if (selectedScope) {
      selectedScope.textContent = (
        scope.geographicScope
        || scope.scopeKind
      );
    }

    if (selectedLink) {
      selectedLink.href = (
        `/regional-intelligence/${scope.slug}`
      );

      selectedLink.hidden = (
        !scope.slug
      );
    }
  }


  function setActiveScope(
    regionId,
    {
      fit = true,
    } = {},
  ) {
    const scope = scopes.get(
      regionId,
    );

    if (!scope) {
      return;
    }

    activeRegionId = regionId;

    applyScopeStyles(scope);
    updateControls(scope);
    updateCards(scope);
    updateSelectedSummary(scope);

    if (fit) {
      fitScope(scope);
    }
  }


  function findBestScopeForFeature(
    featureName,
  ) {
    const provinceScope = Array.from(
      scopes.values(),
    ).find(
      (scope) => (
        scope.scopeKind === "province"
        && scope.georefNames.includes(
          featureName,
        )
      ),
    );

    if (provinceScope) {
      return provinceScope;
    }

    const aggregateScope = Array.from(
      scopes.values(),
    ).find(
      (scope) => (
        scope.scopeKind === "aggregate"
        && scope.georefNames.includes(
          featureName,
        )
      ),
    );

    if (aggregateScope) {
      return aggregateScope;
    }

    return Array.from(
      scopes.values(),
    ).find(
      (scope) => (
        scope.scopeKind === "country"
        && scope.includesAllProvinces
      ),
    ) || null;
  }


  function onEachFeature(
    feature,
    layer,
  ) {
    const featureName = getFeatureName(
      feature,
    );

    if (!featureName) {
      return;
    }

    featureLayers.set(
      featureName,
      layer,
    );

    const label = (
      document.createElement(
        "span",
      )
    );

    label.textContent = featureName;

    layer.bindTooltip(
      label,
      {
        direction: "top",
        sticky: true,
      },
    );

    layer.on(
      "mouseover",
      () => {
        const scope = (
          findBestScopeForFeature(
            featureName,
          )
        );

        if (scope) {
          layer.setStyle(
            HOVER_STYLE,
          );
        }
      },
    );

    layer.on(
      "mouseout",
      () => {
        const activeScope = (
          scopes.get(
            activeRegionId,
          )
        );

        applyScopeStyles(
          activeScope,
        );
      },
    );

    layer.on(
      "click",
      () => {
        const scope = (
          findBestScopeForFeature(
            featureName,
          )
        );

        if (
          scope
          && scopes.size > 1
        ) {
          setActiveScope(
            scope.regionId,
          );
        }
      },
    );
  }


  function wireScopeElements() {
    controls.forEach((control) => {
      control.addEventListener(
        "click",
        () => {
          setActiveScope(
            control.dataset.regionId,
          );
        },
      );
    });

    cards.forEach((card) => {
      const activate = () => {
        const regionId = (
          card.dataset.regionId
          || ""
        );

        if (regionId) {
          setActiveScope(
            regionId,
          );
        }
      };

      card.addEventListener(
        "mouseenter",
        activate,
      );

      card.addEventListener(
        "focus",
        activate,
      );
    });
  }


  async function loadMap() {
    try {
      const response = (
        await window.fetch(
          geojsonUrl,
          {
            method: "GET",
            credentials: "same-origin",
            headers: {
              Accept: (
                "application/geo+json, "
                + "application/json"
              ),
            },
          },
        )
      );

      if (!response.ok) {
        throw new Error(
          `GeoJSON request failed with ${response.status}.`,
        );
      }

      const geojson = (
        await response.json()
      );

      if (
        geojson?.type
          !== "FeatureCollection"
        || !Array.isArray(
          geojson.features,
        )
      ) {
        throw new Error(
          "Territorial dataset is not "
          + "a GeoJSON FeatureCollection.",
        );
      }

      geojsonLayer = (
        window.L.geoJSON(
          geojson,
          {
            style: BASE_STYLE,
            onEachFeature,
          },
        )
      ).addTo(map);

      wireScopeElements();

      if (controlsPanel) {
        controlsPanel.hidden = false;
      }

      hideMapStatus();

      const initialRegionId = (
        scopes.has(
          requestedInitialRegionId
        )
          ? requestedInitialRegionId
          : (
            scopes.has(
              DEFAULT_REGION_ID
            )
              ? DEFAULT_REGION_ID
              : (
                controls[0]
                  ?.dataset
                  .regionId
                || scopes.keys()
                  .next()
                  .value
              )
          )
      );

      if (initialRegionId) {
        setActiveScope(
          initialRegionId,
        );
      }
    } catch (error) {
      console.error(
        "Regional Intelligence map "
        + "initialization failed.",
        error,
      );

      showMapStatus(
        "Territorial visualization "
        + "could not be loaded. "
        + "Regional profile content "
        + "remains accessible.",
      );

      map.remove();
    }
  }


  loadMap();
}


function boot() {
  installRegionalIntelligenceMap();
}


if (
  document.readyState
  === "loading"
) {
  document.addEventListener(
    "DOMContentLoaded",
    boot,
    {
      once: true,
    },
  );
} else {
  boot();
}