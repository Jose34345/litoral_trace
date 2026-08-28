"""Fail-closed preparation contract for the first Assurance customer pilot.

The first pilot must consume the files the customer already has. This module
therefore configures only the narrow facts needed to evaluate one customer /
market / product combination and explicitly keeps ERP integration and process
redesign outside the entry contract.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path, PurePath
from typing import Iterable

from litoral_trace.assurance.domain import AssuranceDocumentType
from litoral_trace.assurance.ingestion import SUPPORTED_EXTENSIONS
from litoral_trace.assurance.preflight import PreflightInput, PreflightSignalState


PILOT_HISTORICAL_BATCH_SIZE = 20
PILOT_REQUIRES_ERP_INTEGRATION = False
PILOT_REQUIRES_PROCESS_CHANGE = False
PILOT_ACCEPTED_EXTENSIONS = tuple(sorted(SUPPORTED_EXTENSIONS))
_CONFIG_MAX_BYTES = 128 * 1024
_ALLOWED_REQUIRED_DOCUMENT_TYPES = frozenset(
    item.value for item in AssuranceDocumentType if item != AssuranceDocumentType.UNKNOWN
)


class AssurancePilotPreparationError(RuntimeError):
    """Base error with no customer data in the message."""


class AssurancePilotConfigurationError(AssurancePilotPreparationError):
    pass


class AssurancePilotAccessError(AssurancePilotPreparationError):
    pass


class AssurancePilotRuleNotFoundError(AssurancePilotPreparationError):
    pass


def _read_bool(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise AssurancePilotConfigurationError(f"{name} debe ser booleano.")


def _normalized_text(value: object) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _normalized_market(value: object) -> str:
    return str(value or "").strip().upper()


def _strict_bool(value: object, *, field_name: str) -> bool:
    if type(value) is not bool:
        raise AssurancePilotConfigurationError(
            f"{field_name} debe ser booleano en la configuración del piloto."
        )
    return bool(value)


def _required_document_types(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise AssurancePilotConfigurationError(
            "required_document_types debe ser una lista."
        )
    normalized: list[str] = []
    for value in raw:
        document_type = str(value or "").strip().upper()
        if document_type not in _ALLOWED_REQUIRED_DOCUMENT_TYPES:
            raise AssurancePilotConfigurationError(
                "La configuración contiene un document_type no soportado."
            )
        if document_type not in normalized:
            normalized.append(document_type)
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class AssurancePilotRule:
    customer_reference: str
    market: str
    product: str
    required_document_types: tuple[str, ...]
    phytosanitary_required: bool
    eudr_required: bool

    @property
    def match_key(self) -> tuple[str, str, str]:
        return (
            _normalized_text(self.customer_reference),
            _normalized_market(self.market),
            _normalized_text(self.product),
        )


@dataclass(frozen=True, slots=True)
class AssurancePilotConfiguration:
    enabled: bool
    environment: str
    organization_id: int | None = None
    rules: tuple[AssurancePilotRule, ...] = ()
    source_path: str | None = None

    @classmethod
    def from_environment(cls) -> "AssurancePilotConfiguration":
        environment = os.environ.get("ENVIRONMENT", "development").strip().lower()
        enabled = _read_bool("LT_ASSURANCE_PILOT_MODE", default=False)
        if not enabled:
            return cls(enabled=False, environment=environment)

        if environment != "staging":
            raise AssurancePilotConfigurationError(
                "LT_ASSURANCE_PILOT_MODE sólo puede habilitarse con ENVIRONMENT=staging."
            )

        raw_organization_id = os.environ.get(
            "LT_ASSURANCE_PILOT_ORGANIZATION_ID", ""
        ).strip()
        try:
            organization_id = int(raw_organization_id)
        except ValueError as exc:
            raise AssurancePilotConfigurationError(
                "LT_ASSURANCE_PILOT_ORGANIZATION_ID debe ser un entero positivo."
            ) from exc
        if organization_id <= 0:
            raise AssurancePilotConfigurationError(
                "LT_ASSURANCE_PILOT_ORGANIZATION_ID debe ser un entero positivo."
            )

        raw_path = os.environ.get("LT_ASSURANCE_PILOT_CONFIG_PATH", "").strip()
        if not raw_path:
            raise AssurancePilotConfigurationError(
                "LT_ASSURANCE_PILOT_CONFIG_PATH es obligatorio en modo piloto."
            )
        path = Path(raw_path)
        try:
            payload_bytes = path.read_bytes()
        except OSError as exc:
            raise AssurancePilotConfigurationError(
                "No se pudo leer la configuración del piloto."
            ) from exc
        if not payload_bytes or len(payload_bytes) > _CONFIG_MAX_BYTES:
            raise AssurancePilotConfigurationError(
                "La configuración del piloto está vacía o excede el tamaño permitido."
            )
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AssurancePilotConfigurationError(
                "La configuración del piloto debe ser JSON UTF-8 válido."
            ) from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise AssurancePilotConfigurationError(
                "La configuración del piloto requiere schema_version=1."
            )

        try:
            file_organization_id = int(payload.get("organization_id"))
        except (TypeError, ValueError) as exc:
            raise AssurancePilotConfigurationError(
                "organization_id inválido en la configuración del piloto."
            ) from exc
        if file_organization_id != organization_id:
            raise AssurancePilotConfigurationError(
                "El tenant del archivo de configuración no coincide con el tenant del staging."
            )

        raw_rules = payload.get("rules")
        if not isinstance(raw_rules, list) or not raw_rules:
            raise AssurancePilotConfigurationError(
                "La configuración del piloto debe contener al menos una regla."
            )

        rules: list[AssurancePilotRule] = []
        keys: set[tuple[str, str, str]] = set()
        for raw_rule in raw_rules:
            if not isinstance(raw_rule, dict):
                raise AssurancePilotConfigurationError(
                    "Cada regla del piloto debe ser un objeto JSON."
                )
            customer_reference = str(
                raw_rule.get("customer_reference") or ""
            ).strip()
            market = _normalized_market(raw_rule.get("market"))
            product = str(raw_rule.get("product") or "").strip()
            if not customer_reference or not market or not product:
                raise AssurancePilotConfigurationError(
                    "Cada regla requiere customer_reference, market y product."
                )
            rule = AssurancePilotRule(
                customer_reference=customer_reference,
                market=market,
                product=product,
                required_document_types=_required_document_types(
                    raw_rule.get("required_document_types")
                ),
                phytosanitary_required=_strict_bool(
                    raw_rule.get("phytosanitary_required"),
                    field_name="phytosanitary_required",
                ),
                eudr_required=_strict_bool(
                    raw_rule.get("eudr_required"),
                    field_name="eudr_required",
                ),
            )
            if rule.match_key in keys:
                raise AssurancePilotConfigurationError(
                    "La configuración contiene reglas duplicadas para la misma combinación."
                )
            keys.add(rule.match_key)
            rules.append(rule)

        return cls(
            enabled=True,
            environment=environment,
            organization_id=organization_id,
            rules=tuple(rules),
            source_path=str(path),
        )

    def require_organization(self, organization_id: int) -> None:
        if not self.enabled:
            return
        if self.organization_id != int(organization_id):
            raise AssurancePilotAccessError(
                "El tenant autenticado no pertenece al staging de piloto configurado."
            )

    def find_rule(
        self,
        *,
        organization_id: int,
        customer_reference: object,
        market: object,
        product: object,
    ) -> AssurancePilotRule | None:
        if not self.enabled:
            return None
        self.require_organization(organization_id)
        key = (
            _normalized_text(customer_reference),
            _normalized_market(market),
            _normalized_text(product),
        )
        for rule in self.rules:
            if rule.match_key == key:
                return rule
        return None


def get_assurance_pilot_configuration() -> AssurancePilotConfiguration:
    return AssurancePilotConfiguration.from_environment()


def _merged_required_documents(
    configured: Iterable[str],
    requested: Iterable[str],
) -> tuple[str, ...]:
    merged: list[str] = []
    for value in (*tuple(configured), *tuple(requested)):
        normalized = str(value or "").strip().upper()
        if normalized and normalized not in merged:
            merged.append(normalized)
    return tuple(merged)


def apply_pilot_policy_to_preflight(
    payload: PreflightInput,
    *,
    organization_id: int,
) -> PreflightInput:
    """Inject deployment-owned pilot rules without allowing request downgrades."""
    configuration = get_assurance_pilot_configuration()
    if not configuration.enabled:
        return payload
    configuration.require_organization(organization_id)

    customer_reference = str(payload.customer_reference or "").strip()
    market = str(payload.market or "").strip()
    product = str(payload.product or "").strip()
    if not customer_reference or not market or not product:
        return payload

    rule = configuration.find_rule(
        organization_id=organization_id,
        customer_reference=customer_reference,
        market=market,
        product=product,
    )
    if rule is None:
        raise AssurancePilotRuleNotFoundError(
            "No existe una regla piloto para la combinación cliente/mercado/producto."
        )

    phytosanitary_state = payload.phytosanitary_state
    if rule.phytosanitary_required:
        if phytosanitary_state == PreflightSignalState.NOT_APPLICABLE:
            phytosanitary_state = PreflightSignalState.UNASSESSED
    elif phytosanitary_state == PreflightSignalState.UNASSESSED:
        phytosanitary_state = PreflightSignalState.NOT_APPLICABLE

    eudr_state = payload.eudr_state
    if rule.eudr_required:
        if eudr_state == PreflightSignalState.NOT_APPLICABLE:
            eudr_state = PreflightSignalState.UNASSESSED
    elif eudr_state == PreflightSignalState.UNASSESSED:
        eudr_state = PreflightSignalState.NOT_APPLICABLE

    return replace(
        payload,
        required_document_types=_merged_required_documents(
            rule.required_document_types,
            payload.required_document_types,
        ),
        phytosanitary_state=phytosanitary_state,
        eudr_state=eudr_state,
    )


@dataclass(frozen=True, slots=True)
class HistoricalReplayPlan:
    files: tuple[str, ...]
    batches: tuple[tuple[str, ...], ...]
    extensions: tuple[str, ...]
    requires_erp_integration: bool = PILOT_REQUIRES_ERP_INTEGRATION
    requires_process_change: bool = PILOT_REQUIRES_PROCESS_CHANGE


def build_historical_replay_plan(filenames: Iterable[str]) -> HistoricalReplayPlan:
    """Plan file-only replay through the existing universal ingestion surface."""
    files = tuple(str(filename or "").strip() for filename in filenames)
    if not files or any(not filename for filename in files):
        raise AssurancePilotPreparationError(
            "El replay histórico requiere al menos un archivo válido."
        )
    extensions = tuple(PurePath(filename).suffix.lower() for filename in files)
    if any(extension not in SUPPORTED_EXTENSIONS for extension in extensions):
        raise AssurancePilotPreparationError(
            "El replay histórico sólo acepta PDF, XLSX, XLS o CSV."
        )
    batches = tuple(
        files[index : index + PILOT_HISTORICAL_BATCH_SIZE]
        for index in range(0, len(files), PILOT_HISTORICAL_BATCH_SIZE)
    )
    return HistoricalReplayPlan(
        files=files,
        batches=batches,
        extensions=tuple(sorted(set(extensions))),
    )
