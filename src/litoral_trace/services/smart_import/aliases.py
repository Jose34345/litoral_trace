"""Canonical field specifications and broad deterministic aliases for Smart Import."""

from __future__ import annotations

from .contracts import CanonicalFieldSpec


LOTES_CANONICAL_FIELDS: tuple[CanonicalFieldSpec, ...] = (
    CanonicalFieldSpec(
        name="Identificador_Lote",
        required=True,
        semantic_type="identifier",
        aliases=frozenset(
            {
                "identificador lote",
                "id lote",
                "lote",
                "codigo lote",
                "cod lote",
                "numero lote",
                "nro lote",
                "parcela",
                "id parcela",
                "codigo parcela",
                "cod parcela",
                "numero parcela",
                "nro parcela",
                "rodal",
                "id rodal",
                "codigo rodal",
                "cod rodal",
                "numero rodal",
                "nro rodal",
                "unidad forestal",
                "unidad productiva",
                "unidad origen",
                "origen lote",
            }
        ),
    ),
    CanonicalFieldSpec(
        name="ID_Proveedor",
        required=True,
        semantic_type="supplier",
        aliases=frozenset(
            {
                "id proveedor",
                "proveedor",
                "codigo proveedor",
                "cod proveedor",
                "numero proveedor",
                "nro proveedor",
                "productor",
                "id productor",
                "codigo productor",
                "cod productor",
                "titular",
                "propietario",
                "razon social proveedor",
                "razon social productor",
                "cuit proveedor",
                "cuit productor",
                "cuit",
                "origen proveedor",
            }
        ),
        high_risk=True,
    ),
    CanonicalFieldSpec(
        name="Producto_Forestal",
        required=True,
        semantic_type="product",
        aliases=frozenset(
            {
                "producto forestal",
                "producto",
                "especie",
                "especie forestal",
                "tipo madera",
                "tipo de madera",
                "madera",
                "material",
                "materia prima",
                "producto recibido",
                "producto origen",
                "descripcion producto",
            }
        ),
    ),
    CanonicalFieldSpec(
        name="Hectareas",
        required=True,
        semantic_type="area_ha",
        aliases=frozenset(
            {
                "hectareas",
                "hectarea",
                "ha",
                "has",
                "superficie",
                "sup",
                "sup ha",
                "superficie ha",
                "superficie hectareas",
                "superficie plantada",
                "superficie productiva",
                "area",
                "area ha",
                "area hectareas",
            }
        ),
        high_risk=True,
    ),
    CanonicalFieldSpec(
        name="Latitud",
        required=True,
        semantic_type="latitude",
        aliases=frozenset(
            {
                "latitud",
                "lat",
                "latitude",
                "coord lat",
                "coordenada latitud",
                "coordenada y",
                "y gps",
                "gps lat",
            }
        ),
        high_risk=True,
    ),
    CanonicalFieldSpec(
        name="Longitud",
        required=True,
        semantic_type="longitude",
        aliases=frozenset(
            {
                "longitud",
                "lon",
                "lng",
                "long",
                "longitude",
                "coord lon",
                "coord long",
                "coordenada longitud",
                "coordenada x",
                "x gps",
                "gps lon",
            }
        ),
        high_risk=True,
    ),
    CanonicalFieldSpec(
        name="Volumen_Ingresado_Ton",
        required=True,
        semantic_type="volume_ton_in",
        aliases=frozenset(
            {
                "volumen ingresado ton",
                "volumen ingresado",
                "volumen ingreso",
                "ingreso ton",
                "ingreso tn",
                "ton ingresadas",
                "tn ingresadas",
                "toneladas ingresadas",
                "volumen recibido",
                "ton recibidas",
                "tn recibidas",
                "toneladas recibidas",
                "peso neto ton",
                "peso neto tn",
                "cantidad ingresada ton",
                "cantidad ingresada tn",
            }
        ),
        high_risk=True,
    ),
    CanonicalFieldSpec(
        name="Volumen_Exportar_Ton",
        required=True,
        semantic_type="volume_ton_out",
        aliases=frozenset(
            {
                "volumen exportar ton",
                "volumen exportar",
                "volumen exportable",
                "volumen disponible",
                "ton exportar",
                "tn exportar",
                "toneladas exportar",
                "ton exportables",
                "tn exportables",
                "toneladas exportables",
                "stock exportable",
                "disponible ton",
                "disponible tn",
                "saldo exportable",
            }
        ),
        high_risk=True,
    ),
)


CANONICAL_FIELD_BY_NAME = {
    field.name: field for field in LOTES_CANONICAL_FIELDS
}
