from pathlib import Path

from src.eda_profundo.ventas_generales import (
    distribucion_ventas_familias,
    ventas_tienda_ranking,
    ventas_ciudad_provincia,
    evolucion_temporal,
)
from src.eda_profundo.estacionalidad_feriados import analisis_feriados
from src.eda_profundo.petroleo_economia import (
    correlacion_precio_ventas,
    lag_caida_petroleo_ventas,
    sensibilidad_ciudades_caida,
)
from src.eda_profundo.promociones import (
    comparacion_ventas,
    promociones_ventas_familias,
)
from src.eda_profundo.transacciones import (
    relacion_transacciones_ventas,
    tiendas_promedio,
)


def ejecutar_analisis_exploratorio(ventas_completo, datasets):
    """Ejecuta los análisis EDA propuestos sobre el dataset limpio."""
    resultados = {
        "distribucion_ventas_familias": distribucion_ventas_familias(ventas_completo),
        "ventas_tienda_ranking": ventas_tienda_ranking(ventas_completo),
        "ventas_ciudad_provincia": ventas_ciudad_provincia(ventas_completo),
        "evolucion_temporal": evolucion_temporal(ventas_completo),
        "analisis_feriados": analisis_feriados(ventas_completo, datasets),
        "correlacion_precio_ventas": correlacion_precio_ventas(ventas_completo),
        "lag_caida_petroleo_ventas": lag_caida_petroleo_ventas(
            correlacion_precio_ventas(ventas_completo)[0]
        ),
        "sensibilidad_ciudades_caida": sensibilidad_ciudades_caida(ventas_completo),
        "comparacion_ventas": comparacion_ventas(ventas_completo),
        "promociones_ventas_familias": promociones_ventas_familias(
            comparacion_ventas(ventas_completo)
        ),
        "relacion_transacciones_ventas": relacion_transacciones_ventas(ventas_completo),
        "tiendas_promedio": tiendas_promedio(
            relacion_transacciones_ventas(ventas_completo)[0]
        ),
    }
    return resultados
