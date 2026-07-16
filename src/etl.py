from pathlib import Path
from typing import Dict, Any

import polars as pl

from src.carga import cargar_datasets
from src.limpieza import (
    estandarizacion_fechas,
    eliminar_duplicados,
    imputacion_nulos,
    correccion_tipo_datos,
)
from src.transformacion import unificar_datasets
from src.validacion import validar_dataset, limpieza_final
from src.evaluacion_inicial import generar_reporte
from src.exportacion import exportar_postgres


def ejecutar_pipeline(base_dir: str | Path | None = None, exportar_a_postgres: bool = False) -> Dict[str, Any]:
    """Ejecuta el flujo completo de carga, limpieza, transformación, validación y exportación."""
    if base_dir is None:
        base_dir = Path(__file__).resolve().parents[1]
    else:
        base_dir = Path(base_dir)

    data_dir = base_dir / "data"
    reportes_dir = base_dir / "reportes"
    reportes_dir.mkdir(exist_ok=True)

    # Cambiar el directorio de trabajo para que los módulos encuentren los CSV en la ruta esperada.
    previous_cwd = Path.cwd()
    try:
        import os
        os.chdir(base_dir)

        datasets = cargar_datasets()
        datasets = estandarizacion_fechas(datasets)
        datasets = eliminar_duplicados(datasets)
        datasets = imputacion_nulos(datasets)
        datasets = correccion_tipo_datos(datasets)

        ventas = unificar_datasets(datasets)
        duplicados = validar_dataset(ventas)
        ventas_limpia = limpieza_final(ventas, duplicados)

        reporte = generar_reporte(datasets)

        if exportar_a_postgres:
            exportar_postgres({"ventas_limpia": ventas_limpia})

        return {
            "datasets": datasets,
            "ventas_limpia": ventas_limpia,
            "reporte": reporte,
        }
    finally:
        os.chdir(previous_cwd)
