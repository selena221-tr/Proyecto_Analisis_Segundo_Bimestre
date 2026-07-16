
import polars as pl 
import json

def generar_reporte(datasets):

    reporte = {}

    for nombre, d in datasets.items():

        filas, columnas = d.shape

        porcentaje_nulos = (
            d.null_count()
            .transpose(include_header=True, header_name="columna", column_names=["nulos"])
            .with_columns(
                (pl.col("nulos") / filas * 100).round(2).alias("porcentaje")
            )
            .filter(pl.col("nulos") > 0)
            .sort("porcentaje", descending=True)
        )

        total = d.height
        unicos = d.unique().height
        duplicados = total - unicos

        reporte[nombre] = {
            "filas": filas,
            "columnas": columnas,
            "tipos_datos": {str(col): str(tipo) for col, tipo in d.schema.items()},
            "nulos": porcentaje_nulos.to_dicts() if porcentaje_nulos.height > 0 else "Sin valores nulos",
            "duplicados": duplicados,
            "porcentaje_duplicados": round((duplicados / total) * 100, 2)
        }

    with open("./reportes/reporte_datasets.json", "w", encoding="utf-8") as archivo:
        json.dump(reporte, archivo,indent=4,ensure_ascii=False)
    print("Reporte JSON generado correctamente")

    return reporte



