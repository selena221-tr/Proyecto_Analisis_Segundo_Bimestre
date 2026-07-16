import polars as pl
from datetime import timedelta

#   PREGUNTA 1
# 	Impacto de feriados nacionales en el volumen de ventas: 
#   comparación días feriados vs días normales.

def analisis_feriados(ventas_completo, datasets):

    fechas_feriados = (
        datasets["ventas_feriado"]
        .filter(
            (pl.col("type") == "Holiday") &
            (pl.col("locale") == "National")
        )
        .select("date")
        .unique()
    )

    ventas_feriado_flag = (
        ventas_completo
        .join(
            fechas_feriados.with_columns(
                pl.lit(True).alias("es_feriado")
            ),
            on="date",
            how="left"
        )
        .with_columns(
            pl.col("es_feriado").fill_null(False)
        )
    )

    comparacion_feriados = (
        ventas_feriado_flag
        .group_by("es_feriado")
        .agg(
            pl.col("sales").sum().round(2).alias("ventas_totales"),
            pl.col("sales").mean().round(2).alias("venta_promedio"),
            pl.len().alias("numero_registros")
        )
    )

    # PREGUNTA 2
    # 	Ventas en los tres días 
    #   previos y posteriores a feriados nacionales por familia de producto.

    fechas_feriados_nacionales = (
        datasets["ventas_feriado"]
        .filter(
            (pl.col("type") == "Holiday") &
            (pl.col("locale") == "National")
        )
        .select("date")
        .unique()
        .to_series()
        .to_list()
    )

    periodos = []

    for fecha in fechas_feriados_nacionales:
        for offset in range(-3, 4):
            periodos.append({
                "date": fecha + timedelta(days=offset),
                "dias_relativo": offset,
                "feriado_referencia": fecha
            })

    df_periodos = pl.DataFrame(periodos)

    ventas_periodos_feriado = (
        ventas_completo
        .join(df_periodos, on="date", how="inner")
        .group_by(["family", "dias_relativo"])
        .agg(
            pl.col("sales").mean().round(2).alias("venta_promedio")
        )
        .sort(["family", "dias_relativo"])
    )


    # PREGUNTA 3
    # ¿Qué familias de productos son más sensibles a los feriados?

    comparacion_familias = (
        ventas_feriado_flag
        .group_by(["family", "es_feriado"])
        .agg(
            pl.col("sales").mean().round(2).alias("venta_promedio")
        )
    )

    sensibilidad_feriados = (
        comparacion_familias
        .pivot(
            values="venta_promedio",
            index="family",
            on="es_feriado"
        )
        .rename({
            "true": "venta_feriado",
            "false": "venta_normal"
        })
        .with_columns(
            (
                ((pl.col("venta_feriado") - pl.col("venta_normal"))
                 / pl.col("venta_normal")) * 100
            ).round(2).alias("variacion_porcentaje")
        )
        .sort("variacion_porcentaje", descending=True)
    )

    return {
        "comparacion_feriados": comparacion_feriados,
        "ventas_periodos_feriado": ventas_periodos_feriado,
        "sensibilidad_feriados": sensibilidad_feriados,
    }