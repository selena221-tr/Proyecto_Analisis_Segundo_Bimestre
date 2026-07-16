import polars as pl

#	PREGUNTA 1: Correlación entre precio del petróleo y ventas totales mensuales.
def correlacion_precio_ventas(ventas_completo): 
    ventas_petroleo_mensual = (
        ventas_completo
        .with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes"),
        ])
        .group_by(["anio", "mes"])
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio")
        ])
        .sort(["anio", "mes"])
    )

    correlacion = ventas_petroleo_mensual.select(
         pl.corr("ventas_totales", "precio_petroleo_promedio").alias("correlacion")
    )

    return ventas_petroleo_mensual, correlacion



#	PREGUNTA 2: Identificación del lag temporal entre caída del petróleo y caída en ventas 
# (período 2015-2016).
def lag_caida_petroleo_ventas(ventas_petroleo_mensual):

    periodo_analisis = (
        ventas_petroleo_mensual
        .filter(
            (pl.col("anio") >= 2015) &
            (pl.col("anio") <= 2016)
        )
        .sort(["anio", "mes"])
        .with_columns([
            pl.col("precio_petroleo_promedio")
            .pct_change()
            .alias("var_petroleo"),

            pl.col("ventas_totales")
            .pct_change()
            .alias("var_ventas")
        ])
    )

    resultados = []

    for lag in range(1, 4):

        lag_df = periodo_analisis.with_columns(
            pl.col("var_petroleo")
            .shift(lag)
            .alias(f"var_petroleo_lag{lag}")
        )

        corr_lag = (
            lag_df
            .select(
                pl.corr(
                    f"var_petroleo_lag{lag}",
                    "var_ventas"
                )
                .alias("correlacion")
            )
        )

        resultados.append({
            "lag": lag,
            "correlacion": corr_lag[0,0]
        })

    return pl.DataFrame(resultados)

#	PREGUNTA 3: ¿Qué ciudades mostraron mayor sensibilidad a la caída del petróleo?
def sensibilidad_ciudades_caida(ventas_completo): 
    ventas_ciudad_petroleo = (
        ventas_completo
        .filter((pl.col("date").dt.year() >= 2015) & (pl.col("date").dt.year() <= 2016))
        .with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes"),
        ])
        .group_by(["city", "anio", "mes"])
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio")
        ])
        .sort(["city", "anio", "mes"])
    )

    correlacion_por_ciudad = (
         ventas_ciudad_petroleo
        .group_by("city")
        .agg(pl.corr("ventas_totales", "precio_petroleo_promedio").alias("correlacion"))
        .sort("correlacion")
    )
    return correlacion_por_ciudad