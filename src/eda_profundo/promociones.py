import polars as pl

#  PREGUNTA 1: Comparación de ventas con y sin promoción por familia de producto.
def comparacion_ventas(ventas_completo): 
    ventas_promocion = (
        ventas_completo
        .with_columns(
            pl.when(pl.col("onpromotion") > 0)
            .then(pl.lit("Con promoción"))
            .otherwise(pl.lit("Sin promoción"))
            .alias("estado_promo")
        )
        .group_by(["family", "estado_promo"])
        .agg(pl.col("sales").mean().round(2).alias("venta_promedio"))
        .sort(["family", "estado_promo"])
    )
    return ventas_promocion


#  PREGUNTA 2: ¿Las promociones incrementan las ventas?
#  ¿En qué familias tienen mayor efecto?
def promociones_ventas_familias(ventas_promocion):
    venta_sin_promo_por_familia = (
        ventas_promocion
        .filter(pl.col("estado_promo") == "Sin promoción")
        .select(["family", "venta_promedio"])
        .rename({"venta_promedio": "venta_sin_promo"})
    )

    venta_con_promo_por_familia = (
        ventas_promocion
        .filter(pl.col("estado_promo") == "Con promoción")
        .select(["family", "venta_promedio"])
        .rename({"venta_promedio": "venta_con_promo"})
    )

    efecto_promocion = (
        venta_sin_promo_por_familia
        .join(venta_con_promo_por_familia, on="family", how="inner")
        .with_columns(
            ((pl.col("venta_con_promo") - pl.col("venta_sin_promo")) / pl.col("venta_sin_promo") * 100)
            .round(2).alias("variacion_pct")
        )
        .sort("variacion_pct", descending=True)
    )


    incremento_promedio = efecto_promocion["variacion_pct"].mean()

    return efecto_promocion, incremento_promedio
    