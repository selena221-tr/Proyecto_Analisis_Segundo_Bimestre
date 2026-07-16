import polars as pl 

#	Relación entre número de transacciones y volumen de ventas por tienda.
def relacion_transacciones_ventas(ventas_completo): 
    ventas_transacciones_tienda = (
        ventas_completo
        .group_by("store_nbr")
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("transactions").sum().alias("transacciones_totales")
        ])
    )

    correlacion_ventas_transacciones = ventas_transacciones_tienda.select(
        pl.corr("ventas_totales", "transacciones_totales").alias("correlacion")
    )
    
    return ventas_transacciones_tienda, correlacion_ventas_transacciones


#	Identificación de tiendas con ticket promedio alto (pocas transacciones, altas ventas) vs ticket bajo (muchas transacciones, bajas ventas).
def tiendas_promedio(ventas_transacciones_tienda):
    ticket_promedio = (
        ventas_transacciones_tienda
        .filter(pl.col("transacciones_totales") > 0)
        .with_columns(
            (pl.col("ventas_totales")/ pl.col("transacciones_totales"))
            .round(2)
            .alias("ticket_promedio")
        )
        .sort("ticket_promedio", descending=True)
    )

    return ticket_promedio
