import polars as pl

# PREGUNTA 1:  Distribución de ventas por familia de producto: 
# ¿qué categorías concentran el mayor volumen?

def distribucion_ventas_familias(ventas_completo): 
    ventas_por_familia = (ventas_completo.group_by("family")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
        .with_columns(
            (pl.col("ventas_totales") / pl.col("ventas_totales").sum() * 100).round(2).alias("porcentaje")
        )
    )
    return ventas_por_familia

# PREGUNTA 2: Ventas totales por tienda y ranking de 
# las 10 tiendas con mayor y menor venta.

def ventas_tienda_ranking(ventas_completo): 
    ventas_por_tienda = (ventas_completo.group_by("store_nbr")
    .agg(pl.col("sales").sum().alias("ventas_totales"))
    .sort("ventas_totales", descending=True)
    )

    return ventas_por_tienda

# PREGUNTA 3: •	Ventas promedio por ciudad y provincia.

def ventas_ciudad_provincia(ventas_completo):
    ventas_por_ciudad_provincia = (
        ventas_completo.group_by(["state", "city"])
        .agg(pl.col("sales").mean().round(2).alias("venta_promedio"))
        .sort("venta_promedio", descending=True)
    )
    
    return ventas_por_ciudad_provincia


# Evolución temporal de ventas: tendencia mensual y anual entre 2013 y 2017.

def evolucion_temporal(ventas_completo):
    ventas_mensual = (
        ventas_completo
        .with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes"),
        ])
        .group_by(["anio", "mes"])
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort(["anio", "mes"])
    )
    

    ventas_anual = (
        ventas_completo
        .with_columns(pl.col("date").dt.year().alias("anio"))
        .group_by("anio")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("anio")
    )
    
    return ventas_mensual, ventas_anual




