import polars as pl 

def unificar_datasets(datasets): 
    ventas_feriado_unica = datasets["ventas_feriado"].group_by("date").first()

    ventas_completo = (
        datasets["ventas_principal"]
        .join(datasets["ventas_54"], on="store_nbr", how="left")
        .join(datasets["transacciones"], on=["date", "store_nbr"], how="left")
        .join(datasets["precio_petroleo"], on="date", how="left")
        .join(ventas_feriado_unica, on="date", how="left")
        .with_columns(pl.col("transactions").fill_null(0))
    )

    return ventas_completo


