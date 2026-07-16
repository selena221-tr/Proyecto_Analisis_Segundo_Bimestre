import polars as pl

def estandarizacion_fechas(datasets): 
    columna_fecha = 'date'

    for nombre, d in datasets.items(): 
        try:
            datasets[nombre] = d.with_columns(
              pl.col(columna_fecha).cast(pl.Utf8).str.to_date(format="%Y-%m-%d", strict=False)
            )
        
            rango = datasets[nombre].select([
                pl.col(columna_fecha).min().alias("minimo"),
                pl.col(columna_fecha).max().alias("maximo")
            ])
        
            print(f"Dataset: {nombre}")    
            print(f"  Rango de fechas: {rango['minimo'][0]} hasta {rango['maximo'][0]}")
            print("-" * 45)

        except pl.ColumnNotFoundError:
            print(f"Dataset: {nombre}") 
            print("No existe la columna fecha")
            print("-" * 45)
    return datasets


def eliminar_duplicados(datasets): 
    for nombre, d in datasets.items():
        filas_antes = d.height
        datasets[nombre] = d.unique(maintain_order=True)
        filas_despues = datasets[nombre].height
        print(f"{nombre}: {filas_antes - filas_despues} duplicados eliminados")
    return datasets



def imputacion_nulos(datasets): 
    oil = datasets["precio_petroleo"]
    rango_completo = pl.DataFrame({
        "date": pl.date_range(oil["date"].min(), oil["date"].max(), interval="1d", eager=True)
    })
    oil_completo = (
        rango_completo.join(oil, on="date", how="left")
        .sort("date")
        .with_columns(pl.col("dcoilwtico").interpolate())
        .with_columns(pl.col("dcoilwtico").fill_null(strategy="backward").fill_null(strategy="forward"))
    )
    print(f"precio_petroleo:\nFilas \nAntes: {oil.height}\nDespues: {oil_completo.height}")
    print(f"\nNulos \nAntes: {oil['dcoilwtico'].null_count()}\nDespues: {oil_completo['dcoilwtico'].null_count()}")
    datasets["precio_petroleo"] = oil_completo

    print()

    for nombre, d in datasets.items():
        if nombre == "precio_petroleo":
            continue
        filas_antes, columnas_antes = d.shape
        for col, dtype in d.schema.items():
            if d[col].null_count() == 0:
                continue
            if dtype.is_numeric():
                valor = d[col].median()
            else:
                valor = d[col].mode().first()
            d = d.with_columns(pl.col(col).fill_null(valor))
        filas_despues, columnas_despues = d.shape
        print(f"{nombre}:\nFilas \nAntes: {filas_antes}\nDespues: {filas_despues}")
        print(f"Columnas \nAntes: {columnas_antes}\nDespues: {columnas_despues}\n")
        datasets[nombre] = d
    return datasets


def correccion_tipo_datos(datasets):
    datasets["ventas_principal"] = datasets["ventas_principal"].with_columns([
        pl.col("family").cast(pl.Categorical),
    ])

    datasets["ventas_54"] = datasets["ventas_54"].with_columns([
        pl.col("city").cast(pl.Categorical),
        pl.col("state").cast(pl.Categorical),
        pl.col("type").cast(pl.Categorical),
    ])

    datasets["ventas_feriado"] = datasets["ventas_feriado"].with_columns([
        pl.col("type").cast(pl.Categorical),
        pl.col("locale").cast(pl.Categorical),
        pl.col("locale_name").cast(pl.Categorical),
    ])

    for nombre, d in datasets.items():
        print(f"{nombre}: {d.schema}")
    
    return datasets