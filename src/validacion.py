import polars as pl


def validar_dataset(df):

    filas, columnas = df.shape

    print("Dataset")
    print("-" * 40)

    print(f"Filas: {filas:,}")
    print(f"Columnas: {columnas}")

    print("\nTipos:")
    print(df.schema)

    nulos = (
        df.null_count().transpose(
            include_header=True,
            header_name="columna",
            column_names=["nulos"]
        )
        .with_columns((pl.col("nulos") / filas * 100).round(2).alias("porcentaje"))
        .filter(pl.col("nulos") > 0)
        .sort("porcentaje", descending=True)
    )

    print("\nNulos:")
    print(nulos if nulos.height > 0 else "Sin valores nulos")

    duplicados = filas - df.unique().height

    print(
        f"\nDuplicados: {duplicados} "
        f"({duplicados / filas * 100:.2f}%)"
    )

    if "date" in df.columns:

        rango = df.select([
            pl.col("date").min().alias("inicio"),
            pl.col("date").max().alias("fin")
        ])

        print(f"\nRango fechas: "
            f"{rango['inicio'][0]} - {rango['fin'][0]}")

    return duplicados



def limpieza_final(ventas_completo, duplicados):

    ventas_completo = ventas_completo.with_columns([

        pl.col("transactions").fill_null(0),

        pl.col("dcoilwtico").interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),

        pl.col("onpromotion").fill_null(0),

        pl.col("sales").fill_null(pl.col("sales").median()),

        pl.col("type_right").fill_null("Normal"),
        pl.col("locale").fill_null("National"),
        pl.col("locale_name").fill_null("Ecuador"),
        pl.col("description").fill_null("Sin feriado"),
        pl.col("transferred").fill_null(False)

    ])


    if duplicados > 0:
        ventas_completo = ventas_completo.unique(
            maintain_order=True
        )


    print("\nEstado final:")
    print(f"Filas: {ventas_completo.height:,}")
    print(f"Columnas: {ventas_completo.width}")


    nulos_finales = (
        ventas_completo.null_count().transpose(
            include_header=True,
            header_name="columna",
            column_names=["nulos"]
        )
        .filter(pl.col("nulos") > 0)
    )


    print("\nNulos después de limpieza:")
    print(
        nulos_finales
        if nulos_finales.height > 0
        else "Sin valores nulos"
    )

    print("=" * 50)


    return ventas_completo

