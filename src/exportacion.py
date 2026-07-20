def exportar_postgres(recopilacion):
    uri = "postgresql://postgres:1234@localhost:5432/proyectoAnalisis"
    for nombre_tabla, df in recopilacion.items():
        df.write_database(
            table_name=nombre_tabla,
            connection=uri,
            if_table_exists="replace",
        )
        print(f"{nombre_tabla} exportada correctamente.")
