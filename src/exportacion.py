from sqlalchemy import create_engine

def exportar_postgres(recopilacion):

    engine = create_engine(
        "postgresql+psycopg2://postgres:1234@localhost:5432/proyectoAnalisis"
    )

    for nombre_tabla, df in recopilacion.items():

        df.to_pandas().to_sql(
            nombre_tabla,
            engine,
            if_exists="replace",
            index=False
        )

        print(f"{nombre_tabla} exportada correctamente.")