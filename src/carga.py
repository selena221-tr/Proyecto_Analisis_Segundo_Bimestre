import polars as pl

def cargar_datasets():

    ventas_feriado = pl.read_csv("./data/holidays_events.csv")
    precio_petroleo = pl.read_csv("./data/oil.csv")
    ventas_54 = pl.read_csv("./data/stores.csv")
    ventas_principal = pl.read_csv("./data/train.csv")
    transacciones = pl.read_csv("./data/transactions.csv")

    datasets = {
        "ventas_principal": ventas_principal,
        "ventas_54": ventas_54,
        "ventas_feriado": ventas_feriado,
        "precio_petroleo": precio_petroleo,
        "transacciones": transacciones
    }

    return datasets