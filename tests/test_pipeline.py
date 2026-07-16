from pathlib import Path

from src.etl import ejecutar_pipeline


def test_ejecutar_pipeline_crea_reporte_y_dataframe():
    repo_root = Path(__file__).resolve().parents[1]

    resultado = ejecutar_pipeline(base_dir=repo_root, exportar_a_postgres=False)

    assert "ventas_limpia" in resultado
    assert resultado["ventas_limpia"].shape[0] > 0
    assert (repo_root / "reportes" / "reporte_datasets.json").exists()
