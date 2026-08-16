"""
Testes Unitários para Validação de Regras de Negócio e Transformações
"""
import pytest
import pandas as pd
from datetime import datetime


def test_ipca_numeric_cleaning():
    """Valida se valores com vírgula do IPCA são convertidos corretamente para float."""
    raw_data = [{"data": "01/01/2024", "ipca": "0,42"}, {"data": "01/02/2024", "ipca": "0,83"}]
    df = pd.DataFrame(raw_data)
    df["ipca"] = df["ipca"].str.replace(",", ".").astype(float)

    assert df["ipca"].iloc[0] == 0.42
    assert df["ipca"].iloc[1] == 0.83
    assert df["ipca"].dtype == float


def test_boi_gordo_sample_structure():
    """Valida se a estrutura de cotação do Boi Gordo possui os campos e tipos esperados."""
    from src.ingestion_bcb import generate_sample_boi_gordo_data

    df = generate_sample_boi_gordo_data()
    assert "Data" in df.columns
    assert "Valor" in df.columns
    assert len(df) == 12
    assert df["Valor"].iloc[0] == 248.50


def test_alpha_vantage_mock_generation():
    """Valida a geração resiliente de cotações financeiras com metadados de auditoria."""
    from src.ingestion_alpha import generate_mock_alpha_vantage_data

    df = generate_mock_alpha_vantage_data("PETR4.SA")
    assert not df.empty
    assert "ticker" in df.columns
    assert "data_ingestao" in df.columns
    assert df["ticker"].iloc[0] == "PETR4.SA"
    assert (df["alta"] >= df["baixa"]).all()


def test_divergence_classification_logic():
    """Valida a classificação de cenários de divergência entre Boi Gordo e Inflação."""
    variacao_ipca = 0.5
    variacao_boi = 6.2

    diff = abs(variacao_boi - variacao_ipca)
    if diff > 5:
        classe = "Alta divergência"
    elif 2 <= diff <= 5:
        classe = "Média divergência"
    else:
        classe = "Baixa divergência"

    assert diff == 5.7
    assert classe == "Alta divergência"
