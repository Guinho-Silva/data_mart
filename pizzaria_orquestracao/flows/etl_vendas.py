from prefect import flow
from tasks.extract import extrair_vendas
from tasks.transform import transformar_vendas
from tasks.load import carregar_vendas
from tasks.validate import validar_carga


@flow(name="ETL Vendas - Kairós", log_prints=True)
def etl_vendas():
    dados_brutos = extrair_vendas()
    dados_transformados = transformar_vendas(dados_brutos)
    resultado = carregar_vendas(dados_transformados)
    validar_carga(dados_transformados, resultado)


if __name__ == "__main__":
    etl_vendas()
