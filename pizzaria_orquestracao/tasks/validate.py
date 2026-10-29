from prefect import task
from database.conexao import conn_olap


@task(name="Validate - Conferir carga", retries=1, retry_delay_seconds=3)
def validar_carga(registros, resultado):
    if not registros:
        print("Validação concluída: não havia dados para carregar.")
        return

    ids = [r["id_item_oltp"] for r in registros]
    conn = None
    cursor = None

    try:
        conn = conn_olap()
        cursor = conn.cursor()

        placeholders = ", ".join(["%s"] * len(ids))
        cursor.execute(
            f"SELECT COUNT(DISTINCT id_item_oltp) "
            f"FROM fato_vendas WHERE id_item_oltp IN ({placeholders})",
            tuple(ids)
        )
        encontrados = cursor.fetchone()[0]

        if encontrados != len(set(ids)):
            raise ValueError(
                f"Falha de validação: esperados {len(set(ids))} itens únicos, "
                f"encontrados {encontrados} no Data Mart."
            )

        cursor.execute(
            f"SELECT COALESCE(SUM(valor_bruto), 0) "
            f"FROM fato_vendas WHERE id_item_oltp IN ({placeholders})",
            tuple(ids)
        )
        total_mart = cursor.fetchone()[0]

        total_esperado = sum(
            (r["valor_bruto"] for r in registros),
            start=__import__("decimal").Decimal("0.00")
        )

        if total_mart is None or round(float(total_mart), 2) != round(float(total_esperado), 2):
            raise ValueError(
                f"Falha de validação do valor bruto: esperado {total_esperado}, "
                f"encontrado {total_mart}."
            )

        print(
            f"Validação OK: {encontrados} itens únicos; "
            f"valor bruto dos itens = R$ {float(total_mart):.2f}."
        )
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
