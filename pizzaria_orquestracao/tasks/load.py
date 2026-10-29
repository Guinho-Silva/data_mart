from prefect import task
from database.conexao import conn_olap


STATUS_FATURAMENTO = {
    "Em preparo": 0,
    "Entregue": 1,
    "Cancelado": 0,
}


def garantir_dimensoes(cursor, registros):
    # Cliente especial para pedidos sem cliente associado.
    cursor.execute("""
        INSERT INTO dim_cliente (sk_cliente, id_cliente_oltp, nome)
        VALUES (0, 0, 'Cliente não fidelizado')
        ON DUPLICATE KEY UPDATE nome = 'Cliente não fidelizado'
    """)

    for r in registros:
        # Cliente: id_cliente_oltp é chave natural; sk_cliente é gerada pelo banco.
        if r["id_cliente_oltp"] != 0:
            cursor.execute("""
                INSERT INTO dim_cliente (id_cliente_oltp, nome)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE nome = VALUES(nome)
            """, (r["id_cliente_oltp"], r["nome_cliente"]))

        # Produto e seus atributos descritivos.
        cursor.execute("""
            INSERT INTO dim_produto
                (id_produto_oltp, nome_produto, categoria, preco_atual, ativo)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                nome_produto = VALUES(nome_produto),
                categoria = VALUES(categoria),
                preco_atual = VALUES(preco_atual),
                ativo = VALUES(ativo)
        """, (
            r["id_produto_oltp"], r["nome_produto"], r["categoria"],
            r["preco_atual"], r["ativo"]
        ))

        # Data: chave no formato AAAAMMDD.
        cursor.execute("""
            INSERT INTO dim_tempo
                (sk_tempo, data, ano, trimestre, mes, nome_mes, ano_mes,
                 dia, dia_semana, nome_dia_semana, fim_de_semana)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                data = VALUES(data),
                ano = VALUES(ano),
                trimestre = VALUES(trimestre),
                mes = VALUES(mes),
                nome_mes = VALUES(nome_mes),
                ano_mes = VALUES(ano_mes),
                dia = VALUES(dia),
                dia_semana = VALUES(dia_semana),
                nome_dia_semana = VALUES(nome_dia_semana),
                fim_de_semana = VALUES(fim_de_semana)
        """, (
            r["sk_tempo"], r["data"], r["ano"], r["trimestre"], r["mes"],
            r["nome_mes"], r["ano_mes"], r["dia"], r["dia_semana"],
            r["nome_dia_semana"], r["fim_de_semana"]
        ))

        # Dimensão hora: sk_hora representa a hora cheia (0 a 23).
        cursor.execute("""
            INSERT INTO dim_hora (sk_hora, hora)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE hora = VALUES(hora)
        """, (r["sk_hora"], f'{r["sk_hora"]:02d}:00'))

        # Dimensões de tipo e status podem não ter AUTO_INCREMENT.
        cursor.execute(
            "SELECT sk_tipo_pedido FROM dim_tipo_pedido WHERE tipo_pedido = %s",
            (r["tipo_pedido"],)
        )
        if cursor.fetchone() is None:
            cursor.execute("SELECT COALESCE(MAX(sk_tipo_pedido), 0) + 1 FROM dim_tipo_pedido")
            sk_tipo = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO dim_tipo_pedido (sk_tipo_pedido, tipo_pedido) VALUES (%s, %s)",
                (sk_tipo, r["tipo_pedido"])
            )

        cursor.execute(
            "SELECT sk_status_pedido FROM dim_status_pedido WHERE status_pedido = %s",
            (r["status_pedido"],)
        )
        if cursor.fetchone() is None:
            cursor.execute("SELECT COALESCE(MAX(sk_status_pedido), 0) + 1 FROM dim_status_pedido")
            sk_status = cursor.fetchone()[0]
            conta = STATUS_FATURAMENTO.get(r["status_pedido"], 0)
            cursor.execute("""
                INSERT INTO dim_status_pedido
                    (sk_status_pedido, status_pedido, conta_faturamento)
                VALUES (%s, %s, %s)
            """, (sk_status, r["status_pedido"], conta))


def obter_sk(cursor, tabela, coluna_sk, coluna_natural, valor):
    # Os nomes das tabelas/colunas vêm apenas de constantes internas abaixo.
    cursor.execute(
        f"SELECT {coluna_sk} FROM {tabela} WHERE {coluna_natural} = %s",
        (valor,)
    )
    resultado = cursor.fetchone()
    if resultado is None:
        raise ValueError(f"Não foi possível localizar chave em {tabela}: {valor}")
    return resultado[0]


@task(name="Load - Data Mart", retries=2, retry_delay_seconds=5)
def carregar_vendas(registros):
    if not registros:
        print("Nenhum item encontrado no OLTP; nada para carregar.")
        return {"recebidos": 0, "processados": 0, "ids": []}

    conn = None
    cursor = None
    ids = [r["id_item_oltp"] for r in registros]

    try:
        conn = conn_olap()
        conn.start_transaction()
        cursor = conn.cursor()

        garantir_dimensoes(cursor, registros)

        sql_fato = """
            INSERT INTO fato_vendas (
                id_item_oltp, id_pedido_oltp, sk_tempo, sk_hora,
                sk_produto, sk_cliente, sk_tipo_pedido, sk_status_pedido,
                quantidade, valor_unitario, valor_bruto
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                id_pedido_oltp = VALUES(id_pedido_oltp),
                sk_tempo = VALUES(sk_tempo),
                sk_hora = VALUES(sk_hora),
                sk_produto = VALUES(sk_produto),
                sk_cliente = VALUES(sk_cliente),
                sk_tipo_pedido = VALUES(sk_tipo_pedido),
                sk_status_pedido = VALUES(sk_status_pedido),
                quantidade = VALUES(quantidade),
                valor_unitario = VALUES(valor_unitario),
                valor_bruto = VALUES(valor_bruto)
        """

        for r in registros:
            sk_cliente = obter_sk(
                cursor, "dim_cliente", "sk_cliente", "id_cliente_oltp",
                r["id_cliente_oltp"]
            )
            sk_produto = obter_sk(
                cursor, "dim_produto", "sk_produto", "id_produto_oltp",
                r["id_produto_oltp"]
            )
            sk_tipo = obter_sk(
                cursor, "dim_tipo_pedido", "sk_tipo_pedido", "tipo_pedido",
                r["tipo_pedido"]
            )
            sk_status = obter_sk(
                cursor, "dim_status_pedido", "sk_status_pedido", "status_pedido",
                r["status_pedido"]
            )

            cursor.execute(sql_fato, (
                r["id_item_oltp"], r["id_pedido_oltp"], r["sk_tempo"],
                r["sk_hora"], sk_produto, sk_cliente, sk_tipo, sk_status,
                r["quantidade"], r["valor_unitario"], r["valor_bruto"]
            ))

        conn.commit()
        print(f"Carga concluída: {len(registros)} itens processados.")
        return {"recebidos": len(registros), "processados": len(registros), "ids": ids}

    except Exception:
        if conn is not None and conn.is_connected():
            conn.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
