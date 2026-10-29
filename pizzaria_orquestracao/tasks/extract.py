from prefect import task

from database.conexao import conn_oltp


# Tarefa a ser executada
@task(name="Extract - OLTP", retries=2, retry_delay_seconds=5)

# Função para extrair os dados vindo do oltp
def extrair_vendas():
    # Query
    consulta = """
        SELECT
            ip.Id_itens AS id_item_oltp,
            p.Id_pedido AS id_pedido_oltp,
            p.Data_hora_pedido AS data_hora_pedido,
            p.Status_pedido AS status_pedido,
            p.Tipo_pedido AS tipo_pedido,
            p.Id_cliente AS id_cliente_oltp,
            c.Nome AS nome_cliente,
            pr.Id_produto AS id_produto_oltp,
            pr.Nome_produto AS nome_produto,
            cat.Nome_categoria AS categoria,
            pr.Preco AS preco_atual,
            pr.Status AS status_produto,
            ip.Quantidade AS quantidade,
            ip.Preco_unitario AS valor_unitario
        FROM itens_pedido ip
        INNER JOIN pedido p
            ON p.Id_pedido = ip.Id_pedido
        INNER JOIN produto pr
            ON pr.Id_produto = ip.Id_produto
        LEFT JOIN categoria cat
            ON cat.Id_categoria = pr.Id_categoria
        LEFT JOIN cliente c
            ON c.Id_cliente = p.Id_cliente
        ORDER BY p.Data_hora_pedido, p.Id_pedido, ip.Id_itens
    """
    # Variaveis a ser utilizadas para fechar e executar a query
    conn = None
    cursor = None

    # Tentativa
    try:   
        # Conexão
        conn = conn_oltp()

        # Cursor recebe conexão
        cursor = conn.cursor(dictionary=True)

        # Executa a query
        cursor.execute(consulta)

        # Armazena os dados
        dados = cursor.fetchall()
        print(f"Extraídos {len(dados)} itens do OLTP.")

        return dados

    finally:
        # Fechamento do cursor e da Conexão
        if cursor is not None:
            cursor.close()

        if conn is not None and conn.is_connected():
            conn.close()
