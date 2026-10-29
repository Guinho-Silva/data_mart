from datetime import datetime
from decimal import Decimal
from prefect import task


MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}
DIAS_SEMANA = {
    1: "Domingo", 2: "Segunda-feira", 3: "Terça-feira",
    4: "Quarta-feira", 5: "Quinta-feira", 6: "Sexta-feira",
    7: "Sábado",
}


def converter_datetime(valor):
    if isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor))


@task(name="Transform - Preparar dados", retries=1, retry_delay_seconds=3)
def transformar_vendas(registros):
    """
    Normaliza tipos e calcula chaves de data/hora e valor bruto do item.
    O valor bruto é quantidade * preço unitário; descontos não são usados.
    """
    transformados = []

    for registro in registros:
        data_hora = converter_datetime(registro["data_hora_pedido"])
        data = data_hora.date()
        dia_semana = ((data_hora.weekday() + 1) % 7) + 1  # domingo=1 ... sábado=7

        quantidade = int(registro["quantidade"])
        valor_unitario = Decimal(str(registro["valor_unitario"]))
        valor_bruto = (valor_unitario * quantidade).quantize(Decimal("0.01"))

        id_cliente = registro["id_cliente_oltp"]
        nome_cliente = registro["nome_cliente"]

        transformados.append({
            "id_item_oltp": int(registro["id_item_oltp"]),
            "id_pedido_oltp": int(registro["id_pedido_oltp"]),
            "data_hora_pedido": data_hora,
            "data": data,
            "sk_tempo": int(data.strftime("%Y%m%d")),
            "sk_hora": data_hora.hour,
            "ano": data.year,
            "trimestre": ((data.month - 1) // 3) + 1,
            "mes": data.month,
            "nome_mes": MESES[data.month],
            "ano_mes": data.strftime("%Y-%m"),
            "dia": data.day,
            "dia_semana": dia_semana,
            "nome_dia_semana": DIAS_SEMANA[dia_semana],
            "fim_de_semana": int(dia_semana in (1, 7)),
            "status_pedido": registro["status_pedido"],
            "tipo_pedido": registro["tipo_pedido"],
            "id_cliente_oltp": int(id_cliente) if id_cliente is not None else 0,
            "nome_cliente": nome_cliente if id_cliente is not None and nome_cliente else "Cliente não fidelizado",
            "id_produto_oltp": int(registro["id_produto_oltp"]),
            "nome_produto": registro["nome_produto"],
            "categoria": registro["categoria"] or "Sem categoria",
            "preco_atual": Decimal(str(registro["preco_atual"])),
            "ativo": int(registro["status_produto"] == "Ativo"),
            "quantidade": quantidade,
            "valor_unitario": valor_unitario,
            "valor_bruto": valor_bruto,
        })

    print(f"Transformados {len(transformados)} itens.")
    return transformados
