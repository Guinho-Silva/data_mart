# Orquestração do data mart da pizzaria (Prefect)

Pipeline que popula o `kair_pizzaria_mart_v2` (esquema estrela) a partir do OLTP `kair_pizzaria`.
Usa **Prefect** no lugar do Apache Airflow (bem mais leve para rodar localmente), mantendo os mesmos conceitos de orquestração.

## Desenho do pipeline

```
testar_conexoes
  |-- extrair_clientes --> carregar_dim_cliente --|
  |-- extrair_produtos --> carregar_dim_produto --|--> ler_chaves_dimensoes
  |-- ler_estado_fato  --> extrair_itens ---------|            |
                                                               v
                    transformar_itens --> registrar_rejeitados, carregar_fato_vendas
                                                               |
                                                               v
                                                         validar_carga
```

## Airflow x Prefect (para explicar o projeto)

| Conceito | Airflow | Neste projeto (Prefect) |
|---|---|---|
| Pipeline | DAG | `@flow carga_data_mart` |
| Etapa | Operator / `@task` | `@task` em `etl/tarefas.py` |
| Dependências | `a >> b` | passagem de dados entre tarefas e `.submit()` / `.result()` |
| Paralelismo | tasks sem dependência | `extrair_*.submit()` e `carregar_dim_*.submit()` |
| Retentativas | `retries=2` | `retries=2, retry_delay_seconds=10` |
| Agendamento | `schedule="0 2 * * *"` | `flow.serve(schedule=Cron(...))` |
| Conexões | Connections | variáveis de ambiente (.env); Prefect também tem Blocks |
| Passagem de dados | XCom | valores de retorno das tasks |
| Reprocessamento | backfill / catchup | parâmetros `full_refresh` e `lookback_dias` |
| Interface | Airflow UI | Prefect UI (porta 4200) |

## Decisões de projeto

- **Incremental sem tabela de controle**: o estado é lido do próprio mart. Entram na carga (1) itens com id maior que o último carregado, (2) itens de pedidos ainda "Em preparo" (o status muda depois) e (3) pedidos dos últimos `lookback_dias` dias (mudanças tardias, como Entregue -> Cancelado).
- **Idempotente**: a fato usa upsert por `id_item_oltp`; rodar duas vezes seguidas não duplica nada.
- **Dimensões SCD tipo 1**: só insere novos e atualiza alterados. Não consome `AUTO_INCREMENT` à toa. O preço antigo continua preservado na fato (`valor_unitario`).
- **Cliente nulo**: pedido sem cliente vai para `sk_cliente = 0` ("Cliente não fidelizado").
- **Rejeitados**: item cujo cliente/produto/data não existe no mart não derruba a carga; vai para `rejeitados/*.csv` e o log avisa. A reconciliação final falha enquanto houver rejeitados, para ninguém ignorar o problema.
- **Reconciliação**: compara quantidade de itens e soma de `valor_bruto` entre origem e mart. Pedidos sem itens aparecem como aviso (a fato tem grão de item, então eles não entram).
- **Full refresh seguro**: `DELETE` + recarga na mesma transação; se falhar, o mart fica como estava.
- **Limitação conhecida**: itens apagados na origem só saem do mart com `--full-refresh` (a reconciliação detecta e avisa).
