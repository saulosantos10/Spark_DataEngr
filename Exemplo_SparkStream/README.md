## Anotações Spark Stream
- Processamento de dados contínuo
-   Em tempo real
-   Batch
    -   OLAP → Staging → DW → Relatório
    -   Com inicio e fim
    -   Carregamento em Lote
    -   Processamento em lote
-   Streaming
    -   E-Commerce → Processamento → Alerta
    -   Contínuo - Sem fim
    -   Carregado a medida que é produzido
    -   Processado a medida que é produzido
-   Spark Structured Streaming
    -   Source → Monitora Transforma → Sink
    -   Bacth →Processamento de conjunto de dados
    -   Streaming →Processamento a medida que os dados são produzidos
    -   Micro-Batchs →Bloco de dados produzidos em intervalo de tempo
-   Modos de Saídas
    -   Append ⇒ Só novas linhas suporta apenas de consultas stateless
    -   Update ⇒ Apenas linhas que foram atualizadas
    -   Complete ⇒ Toda a tabela é atualizada
-   Trigger
    -   Default ⇒ Dispara quando o micro batch termina
    -   Tempo ⇒ Define o tempo que vai ser executado
    -   Once ⇒ Apenas uma unica vez
    -   Continuous ⇒ Processamento continuo
    -   Stop() para o processamento
-   Checkpointdir
    -   Diretório onde o estado de andamento é salvo
    -   Se você parar o processo e reinicar com o mesmo diretório, ele segue de onde parou

- Executar os script no console
	- spark-submit nomedoarquivo
	- spark-submit - -jars /local_onde_esta_driver_jdbc nomeaplicacao => jdbc driver para acessar banco pelo Spark