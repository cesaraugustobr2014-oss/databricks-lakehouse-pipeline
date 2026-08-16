# 🎯 Guia de Entrevistas Técnicas: Como Defender Este Projeto

Este guia foi elaborado para preparar você para responder às perguntas mais comuns de recrutadores, engenheiros de dados seniores e tech leads sobre as decisões técnicas deste projeto.

---

## 📌 1. Perguntas de Arquitetura & Governança

### P: "Por que você escolheu uma Arquitetura Data Lakehouse em vez de um Data Warehouse ou Data Lake tradicional?"
> **Como responder:**  
> *"Os Data Warehouses tradicionais oferecem excelente governança e suporte a transações ACID, mas têm alto custo de armazenamento e baixa flexibilidade para dados não estruturados ou grandes volumes de APIs. Por outro lado, Data Lakes tradicionais são econômicos, mas frequentemente viram 'Data Swamps' por falta de governança, atomicidade e evolução de schemas.*  
> *O Data Lakehouse unifica o melhor dos dois mundos: armazena dados em formatos abertos (como Delta Lake sobre Parquet) com baixo custo em Object Storage (S3/ADLS/GCS), ao mesmo tempo em que oferece transações ACID, Time Travel e governança centralizada com o Unity Catalog."*

---

### P: "Por que separar o pipeline nas três camadas: Bronze, Silver e Gold?"
> **Como responder:**  
> - **Bronze (Raw Data):** Mantém a cópia imutável e fiel dos dados originais da API com timestamps de coleta. Se a regra de negócio mudar ou um bug for detectado no futuro, podemos reprocessar tudo a partir da Bronze sem consultar a API novamente.
> - **Silver (Cleaned & Conformed):** Aplica qualidade de dados — padronização de tipos de dados (`DoubleType`), unificação de formatos de data (`yyyy-MM-01`), tratamento de nulos e junções referenciais.
> - **Gold (Business Ready):** Contém agregações, métricas analíticas calculadas com Window Functions e visões prontas para serem consumidas por dashboards do Power BI ou Databricks SQL sem necessidade de processamento pesado em tempo de leitura.

---

## ⚡ 2. Perguntas de Performance & Apache Spark

### P: "Como você tratou o problema de pequenos arquivos (Small File Problem) gerados por ingestões frequentes de APIs?"
> **Como responder:**  
> *"Ingestões periódicas de APIs tendem a gerar milhares de pequenos arquivos Parquet/Delta, o que degrada a performance do Spark devido ao overhead de metadados e abertura de conexões I/O.*  
> *Para resolver isso, apliquei o comando `OPTIMIZE` do Delta Lake, que executa bin-packing para compactar arquivos pequenos em arquivos ideais de aproximadamente 1GB. Além disso, configurei as propriedades da tabela `delta.autoOptimize.optimizeWrite = true` e `delta.autoOptimize.autoCompact = true` para realizar essa compactação automaticamente durante as gravações."*

---

### P: "O que é Z-ORDER e por que você o utilizou nas cotações de ações?"
> **Como responder:**  
> *"O Z-ORDER é uma técnica de ordenação multidimensional baseada na curva de Hilbert. Em vez de ordenar linearmente apenas por uma coluna (como o particionamento tradicional), o Z-ORDER co-localiza dados relacionados em blocos com base em múltiplas colunas (no nosso caso, `ticker` e `data`).*  
> *Quando um usuário filtra por um ativo específico em um intervalo de datas, o Spark utiliza as estatísticas mín/máx do Delta Log para pular (*Data Skipping*) mais de 90% dos arquivos que não contêm esses dados, acelerando a consulta drasticamente e reduzindo o consumo de memória e I/O."*

---

### P: "Em que cenário você utilizou Broadcast Hash Join e por que ele é vantajoso?"
> **Como responder:**  
> *"Em operações de Join distribuídas no Spark, o padrão costuma ser o Sort-Merge Join, que exige um 'Shuffle' (envio de dados de todos os nós através da rede para re-particionar pelas chaves do join), o que é computacionalmente caro.*  
> *Ao cruzar uma tabela de fatos grande com uma tabela dimensional pequena (como cadastro de produtos ou lista de tickers), utilizei a diretiva `/*+ BROADCAST(p) */`. O Spark envia uma cópia da tabela menor diretamente para a memória de cada nó executor, eliminando o Shuffle e tornando a junção quase instantânea."*

---

## 🔍 3. Perguntas de Transformação & Lógica Analítica

### P: "Como você calculou as métricas de variação mês a mês sem usar queries lentas de auto-join?"
> **Como responder:**  
> *"Em vez de fazer auto-joins (juntar a tabela com ela mesma no mês anterior, o que causa shuffles pesados), utilizei **Window Functions** nativas do PySpark (`Window.orderBy('data')`) em conjunto com a função `F.lag()`. Isso permitiu acessar o valor do mês imediatamente anterior na mesma partição em um único passe de processamento, calculando a taxa de variação percentual de forma extremamente performática."*

---

### P: "Como você garantiu a segurança de chaves de API e segredos?"
> **Como responder:**  
> *"Nenhuma credencial ou API Key fica hardcoded no código. No Databricks, utilizamos o **Databricks Secrets** (integrado ao Azure Key Vault ou AWS Secrets Manager) via `dbutils.secrets.get()`. Em ambiente local, utilizamos variáveis de ambiente gerenciadas pelo `python-dotenv` com um arquivo `.env` estritamente ignorado pelo `.gitignore`."*
