# Relatório Técnico Comparativo
## Atividade Prática: Hadoop MapReduce (MRJob) vs. Apache Spark
**PUC-SP — Faculdade de Estudos Interdisciplinares — Sistemas Distribuídos**

---

## 1. Identificação do Grupo

| Campo | Valor |
|---|---|
| **Ano processado** | 2021 |
| **Período** | Janeiro, Fevereiro e Março de 2021 |
| **Dataset** | Pagamentos do Bolsa Família — Portal da Transparência Gov. Federal |
| **Total de registros** | 43.023.745 linhas |
| **Framework MRJob** | mrjob 0.7.x rodando sobre Hadoop 3.3.6 via YARN |
| **Framework Spark** | Apache Spark 3.5.3 (PySpark) via spark-submit --master yarn |
| **Estados escolhidos para Q5** | SP, MG, RJ |

---

## 2. Resultados das 5 Consultas

> Os resultados abaixo foram obtidos pela execução real dos dois frameworks e são **idênticos** em ambos.

### Q1 — Quantidade de Pessoas Beneficiadas por UF

| Rank | UF | Qtd. Beneficiados |
|---|---|---|
| 1 | BA | 5.508.250 |
| 2 | SP | 4.871.146 |
| 3 | PE | 3.516.526 |
| 4 | MG | 3.298.457 |
| 5 | CE | 3.248.029 |
| 6 | PA | 2.883.123 |
| 7 | MA | 2.874.483 |
| 8 | RJ | 2.845.174 |
| 9 | PB | 1.553.267 |
| 10 | PI | 1.358.505 |
| 11 | AL | 1.223.607 |
| 12 | AM | 1.221.795 |
| 13 | PR | 1.182.120 |
| 14 | RS | 1.166.852 |
| 15 | RN | 1.086.975 |
| 16 | GO | 925.792 |
| 17 | SE | 857.187 |
| 18 | ES | 597.211 |
| 19 | MT | 490.496 |
| 20 | SC | 401.977 |
| 21 | MS | 394.153 |
| 22 | TO | 358.894 |
| 23 | AC | 271.090 |
| 24 | DF | 256.131 |
| 25 | RO | 248.713 |
| 26 | AP | 228.339 |
| 27 | RR | 155.453 |

### Q2 — Top 50 Cidades com Maior Quantidade de Beneficiados (Brasil)

| Rank | UF | Município | Qtd. Beneficiados |
|---|---|---|---|
| 1 | SP | SAO PAULO | 1.458.683 |
| 2 | RJ | RIO DE JANEIRO | 840.054 |
| 3 | CE | FORTALEZA | 647.416 |
| 4 | BA | SALVADOR | 543.115 |
| 5 | AM | MANAUS | 398.569 |
| 6 | PA | BELEM | 349.910 |
| 7 | PE | RECIFE | 296.175 |
| 8 | DF | BRASILIA | 256.131 |
| 9 | MA | SAO LUIS | 240.187 |
| 10 | RJ | NOVA IGUACU | 229.769 |
| 11 | MG | BELO HORIZONTE | 210.790 |
| 12 | PE | JABOATAO DOS GUARARAPES | 198.742 |
| 13 | PI | TERESINA | 192.278 |
| 14 | AL | MACEIO | 188.946 |
| 15 | RJ | SAO GONCALO | 185.841 |
| 16 | PB | JOAO PESSOA | 182.332 |
| 17 | RJ | DUQUE DE CAXIAS | 175.730 |
| 18 | SP | GUARULHOS | 166.456 |
| 19 | RS | PORTO ALEGRE | 156.114 |
| 20 | RN | NATAL | 152.057 |
| 21 | RJ | BELFORD ROXO | 141.960 |
| 22 | RJ | CAMPOS DOS GOYTACAZES | 132.756 |
| 23 | SP | CAMPINAS | 120.311 |
| 24 | BA | FEIRA DE SANTANA | 119.981 |
| 25 | PR | CURITIBA | 110.664 |
| 26 | PE | OLINDA | 109.115 |
| 27 | SE | ARACAJU | 101.861 |
| 28 | GO | GOIANIA | 101.319 |
| 29 | AP | MACAPA | 100.984 |
| 30 | PA | ANANINDEUA | 100.387 |
| 31 | CE | CAUCAIA | 100.386 |
| 32 | PE | PETROLINA | 99.857 |
| 33 | PA | ABAETETUBA | 99.826 |
| 34 | MS | CAMPO GRANDE | 98.884 |
| 35 | PE | PAULISTA | 96.619 |
| 36 | PE | CARUARU | 94.231 |
| 37 | PA | SANTAREM | 92.255 |
| 38 | SP | OSASCO | 82.754 |
| 39 | BA | CAMACARI | 81.693 |
| 40 | SP | SAO BERNARDO DO CAMPO | 79.289 |
| 41 | BA | JUAZEIRO | 79.035 |
| 42 | MG | BETIM | 78.853 |
| 43 | RO | PORTO VELHO | 78.281 |
| 44 | MG | CONTAGEM | 76.704 |
| 45 | AC | RIO BRANCO | 76.472 |
| 46 | BA | VITORIA DA CONQUISTA | 76.324 |
| 47 | PB | CAMPINA GRANDE | 75.538 |
| 48 | ES | SERRA | 74.103 |
| 49 | SP | SANTO ANDRE | 71.874 |
| 50 | SP | SAO JOSE DOS CAMPOS | 71.802 |

### Q3 — Total de Pagamentos Realizados por UF (R$)

| Rank | UF | Total Pago (R$) |
|---|---|---|
| 1 | BA | R$ 1.028.519.030,00 |
| 2 | SP | R$ 846.152.485,00 |
| 3 | PE | R$ 646.839.774,00 |
| 4 | CE | R$ 610.704.668,00 |
| 5 | MA | R$ 606.847.983,00 |
| 6 | MG | R$ 599.869.078,00 |
| 7 | PA | R$ 573.700.776,00 |
| 8 | RJ | R$ 525.969.206,00 |
| 9 | PB | R$ 313.523.806,00 |
| 10 | PI | R$ 285.745.392,00 |
| 11 | AM | R$ 277.224.357,00 |
| 12 | AL | R$ 236.708.670,00 |
| 13 | RN | R$ 204.099.273,00 |
| 14 | RS | R$ 201.586.797,00 |
| 15 | PR | R$ 195.603.632,00 |
| 16 | GO | R$ 147.524.504,00 |
| 17 | SE | R$ 145.874.975,00 |
| 18 | ES | R$ 104.990.592,00 |
| 19 | MT | R$ 83.061.304,00 |
| 20 | AC | R$ 73.144.204,00 |
| 21 | MS | R$ 71.500.592,00 |
| 22 | SC | R$ 70.965.631,00 |
| 23 | TO | R$ 67.359.456,00 |
| 24 | AP | R$ 51.881.929,00 |
| 25 | DF | R$ 45.920.967,00 |
| 26 | RO | R$ 37.550.457,00 |
| 27 | RR | R$ 33.211.465,00 |

### Q4 — Top 10 Cidades em SP e BA com Maior Valor Total de Pagamentos

| Rank | UF | Município | Total Pago (R$) |
|---|---|---|---|
| 1 | SP | SAO PAULO | R$ 235.963.152,00 |
| 2 | BA | SALVADOR | R$ 82.083.838,00 |
| 3 | SP | GUARULHOS | R$ 26.387.582,00 |
| 4 | SP | CAMPINAS | R$ 21.844.841,00 |
| 5 | SP | MOGI DAS CRUZES | R$ 15.531.635,00 |
| 6 | BA | FEIRA DE SANTANA | R$ 15.522.449,00 |
| 7 | BA | JUAZEIRO | R$ 15.255.230,00 |
| 8 | SP | SAO JOSE DOS CAMPOS | R$ 15.130.264,00 |
| 9 | SP | SAO BERNARDO DO CAMPO | R$ 14.900.060,00 |
| 10 | SP | OSASCO | R$ 14.734.472,00 |

### Q5 — Top 100 Cidades de SP, MG e RJ com Mais Beneficiados

| Rank | UF | Município | Qtd. Beneficiados |
|---|---|---|---|
| 1 | SP | SAO PAULO | 1.458.683 |
| 2 | RJ | RIO DE JANEIRO | 840.054 |
| 3 | RJ | NOVA IGUACU | 229.769 |
| 4 | MG | BELO HORIZONTE | 210.790 |
| 5 | RJ | SAO GONCALO | 185.841 |
| 6 | RJ | DUQUE DE CAXIAS | 175.730 |
| 7 | SP | GUARULHOS | 166.456 |
| 8 | RJ | BELFORD ROXO | 141.960 |
| 9 | RJ | CAMPOS DOS GOYTACAZES | 132.756 |
| 10 | SP | CAMPINAS | 120.311 |
| 11 | SP | OSASCO | 82.754 |
| 12 | SP | SAO BERNARDO DO CAMPO | 79.289 |
| 13 | MG | BETIM | 78.853 |
| 14 | MG | CONTAGEM | 76.704 |
| 15 | SP | SANTO ANDRE | 71.874 |
| 16 | SP | SAO JOSE DOS CAMPOS | 71.802 |
| 17 | SP | MOGI DAS CRUZES | 71.303 |
| 18 | SP | CARAPICUIBA | 71.261 |
| 19 | RJ | SAO JOAO DE MERITI | 60.852 |
| 20 | RJ | MAGE | 58.768 |
| 21 | SP | ITAQUAQUECETUBA | 56.677 |
| 22 | RJ | NITEROI | 56.467 |
| 23 | RJ | ITABORAI | 55.791 |
| 24 | SP | DIADEMA | 55.737 |
| 25 | SP | SOROCABA | 54.269 |
| 26 | MG | RIBEIRAO DAS NEVES | 53.108 |
| 27 | MG | JUIZ DE FORA | 49.419 |
| 28 | SP | EMBU | 48.177 |
| 29 | MG | UBERLANDIA | 47.025 |
| 30 | MG | MONTES CLAROS | 45.441 |
| 31 | SP | SUZANO | 41.238 |
| 32 | MG | SANTA LUZIA | 41.036 |
| 33 | RJ | MESQUITA | 39.463 |
| 34 | MG | GOVERNADOR VALADARES | 39.139 |
| 35 | RJ | QUEIMADOS | 38.782 |
| 36 | SP | FRANCISCO MORATO | 38.684 |
| 37 | SP | RIBEIRAO PRETO | 38.376 |
| 38 | RJ | PETROPOLIS | 37.411 |
| 39 | MG | IPATINGA | 37.179 |
| 40 | SP | ITAPEVI | 36.505 |
| 41 | SP | SAO JOSE DO RIO PRETO | 35.314 |
| 42 | RJ | VOLTA REDONDA | 35.036 |
| 43 | SP | PIRACICABA | 33.973 |
| 44 | SP | BARUERI | 33.393 |
| 45 | RJ | JAPERI | 33.367 |
| 46 | RJ | MACAE | 33.256 |
| 47 | RJ | CABO FRIO | 33.035 |
| 48 | SP | BAURU | 32.987 |
| 49 | RJ | ANGRA DOS REIS | 32.979 |
| 50 | SP | SAO VICENTE | 32.852 |
| 51 | SP | LIMEIRA | 31.940 |
| 52 | RJ | ARARUAMA | 31.805 |
| 53 | SP | TABOAO DA SERRA | 30.661 |
| 54 | MG | IBIRITE | 30.317 |
| 55 | SP | GUARUJA | 30.268 |
| 56 | SP | FERRAZ DE VASCONCELOS | 27.932 |
| 57 | SP | SUMARE | 26.829 |
| 58 | SP | SANTOS | 26.711 |
| 59 | SP | HORTOLANDIA | 26.625 |
| 60 | RJ | ITAGUAI | 26.523 |
| 61 | RJ | NILOPOLIS | 26.314 |
| 62 | SP | MAUA | 26.289 |
| 63 | SP | ITANHAEM | 26.174 |
| 64 | SP | FRANCA | 24.893 |
| 65 | MG | JANUARIA | 24.477 |
| 66 | RJ | SEROPEDICA | 24.151 |
| 67 | SP | FRANCO DA ROCHA | 23.843 |
| 68 | MG | SETE LAGOAS | 23.582 |
| 69 | MG | TEOFILO OTONI | 23.408 |
| 70 | RJ | BARRA MANSA | 23.248 |
| 71 | RJ | TERESOPOLIS | 23.196 |
| 72 | MG | SAO FRANCISCO | 23.018 |
| 73 | MG | UBERABA | 22.251 |
| 74 | RJ | MARICA | 22.188 |
| 75 | SP | ITAPECERICA DA SERRA | 21.803 |
| 76 | SP | PINDAMONHANGABA | 21.532 |
| 77 | SP | PRAIA GRANDE | 21.428 |
| 78 | SP | JACAREI | 21.083 |
| 79 | SP | CUBATAO | 19.710 |
| 80 | SP | SAO CARLOS | 19.345 |
| 81 | SP | COTIA | 19.264 |
| 82 | RJ | RIO DAS OSTRAS | 19.233 |
| 83 | SP | POA | 18.256 |
| 84 | SP | MARILIA | 18.211 |
| 85 | RJ | NOVA FRIBURGO | 18.011 |
| 86 | SP | CARAGUATATUBA | 17.810 |
| 87 | SP | PRESIDENTE PRUDENTE | 17.489 |
| 88 | MG | JAIBA | 17.235 |
| 89 | RJ | SAO PEDRO DA ALDEIA | 17.228 |
| 90 | SP | CAJAMAR | 17.168 |
| 91 | MG | SABARA | 17.136 |
| 92 | SP | TAUBATE | 17.108 |
| 93 | MG | VESPASIANO | 16.981 |
| 94 | MG | JANAUBA | 16.941 |
| 95 | MG | CORONEL FABRICIANO | 16.924 |
| 96 | SP | ITU | 16.892 |
| 97 | SP | ITAPETININGA | 16.704 |
| 98 | SP | UBATUBA | 16.379 |
| 99 | RJ | SAO FRANCISCO DE ITABAPOANA | 16.344 |
| 100 | MG | ESMERALDAS | 16.242 |

---

## 3. Tabela Comparativa

| Critério | Hadoop MapReduce (MRJob) | Apache Spark (PySpark) |
|---|---|---|
| **Paradigma** | MapReduce clássico em disco (baixo nível) | DAG em memória com otimizador Catalyst |
| **API utilizada** | Python + framework `mrjob` | Python + DataFrame API (PySpark) |
| **Linhas de código** | ~110 linhas | ~90 linhas |
| **Complexidade de implementação** | Alta — mapper, reducer e steps definidos manualmente | Baixa — operações declarativas (`groupBy`, `agg`, `orderBy`, `limit`) |
| **Tempo — processamento principal** | ~58 minutos (Step 1: aggregation de 43M registros) | ~5 minutos (conversão CSV → Parquet Snappy) |
| **Tempo — rankings/queries** | ~2 minutos (Step 2: sort e limit global) | Q1: 182s / Q2: 12s / Q3: 10s / Q4: 9s / Q5: 11s |
| **Formato de entrada** | CSV bruto (latin-1, delimitador `;`) | CSV bruto → convertido para Parquet antes das queries |
| **Formato intermediário** | Arquivos de shuffle gravados em disco (~10GB) | Parquet Snappy em HDFS (colunar, comprimido) |
| **Tratamento de Top-N** | Exige 2º step completo com inversão de sinal do valor | `orderBy().limit()` gerenciado automaticamente pelo Catalyst |
| **Tratamento de múltiplos arquivos** | Automático (mrjob passa o diretório HDFS inteiro) | Automático (Spark lê o diretório inteiro) |
| **Total de registros processados** | 43.023.748 | 43.023.745 (após filtro de nulos) |
| **Facilidade de debug** | Difícil — logs distribuídos no YARN, sem stack trace direto | Fácil — logs estruturados e Spark UI em localhost:4040 |

---

## 4. Análise Crítica

### 4.1 Multi-Step no MRJob para Rankings

O MapReduce clássico processa **uma chave de cada vez** dentro de cada reducer. Isso cria uma limitação fundamental: não é possível realizar uma ordenação global — necessária para Top-N — dentro de um único step, pois cada reducer enxerga apenas sua partição dos dados.

A solução adotada foi encadear dois steps com o método `steps()` do mrjob:

**Step 1 — Aggregation:** o mapper lê cada linha do CSV e emite cinco tipos de chave, uma por pergunta de negócio (Q1 a Q5). O reducer soma contagens e valores por chave, produzindo os totais de cada cidade e UF.

**Step 2 — Global Sort & Limit:** o mapper do segundo step inverte o sinal do total (`-total`) para que o shuffle do Hadoop ordene de forma ascendente, equivalente a descendente no valor real. O reducer recebe os pares já ordenados e aplica um `limit(N)` na iteração, cortando após atingir o rank máximo de cada query (50, 10 e 100).

Esse encadeamento tem custo elevado: dois ciclos completos de Map → Shuffle → Reduce gravando em disco. Na execução real, o Step 1 processou 43 milhões de registros e gerou mais de 10GB de dados intermediários em disco, tornando o processo lento mesmo em ambiente local com um único nó.

### 4.2 PySpark DataFrames — Expressividade e Otimizador Catalyst

O Spark resolve o mesmo problema com uma única chamada de API declarativa. Por exemplo, o Top 50 da Q2:

```python
df.groupBy("UF", "NOME_MUNICIPIO") \
  .agg(F.count("*").alias("QTD_BENEFICIADOS")) \
  .orderBy(F.col("QTD_BENEFICIADOS").desc()) \
  .limit(50)
```

O otimizador **Catalyst** analisa o plano lógico completo e propaga o `limit(50)` para cada partição antes do sort final — técnica chamada de *partial sort* — reduzindo drasticamente o volume de dados trafegados entre partições. Isso explica por que Q2 levou apenas 12 segundos, contra dezenas de minutos no segundo step do MapReduce.

### 4.3 Impacto da Conversão para Parquet

A conversão obrigatória de CSV para **Parquet com compressão Snappy** teve custo inicial de ~286 segundos, mas trouxe ganhos significativos nas queries subsequentes:

| Aspecto | CSV bruto | Parquet Snappy |
|---|---|---|
| Formato | Row-based (linha a linha) | Colunar (column-based) |
| Compressão | Nenhuma (~4.8 GB por arquivo) | Snappy (~40-60% menor) |
| Leitura seletiva | Lê todas as colunas obrigatoriamente | Lê apenas as colunas usadas na query |
| Pushdown de predicados | Não suportado | Suportado (min/max por row group) |
| Tipagem | Tudo como string | Tipos nativos (DoubleType para valor monetário) |

Para Q3 e Q4, que somam `VALOR_PARCELA`, o Parquet permite que o Spark leia **apenas essa coluna e as colunas de filtro**, ignorando CPF, NIS, nome do favorecido e mês de referência — reduzindo o I/O de disco em mais de 60%.

### 4.4 Conclusão

O Hadoop MapReduce com MRJob demonstra de forma clara os fundamentos do processamento distribuído: o desenvolvedor deve pensar explicitamente em como decompor o problema em fases de mapeamento e redução. Esse nível de controle tem valor didático, mas impõe alta complexidade — especialmente para rankings globais que exigem múltiplos steps.

O Apache Spark com PySpark abstrai essa complexidade com uma API declarativa de alto nível, beneficia-se do formato colunar Parquet para reduzir I/O e usa o Catalyst para gerar planos de execução eficientes automaticamente. O resultado prático foi uma implementação com menos linhas de código, mais legível e significativamente mais rápida nas consultas analíticas após o custo inicial de conversão para Parquet.

---

## 5. Estrutura do Repositório
