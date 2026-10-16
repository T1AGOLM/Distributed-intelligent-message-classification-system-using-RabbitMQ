# Sistema de Classificação Inteligente de Mensagens Distribuído usando RabbitMQ

Um sistema distribuído construído com **Java**, **Docker**, **RabbitMQ** e **Inteligência Artificial embarcada** que gera, roteia e classifica automaticamente mensagens por meio de dois consumidores independentes.

Mensagens de dois tipos diferentes — avaliações de produtos e solicitações de suporte — são publicadas em um **Topic Exchange** do RabbitMQ e consumidas por dois serviços separados, cada um com seu próprio mecanismo de classificação baseado em IA.

---

## 📋 Sumário

- [Arquitetura](#-arquitetura)
- [Como Funciona](#-como-funciona)
- [Tech Stack](#-tech-stack)
- [Pré-requisitos](#-pré-requisitos)
- [Como Começar](#-como-começar)
- [As IAs](#-as-ias)
- [Fluxo de Mensagens](#-fluxo-de-mensagens)
- [Recursos de Confiabilidade](#-recursos-de-confiabilidade)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Como Demonstrar](#-como-demonstrar)
- [Licença](#-licença)

---

## 🏗 Arquitetura

O sistema é composto por **4 containers** conectados à mesma rede Docker, orquestrados pelo Docker Compose:

```
┌──────────────────────┐        ┌─────────────────────────────────────┐
│  Gerador de Mensagens│        │              RabbitMQ               │
│      (Java)          │        │         Topic Exchange              │
│                      │  publica     routing keys:                  │
│  ≥ 5 mensagens/seg   ├───────►•  review.product                    │
│  - avaliações de     │        │  support.request                   │
│    produtos          │        │                                     │
│  - solicitações de   │        │  ┌──────────────┐ ┌──────────────┐  │
│    suporte           │        │  │ Fila:        │ │ Fila:        │  │
                                │  │ reviews      │ │ support      │  │
                                │  │ + DLQ        │ │ + DLQ        │  │
                                │  └──────┬───────┘ └──────┬───────┘  │
                                └─────────┼────────────────┼──────────┘
                                          │                │
                     ┌────────────────────▼──┐   ┌─────────▼──────────────┐
                     │  Consumer 1 (Java)    │   │  Consumer 2 (Java)     │
                     │  Análise de Sentimento│   │  Classificação de       │
                     │  positivo/negativo/   │   │  Suporte               │
                     │  neutro               │   │  acess/performance/    │
                     │  (Smile ML library)   │   │  conexão               │
                     │                       │   │  (Smile ML library)    │
                     └───────────────────────┘   └────────────────────────┘
```

| Container | Papel |
|-----------|-------|
| **Gerador de Mensagens** | Gera continuamente pelo menos 5 mensagens por segundo e as publica no RabbitMQ, marcando cada mensagem com seu tipo. |
| **RabbitMQ** | Roteia mensagens por um Topic Exchange usando as routing keys `review.product` e `support.request`. Uma fila por consumidor, além de uma Dead Letter Queue (DLQ) para mensagens com erro. Management UI habilitado. |
| **Consumer 1 – Análise de Sentimento** | Consome avaliações de produtos e classifica cada uma como **positivo**, **negativo** ou **neutro** usando um modelo de IA embarcada. Os resultados são impressos no console. |
| **Consumer 2 – Classificação de Solicitações de Suporte** | Consome solicitações de suporte e classifica cada uma como **problema de acesso**, **problema de desempenho** ou **problema de conexão** usando um modelo de IA embarcada. Os resultados são impressos no console. |

---

## ⚙️ Como Funciona

1. O **Gerador** cria mensagens textuais de dois tipos — avaliações de produtos e solicitações de suporte — a uma taxa de pelo menos 5 mensagens por segundo.
2. Cada mensagem contém um **ID único**, **timestamp de geração**, **tipo** e **conteúdo**.
3. As mensagens são publicadas em um **Topic Exchange** com a routing key `review.product` ou `support.request`.
4. O exchange encaminha cada mensagem para a fila do consumidor correspondente.
5. Cada consumidor processa as mensagens **intencionalmente mais devagar que a taxa de chegada** (configurável por consumidor via `RABBITMQ_PROCESSING_DELAY_MS`: 15 ms para o Consumer 1, 50 ms para o Consumer 2). Isso é mais devagar que a velocidade do produtor, permitindo **observar o crescimento das filas na interface de gerenciamento do RabbitMQ**.
6. Cada resultado de classificação inclui a **confiança** do modelo (a probabilidade prevista da classe escolhida), impressa no console do consumidor.
7. Os resultados de classificação (`positivo / negativo / neutro` ou `acesso / desempenho / conexão`) são exibidos no console de cada consumidor.
8. O consumo usa **ACK manual**: uma mensagem só é confirmada após o processamento bem-sucedido. Se o processamento falhar, a mensagem é encaminhada para uma **Dead Letter Queue (DLQ)** em vez de ser descartada.

---

## 🛠 Tech Stack

| Tecnologia | Finalidade |
|------------|------------|
| **Java** | Implementação do gerador e dos dois consumidores |
| **RabbitMQ** | Broker de mensagens com Topic Exchange, DLQ e management UI |
| **Docker / Docker Compose** | Containerização e orquestração dos 4 serviços |
| **Smile** | Biblioteca de machine learning embarcada usada pelos dois consumidores para classificação |
| **SLF4J / Logback** *(planejado)* | Logging estruturado no console dos resultados de classificação |

---

## 📦 Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) com Docker Compose v2
- Java JDK 17+ *(necessário apenas para executar os serviços fora do Docker)*

---

## 🚀 Como Começar

```bash
# 1. Clonar o repositório
git clone https://github.com/<your-username>/<repository-name>.git
cd <repository-name>

# 2. Construir e iniciar os 4 containers
docker compose up --build

# 3. Abrir a interface de gerenciamento do RabbitMQ (credenciais padrão: guest/guest)
   http://localhost:15672

# 4. Acompanhar os resultados de classificação (cada linha agora inclui a confiança
#    do modelo, e os consumidores imprimem um relatório periódico com a estimativa
#    da profundidade da fila e o tempo médio de processamento por mensagem)
docker compose logs -f consumer-1   # análise de sentimentos
docker compose logs -f consumer-2   # classificação de solicitações de suporte

# 5. Parar o sistema
docker compose down
```

---

## 🤖 As IAs

Ambos os consumidores embutem seu próprio mecanismo de IA independente, alimentado pela biblioteca de machine learning [Smile](https://haifengl.github.io/smile/) para Java.

### Consumer 1 — Análise de Sentimento
- **Tarefa:** classificação multiclasse de avaliações de produtos em texto livre.
- **Classes:** `positivo`, `negativo`, `neutro`.
- **Abordagem:** os atributos textuais são extraídos do conteúdo da avaliação e alimentados em um classificador Smile embarcado, treinado com um conjunto de dados rotulado de avaliações.

### Consumer 2 — Classificação de Solicitações de Suporte
- **Tarefa:** categorização de solicitações de suporte técnico.
- **Classes:** `problema de acesso`, `problema de desempenho`, `problema de conexão`.
- **Abordagem:** cada solicitação é convertida em vetores de atributos e classificada por seu próprio modelo Smile embarcado, treinado com tickets de suporte rotulados.

> Cada consumidor possui seu próprio classificador — nenhum serviço externo de IA ou chamada de rede é necessário em runtime.

---

## 📨 Fluxo de Mensagens

```
Gerador ──► Topic Exchange ──┬──► review.queue   ──► Consumer 1 ──► console
                              │         │ (em caso de erro)
                              │         └──────► review.dlq
                              │
                              └──► support.queue ──► Consumer 2 ──► console
                                         │ (em caso de erro)
                                         └──────► support.dlq
```

**Routing keys:** `review.product` · `support.request`

---

## 🛡 Recursos de Confiabilidade

- **ACK manual** — as mensagens são confirmadas apenas após o processamento bem-sucedido, evitando perda acidental de mensagens.
- **Dead Letter Queue (DLQ)** — mensagens que geram erro de processamento são redirecionadas para uma DLQ em vez de serem descartadas, permitindo inspeção e reprocessamento posterior.
- **Consumidores mais devagar** (tarefa 9) — a latência de processamento é intencionalmente maior que a taxa de produção, para que o acúmulo na fila seja visível na interface de gerenciamento do RabbitMQ. Cada consumidor opera mais devagar que a velocidade do gerador por meio de um atraso de processamento configurável (padrão: 15 ms para o Consumer 1, 50 ms para o Consumer 2), ambos inferiores à taxa de chegada de ~5 msg/s.
- **Pontuação de confiança** — cada resultado de classificação inclui a probabilidade prevista pelo modelo para a classe escolhida, de modo que o sistema mostra o quanto a IA embarcada está confiante em cada decisão.
- **Identidade única de mensagem** — cada mensagem contém ID, timestamp, tipo e conteúdo para rastreabilidade completa.

---

## 📁 Estrutura do Projeto

```
.
├── generator/                  # Serviço gerador de mensagens
│   ├── Dockerfile
│   └── src/
├── consumer-1/                 # Consumidor de análise de sentimentos
│   ├── Dockerfile
│   └── src/
├── consumer-2/                 # Consumidor de classificação de solicitações de suporte
│   └── Dockerfile
├── rabbitmq/                   # Configuração do broker (exchange, filas, DLQ)
├── docker-compose.yml          # Orquestração de todos os containers
├── docs/                       # Capturas de tela e evidências de execução
└── README.md
```

---

## 📽 Como demonstrar (evidências a coletar)

Os cinco pontos abaixo são os itens que o professor pede para demonstrar. Para cada um, execute o sistema (`docker compose up --build`) e depois registre a evidência em `docs/` (capturas de tela, logs do console, saída do `rabbitmqctl`).

| # | O que demonstrar | Como comprovar | Evidência a salvar |
|---|------------------|----------------|---------------------|
| 1 | **Geração contínua de mensagens** | O gerador publica ≥5 mensagens/seg, alternando entre `review.product` e `support.request`. Cada mensagem possui `id`, `timestamp`, `type`, `content`. | `docker compose logs -f generator` mostrando ≥5 msg/s em uma janela de 10 s (relatório de throughput); cerca de 50 linhas publicadas. |
| 2 | **Roteamento correto pelo Topic Exchange** | `messages.topic` roteia por routing key: `review.product` → `review.queue` → Consumer 1; `support.request` → `support.queue` → Consumer 2. Sem tráfego cruzado. | `docker compose exec rabbitmq rabbitmqctl list_bindings --silent` (exchange → filas com as duas routing keys); uma linha de log por mensagem no consumidor correto, correspondendo ao `type`. |
| 3 | **Classificação pelas duas IAs** | Consumer 1 rotula avaliações como `positivo`/`negativo`/`neutro`; Consumer 2 rotula solicitações de suporte como `problema_de_acesso`/`problema_de_desempenho`/`problema_de_conexão`; cada resultado inclui uma pontuação de confiança. | Dois logs de console (consumer-1, consumer-2) com `sentiment=... confidence=...` e `classification=... confidence=...` em cada mensagem. |
| 4 | **Crescimento da fila na interface do RabbitMQ** | Os consumidores são intencionalmente mais devagar que a taxa de chegada (15 ms/msg Consumer 1, 50 ms/msg Consumer 2), fazendo a profundidade da fila crescer na interface de gerenciamento. | Capturas de tela de `http://localhost:15672` → Queues/Overview enquanto a profundidade da fila aumenta; relatórios periódicos dos consumidores (`received N`, `consumption rate`, `average processing time`, `estimated queue depth`). |
| 5 | **Tratamento de erros pela DLQ** | Uma mensagem que falha no processamento é NACKed com `requeue=false`, então vai para `review.dlq` / `support.dlq` (via `messages.dlx`) em vez de ser descartada. | Log do consumidor mostrando `⚠ PROCESSING ERROR — nacking to DLQ ...`; `docker compose exec rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged` mostrando mensagens na DLQ e zero não confirmadas na fila normal. |

---

## 📄 Licença

Este projeto é licenciado sob os termos do arquivo [LICENSE](LICENSE).
