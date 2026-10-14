# Demonstração — Trabalho 05: Comunicação via RabbitMQ

Roteiro de evidências para o relatório do trabalho. Para cada um dos itens pedidos, indicam-se **o que observar**, **o comando / interface para comprovar** e **como registrar a evidência** (print, log, `curl`, `rabbitmqctl`, etc.).

> Sistemas abaixo são executados via Docker Compose. A interface de gestão está em `http://localhost:15672` (login `guest`/`guest`). O `docs/` é o local indicado para guardar prints e logs do laboratório.

---

## 1. Geração contínua de mensagens (≥ 5 mensagens/s)

**O que observar**

- O **Message Generator** publica mensagens de forma contínua e constante, alternando entre review (`review.product`) e support request (`support.request`).
- Cada mensagem carrega `id`, `timestamp`, `type` e `content` (JSON).
- O generation rate está configurado por `RABBITMQ_MESSAGE_RATE` (padrão 5 msg/s, um a cada 200 ms) e o space/time é dinâmico para não acumular atraso.

**Como comprovar**

- Levantar o container `generator` e acompanhar os logs:

  ```bash
  docker compose logs -f generator
  ```

  Cada linha de log contém `[review.product]` ou `[support.request]`, o `id` da mensagem e o conteúdo.

- Ver o *throughput report* que o generator imprime a cada 10 s, ex.:

  ```
  → rate: 5.0 msg/s (50 messages total)
  ```

- No terminal, medir um rate independente com o console do consumer ou com `docker compose logs --since`:

  ```bash
  docker compose logs --since 10s generator | grep '\[.*\] published' | wc -l
  ```

**Evidência a registrar**

- Print do terminal dos logs do `generator` mostrando pelo menos 5 msg/s (ex.: 10 s de execução → 50+ linhas).
- O *throughput report* com o valor em msg/s.

---

## 2. Encaminhamento correto das mensagens pelo Topic Exchange

**O que observar**

- A fila dos dois consumidores recebe exclusivamente o tipo correto:

  - `messages.topic` — `review.product` → `review.queue` → **Consumer 1** (sentiment)
  - `messages.topic` — `support.request` → `support.queue` → **Consumer 2** (support)
- O consumo é com **manual ACK** e contagem dos tipos: em cada janela de 10 s, os dois consumidores devem receber a mesma quantidade de mensagens, e o tipo de mensagem em cada linha de log do consumer deve corresponder à fila de onde veio.

**Como comprovar**

- As bindings declaradas no `rabbitmq/definitions.json` e carregadas pelo RabbitMQ no boot:

  ```bash
  docker compose exec rabbitmq rabbitmqctl list_bindings --silent
  ```

  Devem aparecer, para o vhost `/`, o exchange `messages.topic` ligado às duas filas com as routing keys `review.product` e `support.request`.

- No console do consumer, acompanhar a linha de log por mensagem:

  ```
  [review.queue] received id=... type=product_review sentiment=positive confidence=0.xxx content="..."
  [support.queue] received id=... type=support_request classification=access_problem confidence=0.xxx content="..."
  ```

  Note que os `type` vêm do JSON (`product_review` / `support_request`), e o consumo só acontece na fila correta.

**Evidência a registrar**

- Output de `rabbitmqctl list_bindings` (ou o painel de *Bindings* no RabbitMQ Management).
- Prints dos logs dos dois consumidores mostrando mensagens de cada tipo chegando na fila esperada.

---

## 3. Classificação realizada pelas duas IAs

**O que observar**

- **Consumer 1** classifica review como `positive`, `negative` ou `neutral` (modelo Smile, regressão logística multinomial, treinado no `sentiment-corpus.txt`).
- **Consumer 2** classifica support request como `access_problem`, `performance_problem` ou `connection_problem` (modelo Smile treinado no `support-corpus.txt`).
- Cada classificação vem com **confidence** (probabilidade da classe prevista).
- O treinamento é feito uma única vez no startup; não há chamada a serviço externo de IA.

**Como comprovar**

- Logs dos consumidores:

  ```
  [review.queue] received id=... type=product_review sentiment=positive confidence=0.xxx content="..."
  [support.queue] received id=... type=support_request classification=access_problem confidence=0.xxx content="..."
  ```

  Execute com:

  ```bash
  docker compose logs -f consumer-1
  docker compose logs -f consumer-2
  ```

- Verificar os *classes* e *confidence* nos prints.
- Para uma verificação da rompimento da crença, executar um consumer fora do Docker apontando para o RabbitMQ local e enviar manualmente uma mensagem com o tipo correto (via `docker compose exec rabbitmq rabbitmq-shell` + `amqp-declare-queue` etc.), observando o label e a confiança.

**Evidência a registrar**

- Prints dos logs dos dois consumidores (tipos + labels + confidence).
- (Opcional) exempo de classificação do modelo com um texto fora do corpus.

---

## 4. Crescimento das filas na interface do RabbitMQ

**O que observar**

- Os consumidores processam com atraso deliberado (`RABBITMQ_PROCESSING_DELAY_MS`: 15 ms para Consumer 1, 50 ms para Consumer 2), mais lento que o rate de produção (~5 msg/s), de modo que a fila cresce visivelmente.
- O *queue depth* (número de mensagens não-ACK) deve subir no tempo em `http://localhost:15672` → *Queues* → *overview* (ou clicando em cada fila).
- Os consumidores imprimem um relatório periódico com o número de mensagens recebidas, taxa de consumo, tempo médio de processamento e profundidade estimada da fila.

**Como comprovar**

- Abra a interface: `http://localhost:15672` → `Queues` → `overview`.
  - `review.queue` (Consumer 1) e `support.queue` (Consumer 2) devem variar de tamanho com o tempo.
- Confira também o *Overview* da broker: `Message rates` (publishes vs acks) e `Queue depth`.

**Evidência a registrar**

- Print da tela *Queues* / *overview* do RabbitMQ Management com o crescimento da fila ao longo do tempo ( screenshots sequenciais, se possível)
- Os relatórios periódicos dos consumidores (output de `docker compose logs -f consumer-1` / `-f consumer-2`), que contêm `received N messages`, `consumption rate`, `average processing time` e `estimated queue depth`.

---

## 5. Tratamento de mensagem com erro por meio da DLQ

**O que observar**

- Se a processagem de uma mensagem falhar (ex.: falha ao deserializar o JSON, erro de classificação), o consumer **NACKa a mensagem sem requeue** (`basicNack(deliveryTag, multiple = false, requeue = false)`).
- A mensagem não é descartada: é empurrada para a fila **DLQ** vinculada via `x-dead-letter-exchange` = `messages.dlx` e `x-dead-letter-routing-key` = `review.dlq` / `support.dlq`.
- A fila DLQ fica vazia (ou com poucas mensagens) enquanto o sistema está saudável, e só enche quando há erros de processamento.

**Como comprovar**

- Gerar uma falha controlada: enviar manualmente uma mensagem malformada para `review.queue` (ex.: texto que não é JSON válido, tipo `"not a json {"`), e observar:

  ```bash
  docker compose exec rabbitmq rabbitmq-shell
  ```

  dentro desse shell, declarar uma fila sem churn e publicar um body inválido com a routing key `review.product`, ou usar `amqp-publish`.
- O consumer deve imprimir:

  ```
  ⚠ PROCESSING ERROR — nacking to DLQ 'review.dlq': ...
  ```

  e o payload deve aparecer na fila `review.dlq` (e não voltar para `review.queue`).

- Verificar a quantidade de mensagens em cada fila:

  ```bash
  docker compose exec rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged
  ```

  O resultado deve mostrar a fila DLQ com mensagens, e `review.queue`/`support.queue` com mensagens não-ACK apenas se houver falha; com mensagens sãs, `messages_unacknowledged` = 0.

**Evidência a registrar**

- Print do log do consumer do erro e do `basicNack`.
- Output de `rabbitmqctl list_queues` ou do painel *Queues* mostrando a DLQ com o payload excedente e a fila normal com zero não-ACK.
- Screenshot da DLQ no RabbitMQ Management, se disponível.

---

## Resumo rápido de comandos

```bash
# Subir o sistema
docker compose up --build

# Logs do generator / consumidores
docker compose logs -f generator
docker compose logs -f consumer-1
docker compose logs -f consumer-2

# Bindings do exchange
docker compose exec rabbitmq rabbitmqctl list_bindings --silent

# Profundidade das filas
docker compose exec rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged

# Relatório de throttle (cada 10 s)
# --> prints dos relatórios dos consumidores
```

---

*Cabe ao aluno guardar a evidência em `docs/` (prints, logs, `rabbitmqctl`).
