# Celer Engine

[![Go Reference](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Docker Compose](https://img.shields.io/badge/docker--compose-ready-2496ED?style=flat&logo=docker)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Status](https://img.shields.io/badge/status-in--development-yellow?style=flat)](#-status-do-projeto)

O **celer-engine** é um motor de processamento de eventos de alta performance e baixa latência desenvolvido em **Go**. Ele foi projetado para consumir telemetria de robôs e dispositivos distribuídos via **Apache Kafka**, aplicar validações estritas de schema via **Protocol Buffers (Protobuf)**, agregar eventos em janelas deslizantes de tempo em memória e emitir alertas/warnings em tempo real.

---

## 🏗️ Arquitetura e Modelo de Concorrência

O motor utiliza um pipeline de processamento concorrente baseado em **3 Goroutines / Threads dedicadas**, interconectadas por **Go Channels buffered**. Esse design isola o I/O de rede das tarefas intensivas em CPU/Memória e provê **backpressure nativo**, prevenindo *Out-Of-Memory (OOM)* sob alta carga.

```text
[ REDE / KAFKA (Tópico Input - Protobuf) ]
               │
               │ (Bytes brutos)
               ▼
┌─────────────────────────────────────────────────────────┐
│ THREAD 1: Consumer Loop (internal/kafka/consumer.go)    │
│ -> Lê bytes brutos do Kafka na velocidade da rede       │
│ -> Alimenta o Ingestion Channel (sem parse do payload)  │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ (Go Channel - Ingestion Buffer / Backpressure)
                         ▼
┌─────────────────────────────────────────────────────────┐
│ THREAD 2: Worker + Validator + Aggregator               │
│ -> Consome bytes brutos do Channel                      │
│ -> Valida e deserializa Protobuf em CPU                 │
│ -> Se Erro Sintático -> Envia JSON para a DLQ           │
│ -> Se Válido -> Alimenta Agregador em Memória           │
│    ├── Anomalia leve   -> Gera Warning (Protobuf)       │
│    └── Limite Atingido -> Gera Alerta (Protobuf)        │
└────────────────────────┬────────────────────────────────┘
                         │
                         │ (Go Channel - Egress Buffer)
                         ▼
┌─────────────────────────────────────────────────────────┐
│ THREAD 3: Producer (internal/kafka/producer.go)         │
│ -> Ouve o Egress Channel                                │
│ -> Roteia DLQ (JSON) para o Tópico "DLQ"                │
│ -> Roteia Warnings (Protobuf) para o Tópico "Warnings"   │
│ -> Roteia Alertas (Protobuf) para o Tópico "Alertas"     │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
[ REDE / KAFKA (Tópicos: DLQ / Warnings / Alertas) ]
```

---

## 🚦 Classificação dos Tópicos

| Tópico | Formato | Descrição / Gatilho |
| :--- | :--- | :--- |
| **`Input`** | Protobuf | Telemetria bruta emitida pelos robôs / gerador de carga. |
| **`DLQ`** | JSON | Eventos com falhas de schema, corrompidos ou rejeitados pelo `Validator`. |
| **`Warnings`** | Protobuf | Eventos sintaticamente válidos que apresentam desvios ou anomalias pontuais. |
| **`Alertas`** | Protobuf | Disparado quando os limites configurados na janela deslizante do `Aggregator` são atingidos. |

---

## 📁 Estrutura do Repositório

```text
celer-engine/
├── cmd/
│   ├── engine/       # Ponto de entrada do Celer Engine (main.go)
│   └── chaos-gen/    # Gerador de carga/caos para simulação de telemetria
├── internal/
│   ├── domain/       # Interfaces e modelos de dados do domínio
│   ├── kafka/        # Implementação de Consumer e Producer Kafka
│   ├── proto/        # Definições .proto e código Go gerado
│   ├── validator/    # Validação e deserialização de payloads
│   ├── aggregator/   # Janela deslizante e agregação em memória
│   └── worker/       # Orquestração do pipeline de processamento interno
└── docker-compose.yml # Ambiente completo isolado (Kafka KRaft, Engine, Chaos Gen)
```

---

## ⚙️ Como Executar o Projeto

### Pré-requisitos
* [Go 1.22+](https://go.dev/)
* [Docker](https://www.docker.com/) e Docker Compose

### Subindo o ambiente
Para subir toda a infraestrutura (Kafka em modo KRaft + Gerador de Caos + Celer Engine):

```bash
docker compose up --build
```

---

## 🚧 Status do Projeto

> ⚠️ **Nota:** Este projeto está em desenvolvimento ativo.

- [x] Arquitetura multi-thread desacoplada via Go Channels
- [x] Contratos e interfaces de domínio desacoplados
- [x] Schemas Protocol Buffers (`event.proto`)
- [x] Gerador de Carga (Chaos Generator)
- [ ] Implementação do Consumidor e Produtor Kafka
- [ ] Conclusão do ciclo do Worker Loop e Agregador
- [ ] Testes de carga e integração E2E