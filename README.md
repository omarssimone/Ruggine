# Ruggine

Sistema di chat in tempo reale basato su WebSocket, sviluppato in Rust.

## Architettura

Il progetto è organizzato come workspace Cargo con tre crate:

```
ruggine/
├── ruggine_server/    # Server WebSocket con persistenza SQLite
├── ruggine_client/    # Client TUI con ratatui
└── ruggine_common/    # Tipi condivisi (messaggi)
```

### Server (`ruggine_server`)

- **WebSocket Server**: Gestisce connessioni multiple tramite `tokio-tungstenite`
- **Database**: SQLite con `rusqlite` per persistenza utenti, gruppi e inviti
- **Concorrenza**: Utilizza `tokio` per async I/O e `Arc<Mutex<>>` per stato condiviso
- **Logging**: `tracing` per log strutturati + file `cpu.log` per monitoraggio risorse

### Client (`ruggine_client`)

- **TUI**: Interfaccia terminale con `ratatui` e `crossterm`
- **Async**: Comunicazione non bloccante con il server

### Common (`ruggine_common`)

- Definizione di `ClientMessage` e `ServerMessage` per la comunicazione

## Ammissione Utenti (User Persistence)

Il sistema implementa una **persistenza automatica senza autenticazione**:

1. Al primo avvio, l'utente inserisce uno username
2. Se lo username **non esiste** → viene creato nel database
3. Se lo username **esiste già** → login automatico ("Bentornato!")

**Nessuna password richiesta**. Gli utenti sono persistiti nella tabella:

```sql
users (id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL)
```

## Sistema di Inviti ai Gruppi (Invite-Only Access)

I gruppi sono **accessibili solo su invito**. Le regole sono:

### Regole di Accesso

- ✅ Solo i **membri** di un gruppo possono invitare altri utenti
- ❌ Gli utenti **non possono unirsi** a un gruppo senza invito
- ✅ Gli inviti sono **user-specific** (pendenti per singolo utente)
- ✅ Accettare un invito lo **rimuove** da pending e **aggiunge** l'utente ai membri
- ❌ Violazioni: restituiscono errore e vengono loggate (mai panic)

### Comandi Disponibili

| Comando                   | Descrizione                                    |
|---------------------------|------------------------------------------------|
| `/create <nome>`          | Crea un nuovo gruppo (creatore = primo membro) |
| `/invite <user> <gruppo>` | Invita un utente (per nome o ID gruppo)        |
| `/accept <nome_gruppo>`   | Accetta un invito pendente                     |
| `/reject <nome_gruppo>`   | Rifiuta un invito pendente                     |
| `/join <id>`              | Accetta invito tramite ID gruppo               |
| `/help`                   | Mostra tutti i comandi                         |

### Struttura Database

```sql
groups (id, name)
group_members (user_id, group_id)  -- Membership
group_invites (invited_user_id, group_id, inviter_user_id)  -- Inviti pendenti
```

## CPU Logging

Il server monitora automaticamente l'utilizzo della CPU ogni **120 secondi**.

### Implementazione

- Libreria: `sysinfo`
- Task background spawned all'avvio del server
- Output su file `cpu.log`

### Formato Log

```
[2025-01-06 14:30:00] PID: 12345 - CPU usage: 15.23%
```

Il logging è **completamente automatico** e non richiede intervento utente.

## Scelte di Performance

### Concorrenza

- **Tokio Runtime**: Async/await per I/O non bloccante
- **Unbounded Channels**: `mpsc::unbounded_channel` per comunicazione intra-task senza backpressure
- **Arc<Mutex<>>**: Per stato condiviso thread-safe (DB connection, active users)

### Database

- **SQLite**: Embedded, zero-config, performante per workload moderate
- **Transactions**: Per operazioni atomiche (es. accept invite)
- **Prepared Statements**: Per query ripetute

### Build Ottimizzato

```toml
[profile.release]
opt-level = "z"      # Ottimizza per dimensione
lto = true           # Link-Time Optimization
codegen-units = 1    # Migliore ottimizzazione
panic = "abort"      # Riduce dimensione binario
strip = true         # Rimuove simboli debug
```

### Robustezza

- **Zero `unwrap()`** nel codice: tutti gli errori sono gestiti gracefully
- Il server **non può crashare** per input client malevoli
- Il client gestisce disconnessioni e errori senza terminare

## Piattaforme Supportate

| Piattaforma | Stato        | Note                     |
|-------------|--------------|--------------------------|
| **Windows** | ✅ Supportato | Testato su Windows 10/11 |
| **Linux**   | ✅ Supportato | Testato su Ubuntu 22.04+ |
| **macOS**   | ✅ Supportato | Richiede Xcode CLI tools |

### Requisiti

- Rust 1.75+ (edition 2024)
- Terminale con supporto ANSI (per il client TUI)

## Build e Esecuzione

### Compilazione

```bash
# Debug build
cargo build

# Release build (ottimizzato)
cargo build --release
```

### Esecuzione

```bash
# Avvia il server (in un terminale)
cargo run -p ruggine_server

# Avvia il client (in un altro terminale)
cargo run -p ruggine_client
```

### Dimensione Eseguibili (Release)

```bash
# Verifica dimensione dopo build release
cargo build --release
dir target\release\*.exe  # Windows
ls -lh target/release/ruggine_*  # Linux/macOS
```

Dimensioni tipiche (con ottimizzazioni):

- `ruggine_server`: ~2-4 MB
- `ruggine_client`: ~2-3 MB

## File di Log

| File                        | Contenuto                        |
|-----------------------------|----------------------------------|
| `logs/ruggine_server.log.*` | Log applicativo (eventi, errori) |
| `cpu.log`                   | Monitoraggio CPU ogni 120s       |
| `ruggine.db`                | Database SQLite                  |

## Licenza

Progetto accademico - Tutti i diritti riservati.
