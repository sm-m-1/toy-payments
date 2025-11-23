# toy-payments

A transaction processing engine with publisher-consumer architecture and DLQ support.

## Setup

**Option 1: Virtual environment**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pytest
```

**Option 2: Global install with pipx**
```bash
pipx install pytest
```

## Usage

```
Usage: python main.py <input.csv>
```

```bash
# Print to terminal
$ python src/main.py tests/fixtures/basic.csv
client,available,held,total,locked
1,1.5,0,1.5,false
2,2,0,2,false

# Write to file
$ python src/main.py tests/fixtures/basic.csv > output.csv
```

## Input/Output Format

**Input:**
```csv
type, client, tx, amount
deposit, 1, 1, 100.0
withdrawal, 1, 2, 50.0
dispute, 1, 1,
```

**Output:**
```csv
client,available,held,total,locked
1,50,0,50,false
```

## Testing

```bash
pytest tests/ -v
```

### Correctness

- **Unit tests** for `TransactionProcessor` cover each transaction type and edge cases (wrong client, insufficient funds, frozen account)
- **Integration tests** for `PaymentsEngine` verify end-to-end processing including DLQ retry for out-of-order transactions
- **Edge case tests** cover: duplicate disputes, disputes on withdrawals (rejected), chargeback after resolve (rejected), re-dispute after resolve, partial withdrawal then dispute
- **Large scale tests** with 1000 accounts and 6000 transactions verify correctness under concurrent processing with 10 consumer threads
- All tests use inline CSV data with precise expected values for deterministic verification

## Architecture

```
┌────────────────────────────────────────────────┐
│               PaymentsEngine                   │
└────────────────────────────────────────────────┘
                      │
    ┌─────────────────┼─────────────────┐
    ▼                 ▼                 ▼
┌──────────┐    ┌──────────┐    ┌────────────┐
│ Publisher│    │  Queue   │    │ Consumers  │
│(1 thread)│───▶│  ┌────┐  │◀───│(N threads) │
└──────────┘    │  │DLQ │  │    └────────────┘
    │           │  └────┘  │            │
    │           └──────────┘            │
    │                ▲                  ▼
┌────────┐           │         ┌───────────────┐
│CSV File│      retriable      │  Processor    │
└────────┘      failures       │ ┌───────────┐ │
                               │ │  State    │ │
                               │ │ (accounts,│ │
                               │ │  locks)   │ │
                               │ └───────────┘ │
                               └───────────────┘

Flow:
1. Publisher reads CSV, pushes to queue
2. Consumers pull and process transactions
3. Retriable failures go to DLQ
4. DLQ retried after main processing
```

## Extensibility

The publisher-consumer architecture decouples the data source from processing logic. The queue, consumers, and processor remain unchanged regardless of input source.

**Current:** CSV file -> Publisher -> Queue -> Consumers

**Alternative:** Webhook endpoint -> Queue -> Consumers

To add webhook support, only a new HTTP endpoint is needed:

```python
@app.route("/transaction", methods=["POST"])
def receive_transaction():
    tx = parse_request(request.json)
    queue.publish(tx)  # Same queue interface
    return {"status": "accepted"}
```

Core components (`InMemoryQueue`, `StateManager`, `TransactionProcessor`) require no changes.

## Development Notes

Built with Claude Code assistance. Claude generated the initial implementation for processing transactions from CSV and outputting account states to CSV. I then enhanced it with a publisher-consumer architecture and Dead Letter Queue (DLQ) for handling out-of-order transactions. I did multiple iterations of cleanup and refinements.
