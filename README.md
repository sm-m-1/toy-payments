# toy-payments

A transaction processing engine with publisher-consumer architecture and DLQ support.

## Usage

```bash
python src/main.py input.csv > output.csv
```

## Input Format

```csv
type, client, tx, amount
deposit, 1, 1, 100.0
withdrawal, 1, 2, 50.0
dispute, 1, 1,
```

## Output Format

```csv
client,available,held,total,locked
1,50,0,50,false
```

## Running Tests

**Option 1: Virtual environment**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pytest
pytest tests/ -v
```

**Option 2: Global install with pipx**
```bash
pipx install pytest
pytest tests/ -v
```

## Correctness

- **Unit tests** for `TransactionProcessor` cover each transaction type and edge cases (wrong client, insufficient funds, frozen account)
- **Integration tests** for `PaymentsEngine` verify end-to-end processing including DLQ retry for out-of-order transactions
- **Edge case tests** cover: duplicate disputes, disputes on withdrawals (rejected), chargeback after resolve (rejected), re-dispute after resolve, partial withdrawal then dispute
- **Large scale tests** with 1000 accounts and 6000 transactions verify correctness under concurrent processing with 10 consumer threads
- All tests use inline CSV data with precise expected values for deterministic verification

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
