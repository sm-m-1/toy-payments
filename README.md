# toy-payments

A transaction processing engine with publisher-consumer architecture and DLQ support.

## Usage

```bash
python src/main.py input.csv > output.csv
```

### Example

```bash
$ python src/main.py tests/fixtures/basic.csv
client,available,held,total,locked
1,1.5,0,1.5,false
2,2,0,2,false
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

## Extensibility

The publisher-consumer architecture decouples the data source from processing logic. The queue, consumers, and processor remain unchanged regardless of input source.

**Current:** CSV file → Publisher → Queue → Consumers

**Alternative:** Webhook endpoint → Queue → Consumers

To add webhook support, only a new HTTP endpoint is needed:

```python
@app.route("/transaction", methods=["POST"])
def receive_transaction():
    tx = parse_request(request.json)
    queue.publish(tx)  # Same queue interface
    return {"status": "accepted"}
```

Core components (`InMemoryQueue`, `StateManager`, `TransactionProcessor`) require no changes.
