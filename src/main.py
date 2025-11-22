import sys
import logging

from engine import PaymentsEngine

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s: %(message)s",
    stream=sys.stderr,
)


def format_decimal(value) -> str:
    """Format decimal with up to 4 decimal places, removing trailing zeros."""
    normalized = value.normalize()
    return f"{normalized:f}"


def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <input.csv>", file=sys.stderr)
        sys.exit(1)

    filepath = sys.argv[1]
    engine = PaymentsEngine()
    accounts = engine.process_file(filepath)

    print("client,available,held,total,locked")
    for client_id in sorted(accounts.keys()):
        account = accounts[client_id]
        print(
            f"{client_id},"
            f"{format_decimal(account.available)},"
            f"{format_decimal(account.held)},"
            f"{format_decimal(account.total)},"
            f"{str(account.locked).lower()}"
        )


if __name__ == "__main__":
    main()
