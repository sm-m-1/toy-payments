import sys
import os
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from payments_engine import PaymentsEngine


class TestPaymentsEngineLargeScale:
    def test_1000_accounts_6000_transactions(self, tmp_path):
        """Test with 1000 accounts and 6000 transactions."""
        num_clients = 1000
        rows = ["type, client, tx, amount"]
        tx_id = 1

        # Each client gets: 3 deposits (100, 200, 300) and 2 withdrawals (50, 100)
        # Expected per client: 100 + 200 + 300 - 50 - 100 = 450, plus 50 more below = 500

        for client_id in range(1, num_clients + 1):
            rows.append(f"deposit, {client_id}, {tx_id}, 100")
            tx_id += 1
            rows.append(f"deposit, {client_id}, {tx_id}, 200")
            tx_id += 1
            rows.append(f"deposit, {client_id}, {tx_id}, 300")
            tx_id += 1
            rows.append(f"withdrawal, {client_id}, {tx_id}, 50")
            tx_id += 1
            rows.append(f"withdrawal, {client_id}, {tx_id}, 100")
            tx_id += 1

        # Extra deposit for each client
        for client_id in range(1, num_clients + 1):
            rows.append(f"deposit, {client_id}, {tx_id}, 50")
            tx_id += 1

        # All clients get extra 50
        expected_balance = Decimal("500")  # 450 + 50

        csv_file = tmp_path / "large_test.csv"
        csv_file.write_text('\n'.join(rows))

        engine = PaymentsEngine(num_consumers=10)
        accounts = engine.process_file(str(csv_file))

        assert len(accounts) == num_clients

        for client_id in range(1, num_clients + 1):
            assert accounts[client_id].available == expected_balance, \
                f"Client {client_id}: expected {expected_balance}, got {accounts[client_id].available}"
            assert accounts[client_id].held == Decimal("0")
            assert accounts[client_id].locked is False

    def test_with_disputes_resolves_chargebacks(self, tmp_path):
        """Test with disputes, resolves, and chargebacks across 50 accounts."""
        rows = ["type, client, tx, amount"]

        # Client 1-10: Normal deposits only
        # Expected: 500 each (100 + 150 + 250)
        for client_id in range(1, 11):
            rows.append(f"deposit, {client_id}, {client_id * 100 + 1}, 100")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 2}, 150")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 3}, 250")

        # Client 11-20: Deposit -> Dispute -> Resolve
        # Expected: 500 each, held=0, locked=False
        for client_id in range(11, 21):
            rows.append(f"deposit, {client_id}, {client_id * 100 + 1}, 100")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 2}, 150")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 3}, 250")
        for client_id in range(11, 21):
            rows.append(f"dispute, {client_id}, {client_id * 100 + 1},")
        for client_id in range(11, 21):
            rows.append(f"resolve, {client_id}, {client_id * 100 + 1},")

        # Client 21-30: Deposit -> Dispute -> Chargeback
        # Expected: 400 each (500 - 100 chargebacked), locked=True
        for client_id in range(21, 31):
            rows.append(f"deposit, {client_id}, {client_id * 100 + 1}, 100")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 2}, 150")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 3}, 250")
        for client_id in range(21, 31):
            rows.append(f"dispute, {client_id}, {client_id * 100 + 1},")
        for client_id in range(21, 31):
            rows.append(f"chargeback, {client_id}, {client_id * 100 + 1},")

        # Client 31-40: Deposit -> Withdrawal -> Dispute (on deposit)
        # Expected: available=250 (400 - 150 held), held=150, total=400
        for client_id in range(31, 41):
            rows.append(f"deposit, {client_id}, {client_id * 100 + 1}, 150")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 2}, 250")
            rows.append(f"withdrawal, {client_id}, {client_id * 100 + 3}, 100")
        for client_id in range(31, 41):
            rows.append(f"dispute, {client_id}, {client_id * 100 + 1},")

        # Client 41-50: Multiple deposits, dispute middle one, resolve
        # Expected: 600 each, held=0
        for client_id in range(41, 51):
            rows.append(f"deposit, {client_id}, {client_id * 100 + 1}, 100")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 2}, 200")
            rows.append(f"deposit, {client_id}, {client_id * 100 + 3}, 300")
        for client_id in range(41, 51):
            rows.append(f"dispute, {client_id}, {client_id * 100 + 2},")
        for client_id in range(41, 51):
            rows.append(f"resolve, {client_id}, {client_id * 100 + 2},")

        csv_file = tmp_path / "disputes_test.csv"
        csv_file.write_text('\n'.join(rows))

        engine = PaymentsEngine(num_consumers=10)
        accounts = engine.process_file(str(csv_file))

        # Verify Client 1-10: Normal
        for client_id in range(1, 11):
            assert accounts[client_id].available == Decimal("500"), f"Client {client_id}"
            assert accounts[client_id].held == Decimal("0")
            assert accounts[client_id].locked is False

        # Verify Client 11-20: Disputed then resolved
        for client_id in range(11, 21):
            assert accounts[client_id].available == Decimal("500"), f"Client {client_id}"
            assert accounts[client_id].held == Decimal("0")
            assert accounts[client_id].locked is False

        # Verify Client 21-30: Chargebacked
        for client_id in range(21, 31):
            assert accounts[client_id].available == Decimal("400"), f"Client {client_id}"
            assert accounts[client_id].held == Decimal("0")
            assert accounts[client_id].total == Decimal("400")
            assert accounts[client_id].locked is True

        # Verify Client 31-40: Disputed (held)
        for client_id in range(31, 41):
            assert accounts[client_id].available == Decimal("150"), f"Client {client_id}"
            assert accounts[client_id].held == Decimal("150")
            assert accounts[client_id].total == Decimal("300")
            assert accounts[client_id].locked is False

        # Verify Client 41-50: Disputed then resolved
        for client_id in range(41, 51):
            assert accounts[client_id].available == Decimal("600"), f"Client {client_id}"
            assert accounts[client_id].held == Decimal("0")
            assert accounts[client_id].locked is False
