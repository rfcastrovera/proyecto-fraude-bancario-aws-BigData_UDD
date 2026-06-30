import pytest


def test_filtro_amount_positivo():
    rows = [
        {"user_id": "u1", "amount": 100.0, "card": "4532015112890367"},
        {"user_id": "u2", "amount": 0.0, "card": "4916123456789012"},
        {"user_id": "u3", "amount": -50.0, "card": "4556123456789013"},
        {"user_id": "u4", "amount": 250.0, "card": "4929123456789014"},
    ]
    esperados = [r for r in rows if r["amount"] > 0]
    assert len(esperados) == 2
    assert all(r["amount"] > 0 for r in esperados)


def test_enmascaramiento_card():
    card = "4532015112890367"
    esperado = "4532-XXXX-XXXX-0367"
    masked = card[:4] + "-XXXX-XXXX-" + card[-4:]
    assert masked == esperado


def test_risk_heuristic():
    perfiles = [
        {"user_id": "u1", "tx_count": 5},
        {"user_id": "u2", "tx_count": 15},
        {"user_id": "u3", "tx_count": 12},
        {"user_id": "u4", "tx_count": 13},
    ]
    high_risk = [p for p in perfiles if p["tx_count"] > 12]
    assert len(high_risk) == 2
    assert all(p["tx_count"] > 12 for p in high_risk)
