"""UI contracts for clear U.S. Lacey legal acceptance and billing guidance."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def _template(name: str) -> str:
    return (TEMPLATES / name).read_text(encoding="utf-8")


def test_signup_hides_internal_legal_versions_from_customer_copy() -> None:
    signup = _template("signup.html")

    assert "commercial.terms_version" not in signup
    assert "commercial.privacy_version" not in signup
    assert "commercial.beta_terms_version" not in signup
    assert "I accept the <a" in signup
    assert "Terms of Service" in signup
    assert "Privacy Policy" in signup
    assert "Early Access Terms" in signup
    assert "Private Beta Terms" not in signup


def test_portal_navigation_uses_early_access_language() -> None:
    base = _template("base.html")

    assert "Early Access Terms" in base
    assert ">Private Beta Terms<" not in base


def test_billing_explains_manual_matching_code_and_future_secure_checkout() -> None:
    billing = _template("billing.html")

    assert "How to complete the payment" in billing
    assert "Payment matching code" in billing
    assert "not</strong> a bank account, payment link or destination address" in billing
    assert "transfer memo, reference, concept or description field" in billing
    assert "billing.payment_reference" in billing
    assert 'billing.payment_provider == "LEMON_SQUEEZY"' in billing
    assert "/billing/lemon-checkout" in billing
    assert "Pay " in billing and " securely" in billing
