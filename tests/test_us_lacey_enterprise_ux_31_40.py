from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"


def test_enterprise_ux_31_40_contract() -> None:
    intake = (TEMPLATES / "new_operation.html").read_text(encoding="utf-8")
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    claim = (TEMPLATES / "evaluation_claim.html").read_text(encoding="utf-8")

    # 31-33: one direct multi-file dropzone with visible format/limit guidance.
    assert 'data-dropzone' in intake
    assert 'data-file-dropzone-input' in intake
    assert 'multiple' in intake
    assert "Drop shipment documents here" in intake
    assert "Select documents" in intake
    assert "PDF · CSV · XLS · XLSX" in intake
    assert "up to 10 documents per evaluation shipment" in intake

    # Fix the apparent duplicate action: selecting files and committing the shipment are distinct.
    assert "Choose files" not in intake
    assert 'data-empty-label="Create shipment"' in intake
    assert 'data-files-label="Create shipment & process documents"' in intake

    # 34-35: explicit workflow stages without decorative arrow chains.
    for label in ("Upload", "Extract", "Review", "Package"):
        assert f"'{label}'" in intake
    assert "→" not in intake

    # 36-39: denser navigation, environment identity, temporary-workspace semantics and expiry.
    assert "lt-nav-section mb-3" in base
    assert "Temporary workspace" in base
    assert "fa-hourglass-half" in base
    assert ">Evaluation</span>" in base
    assert ">Production</span>" in base
    assert "7 days after your last activity" in base
    assert "evaluation_expires_at.strftime" in base

    # 40: signup/claim is framed as preserving the workspace, not starting over.
    assert "Keep this workspace" in base
    assert "Keep this workspace" in claim
    assert "Keep this workspace and unlock 4 more shipments" in claim
