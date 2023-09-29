from pathlib import Path
from prodigy_pdf import generate_pdf_pages


def test_smoke_internal():
    # We know this one PDF has six pages.
    paths = Path("tests/pdfs").glob("*.pdf")
    pages = list(generate_pdf_pages(paths))
    assert len(pages) == 6
    for page in pages:
        assert "data" in page['image']
