from pathlib import Path
from prodigy_pdf import generate_pdf_pages, fold_ocr_dashes, pdf_image_manual


def test_generate_pdf_pages():
    # We know this one PDF has six pages.
    paths = Path("tests/pdfs").glob("*.pdf")
    pages = list(generate_pdf_pages(paths))
    assert len(pages) == 6
    for page in pages:
        assert "data" in page['image']


def test_fold_dashes():
    going_in = """
        Real-Time Strategy (RTS) games have become an increas-
        ingly popular test-bed for modern artificial intelligence tech-
        niques. With this rise in popularity has come the creation of
        several annual competitions, in which AI agents (bots) play
        the full game of StarCraft: Broodwar by Blizzard Entertain-
        ment. The three major annual StarCraft AI Competitions are
        the Student StarCraft AI Tournament (SSCAIT), the Com-
        putational Intelligence in Games (CIG) competition, and the
        Artificial Intelligence and Interactive Digital Entertainment
        (AIIDE) competition. In this paper we will give an overview
        of the current state of these competitions, and the bots that
        compete in them.
        """

    expected = "Real-Time Strategy (RTS) games have become an increasingly popular test-bed for modern artificial intelligence techniques. With this rise in popularity has come the creation of several annual competitions, in which AI agents (bots) play the full game of StarCraft: Broodwar by Blizzard Entertainment. The three major annual StarCraft AI Competitions are the Student StarCraft AI Tournament (SSCAIT), the Computational Intelligence in Games (CIG) competition, and the Artificial Intelligence and Interactive Digital Entertainment (AIIDE) competition. In this paper we will give an overview of the current state of these competitions, and the bots that compete in them."
    assert fold_ocr_dashes(going_in) == expected


def test_pdf_image_manual():
    components = pdf_image_manual("xxx", "tests/pdfs", "foo,bar")
    assert isinstance(next(components["stream"]), dict)
