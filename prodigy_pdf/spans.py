import base64
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Tuple

import pypdfium2 as pdfium
from docling_core.types.doc.labels import DocItemLabel
from prodigy.components.preprocess import get_token
from prodigy.components.stream import Stream
from prodigy.core import Arg, recipe
from prodigy.errors import RecipeError
from prodigy.protocols import ControllerComponentsDict
from prodigy.recipes.ner import preprocess_stream as preprocess_ner_stream
from prodigy.types import PathInputType, StreamType, ViewId
from prodigy.util import ensure_path, log, set_hashes
from spacy.language import Language
from spacy.tokens import Doc, Span
from spacy_layout import spaCyLayout

HEADINGS = [DocItemLabel.SECTION_HEADER, DocItemLabel.PAGE_HEADER, DocItemLabel.TITLE]
SEPARATOR = "\n\n"

FONT_SIZE_TEXT = 14
FONT_SIZE_HEADING = 18
CSS = """
.prodigy-content {
    text-align: left;
}
"""
CSS_PREVIEW = """
.prodigy-page-content {
    display: grid;
    grid-template-columns: 1fr 50%;
}

.prodigy-page-content > div:nth-child(1) {
    border-right: 1px solid #ccc;
}

.prodigy-page-content > div:nth-child(2) {
    position: relative;
}

.prodigy-page-content > div:nth-child(2) > div:first-child {
    position: sticky;
    top: 0;
}
"""
CSS_PREVIEW_FOCUS = """
.prodigy-container {
    display: grid;
    grid-template-columns: 0 1fr 50%;
}

.prodigy-container > div:nth-child(2) {
    border-right: 1px solid #ccc;
}

.prodigy-container > div:nth-child(3) {
    position: relative;
}

.prodigy-container > div:nth-child(3) > div:first-child {
    position: sticky;
    top: 0;
}
"""


def get_layout_tokens(
    doc: Span, headings: List[int] = [], disabled: List[int] = []
) -> List[dict]:
    result = []
    for i, token in enumerate(doc):
        token_dict = get_token(token, i)
        if token.text == SEPARATOR or token.i in disabled:
            token_dict["disabled"] = True
            token_dict["style"] = {"display": "none"}
        if token.i in headings:
            token_dict["style"] = {"fontWeight": "bold", "fontSize": FONT_SIZE_HEADING}
        result.append(token_dict)
    return result


def get_special_tokens(
    doc: Doc, disable: List[str] = []
) -> Tuple[List[int], List[int]]:
    headings = []
    disabled = []
    for span in doc.spans["layout"]:
        idxs = range(span.start, span.end)
        if span.label_ in HEADINGS:
            headings.extend(idxs)
        if span.label_ in disable:
            disabled.extend(idxs)
    return headings, disabled


def pdf_to_images(path: Path) -> List[str]:
    images = []
    pdf = pdfium.PdfDocument(path)
    for page_number in range(len(pdf)):
        page = pdf.get_page(page_number)
        pil_image = page.render().to_pil()
        with BytesIO() as buffered:
            pil_image.save(buffered, format="JPEG")
            img_str = base64.b64encode(buffered.getvalue())
        images.append(f"data:image/png;base64,{img_str.decode('utf-8')}")
    return images


class LayoutStream:
    def __init__(
        self,
        f: PathInputType,
        nlp: Language,
        file_ext: List[str] = ["pdf", "docx"],
        view_id: ViewId = "spans_manual",
        disable: List[str] = [],
        split_pages: bool = False,
        hide_preview: bool = False,
        focus: List[str] = [],
    ) -> None:
        dir_path = ensure_path(f)
        if not dir_path.exists() or not dir_path.is_dir():
            raise RecipeError(f"Can't load from directory {f}", dir_path.resolve())
        self.paths = [
            path
            for path in sorted(dir_path.iterdir())
            if path.is_file()
            and not path.name.startswith(".")
            and (path.suffix.lower()[1:] in file_ext)
        ]
        self.view_id = view_id
        self.disable = disable
        self.split_pages = split_pages
        self.hide_preview = hide_preview
        self.focus = focus
        self.css = CSS
        if not hide_preview:
            self.css += CSS_PREVIEW_FOCUS if self.focus else CSS_PREVIEW
        self.nlp = nlp
        self.layout = spaCyLayout(nlp, separator=SEPARATOR)
        log("RECIPE: Initialized spacy-layout")

    def get_stream(self) -> StreamType:
        if self.focus:
            yield from self.get_focus_stream()
        else:
            yield from self.get_full_stream()

    def get_full_stream(self) -> StreamType:
        blocks = [{"view_id": self.view_id}]
        if not self.hide_preview:
            blocks.append({"view_id": "image", "spans": []})
        for file_path in self.paths:
            doc = self.layout(file_path)
            images = pdf_to_images(file_path)
            pages = []
            for i, (page_layout, page_spans) in enumerate(
                doc._.get(self.layout.attrs.doc_pages)
            ):
                headings, disabled = get_special_tokens(doc, disable=self.disable)
                page = {
                    "text": SEPARATOR.join(span.text for span in page_spans),
                    "image": images[i],
                    "tokens": get_layout_tokens(
                        doc[page_spans[0].start : page_spans[-1].end],
                        headings=headings,
                        disabled=disabled,
                    ),
                    "width": page_layout.width,
                    "height": page_layout.height,
                    "view_id": "blocks",
                    "config": {"blocks": blocks},
                }
                pages.append(page)
                if self.split_pages:
                    yield set_hashes(page)
            if not self.split_pages:
                yield set_hashes({"pages": pages})

    def get_focus_stream(self) -> StreamType:
        for file_path in self.paths:
            doc = self.layout(file_path)
            images = pdf_to_images(file_path)
            for i, (page_layout, page_spans) in enumerate(
                doc._.get(self.layout.attrs.doc_pages)
            ):
                _, disabled = get_special_tokens(doc, disable=self.disable)
                for span in page_spans:
                    if span.label_ not in self.focus:
                        continue
                    span_layout = span._.get(self.layout.attrs.span_layout)
                    image_spans = []
                    if span_layout:
                        image_spans.append(
                            {
                                "x": span_layout.x,
                                "y": span_layout.y,
                                "width": span_layout.width,
                                "height": span_layout.height,
                                "color": "magenta",
                                "id": span.id,
                            }
                        )
                    blocks = [{"view_id": self.view_id}]
                    if not self.hide_preview:
                        blocks.append({"view_id": "image", "spans": image_spans})
                    eg = {
                        "text": span.text,
                        "image": images[i],
                        "tokens": get_layout_tokens(span, disabled=disabled),
                        "width": page_layout.width,
                        "height": page_layout.height,
                        "view_id": "blocks",
                        "config": {"blocks": blocks},
                        "text_span": {
                            "token_start": span.start,
                            "token_end": span.end - 1,
                            "start": span.start_char,
                            "end": span.end_char,
                            "text": span.text,
                            "label": span.label_,
                        },
                    }
                    yield set_hashes(eg)


@recipe(
    "pdf.spans.manual",
    # fmt: off
    dataset=Arg(help="Dataset to save annotations to"),
    nlp=Arg(help="Loadable spaCy pipeline"),
    source=Arg(help="Path to directory to load from"),
    labels=Arg("--label", "-l", help="Comma-separated label(s) to annotate or text file with one label per line"),
    add_ents=Arg("--add-ents", "-E", help="Add named enitites for the given labels via the spaCy model"),
    focus=Arg("--focus", "-FX", help="Focus mode: annotate selected sections of a given type, e.g. 'text'"),
    disable=Arg("--disable", "-d", help="Labels of layout spans to disable, e.g. 'footnote'"),
    split_pages=Arg("--split-pages", "-S", help="View pages as separate tasks"),
    hide_preview=Arg("--hide-preview", "-P", help="Hide side-by-side preview of layout"),
    # fmt: on
)
def pdf_spans_manual(
    dataset: str,
    nlp: Language,
    source: str,
    labels: Optional[List[str]] = None,
    add_ents: bool = False,
    focus: Optional[List[str]] = None,
    disable: Optional[List[str]] = None,
    hide_preview: bool = False,
    split_pages: bool = False,
) -> ControllerComponentsDict:
    """
    Apply span annotations to text-based document contents extracted with
    spacy-layout and Docling. For efficiency, the recipe can run with
    --focus text to walk through individual text blocks, which are highlighted
    in a visual preview of the document page.
    """
    log("RECIPE: Starting recipe pdf.spans.manual", locals())
    view_id = "spans_manual"
    layout_stream = LayoutStream(
        source,
        nlp=nlp,
        file_ext=["pdf", "docx"],
        view_id=view_id,
        disable=disable or [],
        split_pages=split_pages,
        hide_preview=hide_preview,
        focus=focus or [],
    )
    stream = Stream.from_iterable(layout_stream.get_stream())
    if add_ents:
        stream.apply(preprocess_ner_stream, nlp, labels=labels, unsegmented=True)

    return {
        "dataset": dataset,
        "stream": stream,
        "view_id": "pages" if not split_pages and not focus else "blocks",
        "config": {
            "labels": labels,
            "global_css": layout_stream.css,
            "shade_bounding_boxes": True,
            "custom_theme": {
                "cardMaxWidth": "95%",
                "smallText": FONT_SIZE_TEXT,
                "tokenHeight": 25,
            },
        },
    }
