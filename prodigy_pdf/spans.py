import base64
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pypdfium2 as pdfium
import srsly
from docling_core.types.doc.labels import DocItemLabel
from prodigy.components.db import connect
from prodigy.components.preprocess import add_answer, resolve_labels
from prodigy.components.stream import Stream, _source_is_dataset, get_stream
from prodigy.core import Arg, recipe
from prodigy.errors import RecipeError
from prodigy.protocols import ControllerComponentsDict
from prodigy.recipes.ner import preprocess_stream as preprocess_ner_stream
from prodigy.types import PathInputType, StreamType, ViewId
from prodigy.util import ensure_path, log, msg, set_hashes
from spacy.language import Language
from spacy.tokens import Doc, Span
from spacy_layout import spaCyLayout

HEADINGS = [DocItemLabel.SECTION_HEADER, DocItemLabel.PAGE_HEADER, DocItemLabel.TITLE]
SEPARATOR = "\n\n"

FONT_SIZE_TEXT = 14
FONT_SIZE_HEADING = 18
CSS_CLS = ".prodigy-annotator:not(:has(.prodigy-page-content)) .prodigy-container"
CSS_CLS_PAGES = ".prodigy-annotator:has(.prodigy-page-content) .prodigy-page-content"
CSS = ".prodigy-content { text-align: left }"
CSS_PREVIEW = f"""
{CSS_CLS}, {CSS_CLS_PAGES} {{ display: grid }}
{CSS_CLS} {{ grid-template-columns: 0 1fr 50% }}
{CSS_CLS_PAGES} {{ grid-template-columns: 1fr 50%; }}
{CSS_CLS} > div:nth-child(2), {CSS_CLS_PAGES} > div:nth-child(1) {{ border-right: 1px solid #ddd }}
{CSS_CLS} > div:nth-child(3), {CSS_CLS_PAGES} > div:nth-child(3) {{ position: relative }}
{CSS_CLS} > div:nth-child(3) > div:first-child, {CSS_CLS_PAGES} > div:nth-child(2) > div:first-child {{ position: sticky; top: 0 }}
{CSS_CLS} .prodigy-meta {{ grid-column: 1 / span 3 }}
"""


def get_layout_tokens(doc: Span, token_labels: Dict[int, str]) -> List[dict]:
    result = []
    offset = 0
    for i, token in enumerate(doc):
        token_label = token_labels.get(token.i)
        token_dict = {
            "text": token.text,
            "start": offset,
            "end": offset + len(token.text),
            "id": i,
            "ws": bool(token.whitespace_),
            "layout": token_label,
        }
        offset += len(token.text)
        if token.text == SEPARATOR:
            token_dict["disabled"] = True
            token_dict["style"] = {"display": "none"}
        if token_label in HEADINGS:
            token_dict["style"] = {"fontWeight": "bold", "fontSize": FONT_SIZE_HEADING}
        result.append(token_dict)
    return result


def get_token_labels(doc: Doc) -> Dict[int, str]:
    labels_by_id = {}
    for span in doc.spans["layout"]:
        for i in range(span.start, span.end):
            labels_by_id[i] = span.label_
    return labels_by_id


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


def disable_tokens(stream: StreamType, disabled: List[str]) -> StreamType:
    for eg in stream:
        for token in eg.get("tokens", []):
            if token.get("layout") in disabled:
                token["disabled"] = True
        yield eg


def remove_preview(stream: StreamType, view_id: ViewId) -> StreamType:
    for eg in stream:
        config = eg.get("config", {})
        if "blocks" in config:
            eg["config"]["blocks"] = [{"view_id": view_id}]
        if "image" in eg:
            del eg["image"]
        yield eg


class LayoutStream:
    def __init__(
        self,
        f: PathInputType,
        nlp: Language,
        file_ext: List[str] = ["pdf"],
        view_id: ViewId = "spans_manual",
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
        self.split_pages = split_pages
        self.hide_preview = hide_preview
        self.focus = focus
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
            images = pdf_to_images(file_path) if not self.hide_preview else None
            pages = []
            for i, (page_layout, page_spans) in enumerate(
                doc._.get(self.layout.attrs.doc_pages)
            ):
                token_labels = get_token_labels(doc)
                page = {
                    "text": SEPARATOR.join(span.text for span in page_spans),
                    "tokens": get_layout_tokens(
                        doc[page_spans[0].start : page_spans[-1].end],
                        token_labels,
                    ),
                    "width": page_layout.width,
                    "height": page_layout.height,
                    "view_id": "blocks",
                    "config": {"blocks": blocks},
                }
                if not self.hide_preview and images:
                    page["image"] = images[i]
                pages.append(page)
                if self.split_pages:
                    meta = {"title": file_path.stem, "page": page_layout.page_no}
                    yield set_hashes({**page, "meta": meta})
            if not self.split_pages:
                yield set_hashes({"pages": pages, "meta": {"title": file_path.stem}})

    def get_focus_stream(self) -> StreamType:
        for file_path in self.paths:
            doc = self.layout(file_path)
            images = pdf_to_images(file_path) if not self.hide_preview else None
            for i, (page_layout, page_spans) in enumerate(
                doc._.get(self.layout.attrs.doc_pages)
            ):
                token_labels = get_token_labels(doc)
                for span in page_spans:
                    if span.label_ not in self.focus:
                        continue
                    blocks = [{"view_id": self.view_id}]
                    if not self.hide_preview:
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
                        blocks.append({"view_id": "image", "spans": image_spans})
                    eg = {
                        "text": span.text,
                        "tokens": get_layout_tokens(span, token_labels),
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
                        "meta": {"title": file_path.stem, "page": page_layout.page_no},
                    }
                    if not self.hide_preview and images:
                        eg["image"] = images[i]
                    yield set_hashes(eg)


@recipe(
    "pdf.spans.manual",
    # fmt: off
    dataset=Arg(help="Dataset to save annotations to"),
    nlp=Arg(help="Loadable spaCy pipeline"),
    source=Arg(help="Path to directory of PDFs or dataset/JSONL file created with pdf.layout.fetch"),
    labels=Arg("--label", "-l", help="Comma-separated label(s) to annotate or text file with one label per line"),
    add_ents=Arg("--add-ents", "-E", help="Add named enitites for the given labels via the spaCy model"),
    focus=Arg("--focus", "-f", help="Focus mode: annotate selected sections of a given type, e.g. 'text'"),
    disable=Arg("--disable", "-d", help="Labels of layout spans to disable, e.g. 'footnote'"),
    split_pages=Arg("--split-pages", "-S", help="View pages as separate tasks"),
    hide_preview=Arg("--hide-preview", "-HP", help="Hide side-by-side preview of layout"),
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
    if source.endswith(".jsonl") or _source_is_dataset(source, None):
        # Load from existing data created with pdf.layout.fetch
        stream = get_stream(source)
    else:
        layout_stream = LayoutStream(
            source,
            nlp=nlp,
            file_ext=["pdf"],
            view_id=view_id,
            split_pages=split_pages,
            hide_preview=hide_preview,
            focus=focus or [],
        )

        stream = Stream.from_iterable(layout_stream.get_stream())
    if add_ents:
        labels = resolve_labels(nlp, "ner", recipe_labels=labels)
        stream.apply(preprocess_ner_stream, nlp, labels=labels, unsegmented=True)
    if disable:
        stream.apply(disable_tokens, disabled=disable)
    css = CSS
    if hide_preview:
        stream.apply(remove_preview, view_id=view_id)
    else:
        css += CSS_PREVIEW

    return {
        "dataset": dataset,
        "stream": stream,
        "view_id": "pages" if not split_pages and not focus else "blocks",
        "config": {
            "labels": labels,
            "global_css": css,
            "shade_bounding_boxes": True,
            "custom_theme": {
                "cardMaxWidth": "95%",
                "smallText": FONT_SIZE_TEXT,
                "tokenHeight": 25,
            },
        },
    }


@recipe(
    "pdf.layout.fetch",
    # fmt: off
    output=Arg(help="Output file or dataset (with prefix dataset:)"),
    nlp=Arg(help="Loadable spaCy pipeline"),
    source=Arg(help="Path to directory to load from"),
    focus=Arg("--focus", "-f", help="Focus mode: annotate selected sections of a given type, e.g. 'text'"),
    split_pages=Arg("--split-pages", "-S", help="View pages as separate tasks"),
    # fmt: on
)
def pdf_layout_fetch(
    output: str,
    nlp: Language,
    source: str,
    focus: Optional[List[str]] = None,
    split_pages: bool = False,
) -> ControllerComponentsDict:
    """
    Pre-process PDFs to use with pdf.spans.manual. This can significantly speed
    up loading time during the annotation process.
    """
    log("RECIPE: Starting recipe pdf.layout.fetch", locals())
    layout_stream = LayoutStream(
        source,
        nlp=nlp,
        file_ext=["pdf"],
        view_id="spans_manual",
        split_pages=split_pages,
        hide_preview=False,
        focus=focus or [],
    )
    msg.info("Creating preprocessed PDFs")
    layout_stream = add_answer(layout_stream.get_stream())
    stream = Stream.from_iterable(layout_stream)
    if _source_is_dataset(output, None):
        dataset = str(output).replace("dataset:", "")
        db = connect()
        if dataset not in db:
            db.add_dataset(dataset)
        db.add_examples(stream, datasets=[dataset])
        msg.good(f"Saved fetched data to dataset {dataset}")
    else:
        srsly.write_jsonl(output, stream)
        msg.good("Saved fetched data to local file", output)
