from typing import List, Dict
import base64
from io import BytesIO
from pathlib import Path
from PIL import Image

import pytesseract
import pypdfium2 as pdfium

from prodigy import recipe, set_hashes, ControllerComponentsDict
from prodigy.components.stream import Stream, get_stream
from prodigy.util import msg, split_string


def page_to_image(page: pdfium.PdfPage) -> str:
    """Turns a PdfPage into a base64 image for Prodigy"""
    pil_image = page.render().to_pil()
    with BytesIO() as buffered:
        pil_image.save(buffered, format="JPEG")
        img_str = base64.b64encode(buffered.getvalue())
    return f"data:image/png;base64,{img_str.decode('utf-8')}"


def generate_pdf_pages(pdf_paths: List[Path]):
    """Generate dictionaries that contain an image for each page in the PDF"""
    for pdf_path in pdf_paths:
        pdf = pdfium.PdfDocument(pdf_path)
        n_pages = len(pdf)
        for page_number in range(n_pages):
            page = pdf.get_page(page_number)
            yield set_hashes({
                "image": page_to_image(page), 
                "meta": {
                    "page": page_number,
                    "path": str(pdf_path)
                }
            })
        pdf.close()


@recipe(
    "pdf.image.manual",
    # fmt: off
    dataset=("Dataset to save answers to", "positional", None, str),
    pdf_folder=("Folder with PDFs to annotate", "positional", None, Path),
    labels=("Comma seperated labels to use", "option", "l", str),
    remove_base64=("Remove base64-encoded image data", "flag", "R", bool)
    # fmt: on
)
def pdf_image_manual(
    dataset: str,
    pdf_folder: Path,
    labels:str,
    remove_base64:bool=False
) -> ControllerComponentsDict:
    """Turns pdfs into images in order to annotate them."""
    # Read in stream as a list for progress bar.
    if not Path(pdf_folder).exists():
        msg.fail(f"Folder `{pdf_folder}` does not exist.", exits=True)
    pdf_paths = list(Path(pdf_folder).glob("*.pdf"))
    if len(pdf_paths) == 0:
        msg.fail("Did not find any .pdf files in folder.")
    source = Stream.from_iterable(pdf_paths).apply(generate_pdf_pages)

    def before_db(examples):
        # Remove all data URIs before storing example in the database
        for eg in examples:
            if eg["image"].startswith("data:"):
                del eg["image"]
        return examples

    color = ["#00ffff", "#ff00ff", "#00ff7f", "#ff6347", "#00bfff",
             "#ffa500", "#ff69b4", "#7fffd4", "#ffd700", "#ffdab9", "#adff2f", 
             "#d2b48c", "#dcdcdc", "#ffff00", ]

    return {
        "dataset": dataset,
        "stream": source,
        "before_db": before_db if remove_base64 else None,
        "view_id": "image_manual",
        "config": {
            "labels": labels.split(","),
            "image_manual_stroke_width": 2,
            "custom_theme": {
                "labels": {
                    lab: color[i] for i, lab in enumerate(labels.split(","))
                }
            }
        },
    }


def page_to_cropped_image(pil_page: Image, span: Dict, scale: int):
    left, upper = span['x'], span['y']
    right, lower = left + span['width'], upper + span['height']
    scaled = (left * scale, upper * scale, right * scale, lower * scale)
    cropped = pil_page.crop(scaled)
    with BytesIO() as buffered:
        cropped.save(buffered, format="JPEG")
        img_str = base64.b64encode(buffered.getvalue())
    return cropped, f"data:image/png;base64,{img_str.decode('utf-8')}"


def fold_ocr_dashes(ocr_input:str) -> str:
    """
    OCR might literally add dashes at the end of the line to indicate
    continuation of the word. This can be fine in some cases, but this
    function can fold it all into a single string. 
    """
    new = ""
    for line in ocr_input.split("\n"):
        line = line.strip()
        if line.rfind("-") == -1:
            newline = line + " "
        else:
            newline = line[:line.rfind("-")]
        new += newline
    return new.strip()


def _validate_ocr_example(ex: Dict):
    if 'meta' not in ex:
        raise ValueError(f"It seems the `meta` key is missing from an example: {ex}. Did you annotate this data with `pdf.image.manual`?")
    if 'path' not in ex['meta']:
        raise ValueError(f"It seems the `path` key is missing from an example metadata: {ex}. Did you annotate this data with `pdf.image.manual`?")
    if 'page' not in ex['meta']:
        raise ValueError(f"It seems the `page` key is missing from an example metadata: {ex}. Did you annotate this data with `pdf.image.manual`?")


@recipe(
    "pdf.ocr.correct",
    # fmt: off
    dataset=("Dataset to save answers to", "positional", None, str),
    source=("Source with PDF Annotations", "positional", None, str),
    labels=("Labels to consider", "option", "l", split_string),
    scale=("Zoom scale. Increase above 3 to upscale the image for OCR.", "option", "s", int),
    remove_base64=("Remove base64-encoded image data", "flag", "R", bool),
    fold_dashes=("Removes dashes at the end of a textline and folds them with the next term.", "flag", "f", bool),
    autofocus=("Autofocus on the transcript UI", "flag", "af", bool)
    # fmt: on
)
def pdf_ocr_correct(
    dataset: str,
    source: str,
    labels: str,
    scale: int = 3,
    remove_base64:bool=False,
    fold_dashes:bool = False,
    autofocus: bool = False
) -> ControllerComponentsDict:
    """Applies OCR to annotated segments and gives a textbox for corrections."""
    stream = get_stream(source)

    def new_stream(stream):
        for ex in stream:
            useful_spans = [span for span in ex.get('spans', []) if span['label'] in labels]
            if useful_spans:
                _validate_ocr_example(ex)
                pdf = pdfium.PdfDocument(ex['meta']['path'])
                page = pdf.get_page(ex['meta']['page'])
                pil_page = page.render(scale=scale).to_pil()
            for annot in useful_spans:
                cropped, img_str = page_to_cropped_image(pil_page, span=annot, scale=scale)
                annot["image"] = img_str
                annot["text"] = pytesseract.image_to_string(cropped)
                if fold_dashes:
                    annot["text"] = fold_ocr_dashes(annot["text"])
                annot["transcription"] = annot["text"]
                
                # passing through metadata, in order to connect OCR text & bounding boxes to the pdf images via path
                # example usecase, finetuning a layoutLM on custom data
                # see details here https://support.prodi.gy/t/pdf-ocr-image-annotation-metadata-feature-suggestion/7211
                annot["meta"] = ex["meta"]
                
                text_input_fields = {
                    "field_rows": 12,
                    "field_label": "Transcript",
                    "field_id": "transcription",
                    "field_autofocus": autofocus,
                }
                del annot['id']
                yield set_hashes({**annot, **text_input_fields})

    def before_db(examples):
        # Remove all data URIs before storing example in the database
        for eg in examples:
            if eg["image"].startswith("data:"):
                del eg["image"]
        return examples
    
    blocks = [{"view_id": "classification"}, {"view_id": "text_input"}]

    return {
        "dataset": dataset,
        "stream": new_stream(stream),
        "before_db": before_db if remove_base64 else None,
        "view_id": "blocks",
        "config": {
            "blocks": blocks
        },
    }
