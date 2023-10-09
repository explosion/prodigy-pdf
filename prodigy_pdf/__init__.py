from typing import List
import base64
from io import BytesIO
from pathlib import Path

import pypdfium2 as pdfium

from prodigy import recipe, set_hashes, ControllerComponentsDict
from prodigy.components.stream import Stream
from prodigy.util import msg

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
                    "pdf": pdf_path.parts[-1],
                    "path": str(pdf_path)
                }
            })
        pdf.close()


@recipe(
    "pdf.image.manual",
    # fmt: off
    dataset=("Dataset to save answers to", "positional", None, str),
    pdf_folder=("Folder with PDFs to annotate", "positional", None, str),
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
    if not pdf_folder.exists():
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

    color = ["#ffff00", "#00ffff", "#ff00ff", "#00ff7f", "#ff6347", "#00bfff",
             "#ffa500", "#ff69b4", "#7fffd4", "#ffd700", "#ffdab9", "#adff2f", 
             "#d2b48c", "#dcdcdc"]

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
