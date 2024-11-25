<a href="https://explosion.ai"><img src="https://explosion.ai/assets/img/logo.svg" width="125" height="125" align="right" /></a>

# 📄 Prodigy-PDF

This repository contains a [Prodigy](https://prodi.gy) plugin with recipes for image- and text-based annotation of PDF files, as well as recipes for OCR (Optical Character Recognition) to extract content from documents. The `pdf.spans.manual` recipe uses [`spacy-layout`](https://github.com/explosion/spacy-layout) and [Docling](https://ds4sd.github.io/docling/) to extract the text contents from PDFs and lets you annotate spans of text, with an optional side-by-side preview of the original document and pre-fetching for faster loading during annotation.

![pdf.image.manual recipe](https://github.com/user-attachments/assets/da40ee4c-369d-407d-a412-fdb8d341aee8)

![pdf_spans_manual](https://github.com/user-attachments/assets/bc0a5fe8-1995-4ff8-8766-7dc4a03a52be)


You can install this plugin via `pip`.

```
pip install "prodigy-pdf @ git+https://github.com/explosion/prodigy-pdf"
```

If you want to use the OCR recipes, you'll also want to ensure that tesseract is installed.

```bash
# for mac
brew install tesseract

# for ubuntu
sudo apt install tesseract-ocr
```

To learn more about this plugin, you can check the [Prodigy docs](https://prodi.gy/docs/plugins/#pdf).

## Issues?

Are you have trouble with this plugin? Let us know on our [support forum](https://support.prodi.gy/) and we'll get back to you!
