<a href="https://explosion.ai"><img src="https://explosion.ai/assets/img/logo.svg" width="125" height="125" align="right" /></a>

# 📄 Prodigy-PDF

This repository contains a Prodigy plugin for recipes that annotating PDF files. At the moment it features a recipe that can turn the PDF into an image, which can then be annotated using the familiar `image_manual` interface. Here's a preview of the interface:

<p align="center">
  <img src="images/pdf_image_manual.png" width="50%">
</p>


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
