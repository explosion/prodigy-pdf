from pathlib import Path
from setuptools import setup, find_packages

setup(
    name="prodigy-pdf",
    description="a collection recipes for prodigy to deal with pdfs",
    packages=find_packages(exclude=["notebooks", "tests"]),
    install_requires=Path("requirements.txt").read_text().split("\n"),
)
