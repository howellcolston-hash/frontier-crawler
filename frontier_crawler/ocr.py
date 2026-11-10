"""Read the text of scanned PDFs with Tesseract (free, runs locally, no model call).

A scanned PDF is pages of pictures, so the fetcher finds no text in it. This runs later,
over PDFs already stored, and writes what it reads to the ocr_text table.

Input contract: text, pages_read, pages_total, langs = read_scan(pdf_bytes)
Needs the `tesseract` program with its `osd` data and the language packs named below.
"""

from __future__ import annotations

import io
import re
import subprocess
import time
from typing import Tuple

# PROVISIONAL. A stored PDF with fewer words than this is taken to be a scan.
SCAN_MAX_WORDS = 20
# PROVISIONAL limits per file. Reading is a few seconds a page, and the stored PDF is complete either way.
OCR_MAX_PAGES = 100
OCR_SECONDS = 600.0
# Tesseract must be told which languages to expect. The first page's script picks the group.
LANGS_BY_SCRIPT = {"Arabic": "ara+eng", "Cyrillic": "rus+eng", "Han": "chi_sim+eng"}
DEFAULT_LANGS = "eng+fra+spa+por+ind"  # Latin script


def _tesseract(png: bytes, *args: str) -> str:
    done = subprocess.run(["tesseract", "stdin", "stdout", *args], input=png, capture_output=True, timeout=120)
    return done.stdout.decode("utf-8", errors="replace")


def read_scan(body: bytes) -> Tuple[str, int, int, str]:
    import pypdfium2

    pdf = pypdfium2.PdfDocument(body)
    try:
        total = len(pdf)
        parts = []
        langs = DEFAULT_LANGS
        deadline = time.monotonic() + OCR_SECONDS
        for index in range(min(total, OCR_MAX_PAGES)):
            if time.monotonic() > deadline:
                break
            page = pdf[index]
            buffer = io.BytesIO()
            page.render(scale=2).to_pil().save(buffer, format="PNG")  # about 144 dots per inch
            page.close()
            if index == 0:
                script = re.search(r"Script: (\w+)", _tesseract(buffer.getvalue(), "--psm", "0"))
                langs = LANGS_BY_SCRIPT.get(script.group(1), DEFAULT_LANGS) if script else DEFAULT_LANGS
            parts.append(_tesseract(buffer.getvalue(), "-l", langs))
        return "\n".join(parts).strip(), len(parts), total, langs
    finally:
        pdf.close()
