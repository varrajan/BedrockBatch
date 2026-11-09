#!/usr/bin/env python3
"""Render blog.md into blog.docx with clean Amazonian typography."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Inches, Pt, RGBColor

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "blog.md"
DST = ROOT / "blog.docx"

BODY_FONT = "Calibri"
BODY_SIZE = Pt(11)
HEADING_FONT = "Calibri"
TITLE_SIZE = Pt(22)
H2_SIZE = Pt(14)
MONO_FONT = "Consolas"


def parse_markdown(md: str) -> list[tuple[str, str]]:
    """Return a list of (block_type, text) tuples."""
    blocks: list[tuple[str, str]] = []
    paragraph: list[str] = []

    def flush() -> None:
        if paragraph:
            blocks.append(("p", " ".join(paragraph).strip()))
            paragraph.clear()

    for raw in md.splitlines():
        line = raw.rstrip()
        if not line:
            flush()
            continue
        if line.startswith("# "):
            flush()
            blocks.append(("h1", line[2:].strip()))
            continue
        if line.startswith("## "):
            flush()
            blocks.append(("h2", line[3:].strip()))
            continue
        paragraph.append(line)

    flush()
    return blocks


INLINE_CODE_RE = re.compile(r"`([^`]+)`")


def add_runs(paragraph, text: str) -> None:
    """Walk the text, emitting body runs and inline-code runs."""
    cursor = 0
    for match in INLINE_CODE_RE.finditer(text):
        if match.start() > cursor:
            run = paragraph.add_run(text[cursor:match.start()])
            run.font.name = BODY_FONT
            run.font.size = BODY_SIZE
        code = paragraph.add_run(match.group(1))
        code.font.name = MONO_FONT
        code.font.size = BODY_SIZE
        cursor = match.end()
    if cursor < len(text):
        run = paragraph.add_run(text[cursor:])
        run.font.name = BODY_FONT
        run.font.size = BODY_SIZE


def set_margins(section) -> None:
    section.top_margin = Inches(1)
    section.bottom_margin = Inches(1)
    section.left_margin = Inches(1)
    section.right_margin = Inches(1)
    section.page_width = Inches(8.5)
    section.page_height = Inches(11)


def build(blocks: list[tuple[str, str]]) -> Document:
    doc = Document()
    set_margins(doc.sections[0])

    # Set the Normal style once, so inherited paragraphs look consistent.
    normal = doc.styles["Normal"]
    normal.font.name = BODY_FONT
    normal.font.size = BODY_SIZE

    for kind, text in blocks:
        if kind == "h1":
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_after = Pt(14)
            run = p.add_run(text)
            run.bold = True
            run.font.name = HEADING_FONT
            run.font.size = TITLE_SIZE
        elif kind == "h2":
            p = doc.add_paragraph()
            p.paragraph_format.space_before = Pt(14)
            p.paragraph_format.space_after = Pt(6)
            p.paragraph_format.keep_with_next = True
            run = p.add_run(text)
            run.bold = True
            run.font.name = HEADING_FONT
            run.font.size = H2_SIZE
        else:
            p = doc.add_paragraph()
            p.paragraph_format.space_after = Pt(8)
            p.paragraph_format.line_spacing = 1.25
            add_runs(p, text)

    return doc


def main() -> int:
    md = SRC.read_text(encoding="utf-8")
    blocks = parse_markdown(md)
    doc = build(blocks)
    doc.save(DST)
    size = DST.stat().st_size
    print(f"Wrote {DST} ({size} bytes, {len(blocks)} blocks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
