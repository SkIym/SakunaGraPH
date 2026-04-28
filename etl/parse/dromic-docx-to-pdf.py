import argparse
import shutil
import time
from typing import Any
from pathlib import Path
import win32com.client
from docx import Document
from docx.oxml.ns import qn
from lxml import etree
from concurrent.futures import ThreadPoolExecutor

def preprocess_all(files: list[Path], dest: Path, min_height_pt: float, workers: int = 4) -> dict[Path, Path]:
    """
    Pre-process all docx files in parallel (python-docx is I/O bound).
    Returns mapping of original path → processed path.
    """
    dest.mkdir(parents=True, exist_ok=True)
    results: dict[Path, Path] = {}

    def _process(f: Path) -> tuple[Path, Path]:
        processed = dest / f"_processed_{f.name}"
        ok = enforce_min_row_height(f, processed, min_height_pt)
        return f, processed if ok else f

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for orig, processed in ex.map(_process, files):
            results[orig] = processed

    return results

# =============================================================================
# PRE-PROCESSING: enforce minimum row height via python-docx
# =============================================================================

def enforce_min_row_height(docx_path: Path, output_path: Path, min_height_pt: float = 14.0) -> bool:
    """
    Set a minimum row height on all table rows in the docx.
    Uses 'atLeast' rule so Word can expand rows if content needs more space
    but won't shrink below min_height_pt — prevents rows from being so compact
    that Docling and pdfplumber merge adjacent rows in the output PDF.
    """
    try:
        doc = Document(str(docx_path))
        min_twips = int(min_height_pt * 20)  # 1pt = 20 twips in OOXML

        for table in doc.tables:
            for row in table.rows:
                tr = row._tr
                trPr = tr.find(qn("w:trPr"))
                if trPr is None:
                    trPr = etree.SubElement(tr, qn("w:trPr"))
                    tr.insert(0, trPr)

                trHeight = trPr.find(qn("w:trHeight"))
                if trHeight is None:
                    trHeight = etree.SubElement(trPr, qn("w:trHeight"))

                current = trHeight.get(qn("w:val"))
                if current is None or int(current) < min_twips:
                    trHeight.set(qn("w:val"), str(min_twips))
                    trHeight.set(qn("w:hRule"), "atLeast")

        doc.save(str(output_path))
        return True
    except Exception as e:
        print(f"[WARN] enforce_min_row_height failed for {docx_path.name}: {e}")
        return False


# =============================================================================
# WORD COM: enforce repeat header rows
# =============================================================================

def enforce_table_headers(doc: Any, header_rows: int = 1):
    """
    Emulate Word UI 'Repeat Header Rows' without using Rows(n),
    which breaks on vertically merged tables.
    """
    for table in doc.Tables:
        try:
            table.AllowAutoFit = True
            table.AutoFitBehavior(0)  # wdAutoFitContent

            table.TopPadding = 0
            table.BottomPadding = 0
            table.LeftPadding = 0
            table.RightPadding = 0

            start = table.Cell(1, 1).Range.Start

            try:
                end = table.Cell(header_rows, table.Columns.Count).Range.End
            except:
                first_cell = table.Cell(1, 1)
                end = first_cell.Range.End
                for cell in table.Range.Cells:
                    if cell.RowIndex != 1:
                        break
                    end = cell.Range.End

            rng = doc.Range(Start=start, End=end)
            rng.Rows.HeadingFormat = True

        except Exception as e:
            print(f"[WARN] Header enforcement failed: {e}")


# =============================================================================
# CONVERSION
# =============================================================================

def convert_docx_to_pdf_word(src: Path, dest: Path, retries: int=3, delay: float=1.5, min_row_height_pt: float=14.0) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)

    files = [f for f in src.rglob("*")
             if f.is_file() and f.suffix.lower() in {".doc", ".docx"}
             and not (dest / f.with_suffix(".pdf").name).exists()]

    print(f"Pre-processing {len(files)} files...")
    docx_files = [f for f in files if f.suffix.lower() == ".docx"]
    processed_map = preprocess_all(docx_files, dest, min_row_height_pt, workers=4)

    print(f"Converting {len(files)} files via Word COM...")
    word = win32com.client.Dispatch("Word.Application")
    word.Visible = False
    converted: list[Path] = []

    try:
        for f in files:
            pdf_path = dest / f.with_suffix(".pdf").name
            src_for_word = processed_map.get(f, f)

            attempt = 0
            while attempt < retries:
                doc = None
                try:
                    doc = word.Documents.Open(str(src_for_word.resolve()))
                    enforce_table_headers(doc)
                    doc.ExportAsFixedFormat(
                        OutputFileName=str(pdf_path.resolve()),
                        ExportFormat=17,
                    )
                    doc.Close(False)
                    converted.append(pdf_path)
                    print(f"[OK] {f.name}")
                    break
                except Exception as e:
                    attempt += 1
                    if doc:
                        try: doc.Close(False)
                        except: pass
                    if attempt >= retries:
                        print(f"[FAIL] {f.name}: {e}")
                    else:
                        time.sleep(delay)

            # Clean up temp file
            if f in processed_map and processed_map[f] != f:
                try: processed_map[f].unlink()
                except: pass
    finally:
        word.Quit()
        del word

    return converted


def copy_pdfs_jsons(src: Path, dst: Path) -> None:
    """Copy any PDFs that already exist and jsons in src into dst (no conversion needed)."""
    dst.mkdir(parents=True, exist_ok=True)
    for p in list(src.rglob("*.pdf")) + list(src.rglob("*.json")):
        if p.is_file():
            target = dst / p.name
            if target.exists():
                continue
            shutil.copy2(p, target)



# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Convert DROMIC .doc/.docx files to PDF")
    parser.add_argument("--year",           required=True,  help="Year to process")
    parser.add_argument("--input-dir",      default=None,   help="Override input directory")
    parser.add_argument("--output-dir",     default=None,   help="Override output directory")
    parser.add_argument("--min-row-height", default=18.0,   type=float,
                        help="Minimum table row height in points (default: 14.0)")
    args = parser.parse_args()

    input_dir  = Path(args.input_dir)  if args.input_dir  else Path(f"../data/raw/dromic-new/{args.year}")
    output_dir = Path(args.output_dir) if args.output_dir else Path(f"../data/raw/dromic-new/{args.year}-pdf")

    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Min row height: {args.min_row_height}pt")

    converted = convert_docx_to_pdf_word(
        input_dir, output_dir,
        min_row_height_pt=args.min_row_height,
    )
    copy_pdfs_jsons(input_dir, output_dir)

    print(f"\nDone. Converted {len(converted)} file(s).")


if __name__ == "__main__":
    main()