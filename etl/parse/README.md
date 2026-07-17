
**NDRRMC**
- Done: Parsing for PDF files w/ machine-readable text
- To do/fix: ocr for nonreadable/nonselectable text

**DROMIC**
- Done: script to convert doc/x files into pdf (works in Windowms machine only)
- Done: Parsing for PDF files 
- To do/fix: Currently, script is prone to column data duplication errors in assistance tables. 

**FAILURE CHECKCING**
- Run `python parse/check_fails.py --all` from `etl/` to check every parsed
  DROMIC year, or use `--year 2022` for one year. An event is registered in
  `_needs_rerun.txt` only when a numbered CSV has a matching base CSV in the
  same folder, such as `damaged_houses.csv` and `damaged_houses_1.csv`.
- Output is checked by the pipeline. Failures are skipped to prevent polluting graph with erratic data.
