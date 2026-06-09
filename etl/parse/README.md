
**NDRRMC**
- Done: Parsing for PDF files w/ machine-readable text
- To do/fix: ocr for nonreadable/nonselectable text

**DROMIC**
- Done: script to convert doc/x files into pdf (works in Windowms machine only)
- Done: Parsing for PDF files 
- To do/fix: Currently, script is prone to column data duplication errors in assistance tables. 

**FAILURE CHECKCING**
- Run `check_fails.py` for each subdata folder (e.g dromic/2022 ). This logs parsed files/events that contain errors. 
- Output is checked by the pipeline. Failures are skipped to prevent polluting graph with erratic data.
