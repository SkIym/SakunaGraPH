from docling.document_converter import DocumentConverter
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption
from docling.datamodel.base_models import InputFormat
import pandas as pd

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False          # PDF is text-native, skip OCR
pipeline_options.do_table_structure = True
pipeline_options.table_structure_options.do_cell_matching = True
pipeline_options.images_scale = 1.0


converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
    }
)

result = converter.convert("../data/raw/dromic/2022/DSWD-DROMIC-Report-81-on-Severe-Tropical-Storm-Paeng-as-of-13-September-2023-6PM.pdf")
doc = result.document

print(f"Tables found: {len(doc.tables)}\n")

for i, table in enumerate(doc.tables):
    # LABEL: Docling auto-associates captions
    caption = table.caption_text(doc)
    prov_pages = [p.page_no for p in table.prov]

    print(f"=== Table {i+1} | pages={prov_pages} | caption={caption!r} ===")
    df = table.export_to_dataframe()
    df.to_csv(f"./dump/{i}.csv")
    print(df.to_string())
    print()
