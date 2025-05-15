import fitz  # PyMuPDF
import re
import pandas as pd

def extract_section_4_holders(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""

    # Combine text from all pages
    for page in doc:
        text += page.get_text()

    # Find "4. Present relevant interests"
    match = re.search(r"4\.\s*Present relevant interests", text, re.IGNORECASE)
    if not match:
        print("❌ Section 4 not found.")
        return []

    section_start = match.start()
    section_text = text[section_start:]

    # Optionally cut off at Section 5
    section_5_match = re.search(r"5\.\s*Changes in association", section_text, re.IGNORECASE)
    if section_5_match:
        section_text = section_text[:section_5_match.start()]

    # Now extract entities listed under "Holder of relevant interest"
    lines = section_text.splitlines()

    return lines
    

# Run it
pdf_path = r'/Users/samkember/Documents/SK_Investair/ASX_SS_Forms/02903407.pdf'
holders = extract_section_4_holders(pdf_path)


create_table(holders)


print("✅ Registered Holders of Relevant Interests (Section 4):")
# for name in holders:
#     # print("-", name)
