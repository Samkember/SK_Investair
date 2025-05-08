import fitz  # PyMuPDF
import pandas as pd
import re

# Assume your output list is stored in a variable called `blocks`
# Each block is a tuple where the 5th element is the text


def save_text_blocks_to_txt(blocks, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        for block in blocks:
            raw_text = block[4]
            f.write(raw_text)
            # print(raw_text)
            # escaped_text = raw_text.encode('unicode_escape').decode('utf-8')  # shows \n literally
            # f.write(escaped_text + '\n\n')  # double spacing between blocks for readability


def extract_section_4_table(pdf_path):
    doc = fitz.open(pdf_path)
    section_started = False
    section_blocks = []

    for page in doc:
        blocks = page.get_text("blocks")

        for block in sorted(blocks, key=lambda b: (b[1], b[0])):  # sort by vertical, then horizontal
            text = block[4].strip()

            # Detect section 4 start
            if re.match(r"4\.\s*Present relevant interests", text, re.IGNORECASE):
                section_started = True
                continue

            # Detect section 5 start to stop
            if section_started and re.match(r"5\.\s*Changes in association", text, re.IGNORECASE):
                section_started = False
                break

            if section_started:
                section_blocks.append(block)

    # Group blocks into lines based on Y coordinate
    output_file = 'ASX_SS_Forms\Output.txt'

    print(section_blocks)
    save_text_blocks_to_txt(section_blocks, output_file)

# Usage
pdf_path = r'/Users/samkember/Documents/SK_Investair/ASX_SS_Forms/02903407.pdf'
df_section4 = extract_section_4_table(pdf_path)

# df_section4.to_csv("output.csv", index=False)

print(df_section4)

print("✅ Extracted Section 4 Table:")
