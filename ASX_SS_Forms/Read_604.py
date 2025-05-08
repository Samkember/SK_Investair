import re
import pdfplumber
import pytesseract
from pdf2image import convert_from_path
import os


pdf_path = "/Users/samkember/Documents/SK_Investair/ASX_SS_Forms/02849317.pdf"



def clean_holder_name(raw_name):
    return re.split(
        r"\s*(on behalf of|named in|listed in|as trustee for|to this form)\b",
        raw_name,
        flags=re.IGNORECASE
    )[0].strip()

def extract_with_pdfplumber(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Extract name
            match_name = re.search(r"(?i)Name\s+(.+)", text)
            substantial_holder = None
            if match_name:
                raw_name = match_name.group(1).strip()
                substantial_holder = clean_holder_name(raw_name)

            # Extract vote changes
            match_votes = re.search(
                r"Class of securities.*?(\d[\d,]*)\s+([\d.]+%)\s+(\d[\d,]*)\s+([\d.]+%)",
                text,
                re.DOTALL
            )
            if match_votes:
                previous_votes = int(match_votes.group(1).replace(",", ""))
                previous_pct = match_votes.group(2)
                present_votes = int(match_votes.group(3).replace(",", ""))
                present_pct = match_votes.group(4)
                return {
                    "method": "pdfplumber",
                    "holder": substantial_holder,
                    "previous_votes": previous_votes,
                    "previous_pct": previous_pct,
                    "present_votes": present_votes,
                    "present_pct": present_pct
                }
    return None  # fallback to OCR

def extract_with_ocr(pdf_path):
    pages = convert_from_path(pdf_path)
    for img in pages:
        text = pytesseract.image_to_string(img)

        match_name = re.search(r"(?i)Name\s+(.+)", text)
        substantial_holder = None
        if match_name:
            raw_name = match_name.group(1).strip()
            substantial_holder = clean_holder_name(raw_name)

        match_votes = re.search(
            r"Class of securities.*?(\d[\d,]*)\s+([\d.]+%)\s+(\d[\d,]*)\s+([\d.]+%)",
            text,
            re.DOTALL
        )
        if match_votes:
            previous_votes = int(match_votes.group(1).replace(",", ""))
            previous_pct = match_votes.group(2)
            present_votes = int(match_votes.group(3).replace(",", ""))
            present_pct = match_votes.group(4)
            return {
                "method": "ocr",
                "holder": substantial_holder,
                "previous_votes": previous_votes,
                "previous_pct": previous_pct,
                "present_votes": present_votes,
                "present_pct": present_pct
            }
    return None

# Try PDFPlumber first
result = extract_with_pdfplumber(pdf_path)

# Fallback to OCR
if result is None:
    result = extract_with_ocr(pdf_path)

# Output result
if result:
    print(f"Method Used: {result['method']}")
    print("Substantial Holder:", result['holder'])
    print("Previous Votes:", result['previous_votes'], "| Voting Power:", result['previous_pct'])
    print("Present Votes:", result['present_votes'], "| Voting Power:", result['present_pct'])
else:
    print("⚠️ Unable to extract data using either method.")
