import pdfplumber
import pytesseract
import re
from PIL import Image
from pdf2image import convert_from_bytes
import io

def extract_text_plumber_from_bytes(pdf_bytes: bytes) -> str:
    text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text.strip()

def extract_text_ocr_from_bytes(pdf_bytes: bytes) -> str:
    images = convert_from_bytes(pdf_bytes, dpi=300)
    text = ""
    for img in images:
        text += pytesseract.image_to_string(img, config='--psm 6') + "\n"
    return text.strip()

def split_sections(text: str) -> dict:
    heading_keywords = [
        "Details of substantial holder",
        "Previous and present voting power",
        "Changes in relevant interests",
        "Present relevant interests"
    ]
    pattern = re.compile(
        rf"(?=\n?\s*(?:\d+\.\s*)?({'|'.join(re.escape(h) for h in heading_keywords)}))",
        re.IGNORECASE
    )

    chunks = pattern.split(text)
    section_dict = {}

    i = 1
    while i < len(chunks):
        raw_heading = chunks[i].strip()
        content = chunks[i + 1].strip() if (i + 1) < len(chunks) else ""
        clean_heading = re.sub(r"^\d+\.\s*", "", raw_heading).lower()
        section_dict[clean_heading] = content.lower()
        i += 2

    return section_dict

def get_voters(text: str) -> dict:
    votes = re.findall(r'\d{1,3}(?:,\d{3})+', text)
    percents = re.findall(r'\d+\.\d+%', text)
    return {
        "previous_votes": votes[0] if len(votes) > 0 else None,
        "previous_power": percents[0] if len(percents) > 0 else None,
        "present_votes": votes[1] if len(votes) > 1 else None,
        "present_power": percents[1] if len(percents) > 1 else None
    }

def get_name(text: str) -> str | None:
    match = re.search(r"name\s+(.*)", text, re.IGNORECASE)
    return match.group(1).strip() if match else None

def extract_from_pdf_bytes(pdf_bytes: bytes) -> dict:
    try:
        # Try plumber first
        text = extract_text_plumber_from_bytes(pdf_bytes)
        if not all(kw in text.lower() for kw in ['name', 'voting power']):
            text = extract_text_ocr_from_bytes(pdf_bytes)
            print(" → OCR fallback used")
        sections = split_sections(text)

        data = {
            "name": get_name(sections.get('details of substantial holder', '')),
            **get_voters(sections.get('previous and present voting power', ''))
        }
        return data
    except Exception as e:
        print(f"[!] Extraction failed: {e}")
        return {"error": str(e)}