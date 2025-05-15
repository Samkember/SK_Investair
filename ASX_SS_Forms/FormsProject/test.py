import pdfplumber
import pytesseract
from PIL import Image
from PIL import ImageEnhance, ImageOps, ImageStat
from openai import OpenAI
import os
import re
from glob import glob
from pdf2image import convert_from_path
import pytesseract
import cv2


def extract_text_plumber(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text.strip()

def extract_text_ocr(pdf_path):
    images = convert_from_path(pdf_path, dpi=300)
    text = ""
    for img in images:
        text += pytesseract.image_to_string(img, config='--psm 6') + "\n"
    return text.strip()


def split_sections(text):
    # Define known section titles (add more as needed)
    heading_keywords = [
        "Details of substantial holder",
        "Previous and present voting power",
        "Changes in relevant interests",
        "Present relevant interests"
    ]

    # Build regex that matches optional leading number (e.g. "1. ") plus the known heading
    pattern = re.compile(
        rf"(?=\n?\s*(?:\d+\.\s*)?({'|'.join(re.escape(h) for h in heading_keywords)}))",
        re.IGNORECASE
    )

    # Split and tag
    chunks = pattern.split(text)
    section_dict = {}

    i = 1
    while i < len(chunks):
        raw_heading = chunks[i].strip()
        content = chunks[i + 1].strip() if (i + 1) < len(chunks) else ""

        # Remove any leading numbering from heading (e.g., "1. Details..." → "Details...")
        clean_heading = re.sub(r"^\d+\.\s*", "", raw_heading).lower()

        section_dict[clean_heading] = content.lower()
        i += 2

    return section_dict

def get_voters(text):

    votes = re.findall(r'\d{1,3}(?:,\d{3})+', text)
    percents = re.findall(r'\d+\.\d+%', text)

    return {
        "previous_votes": votes[0] if len(votes) > 0 else None,
        "previous_power": percents[0] if len(percents) > 0 else None,
        "present_votes": votes[1] if len(votes) > 1 else None,
        "present_power": percents[1] if len(percents) > 1 else None
    }

def get_name(text):
    match = re.search(r"name\s+(.*)", text, re.IGNORECASE)
    name = match.group(1).strip() if match else None
    return name
    



def get_contents(sections):

    data = {}
    data['name'] = get_name(sections['details of substantial holder'])
    data['votes'] = get_voters(sections['previous and present voting power'])

    return data

import csv
import os

def process_pdf(pdf_path, csv_path="output.csv"):
    print(f"Processing: {pdf_path}")

    try:
        text = extract_text_plumber(pdf_path)
        critical_text = ['name', 'voting power']

        if not all(keyword.lower() in text.lower() for keyword in critical_text):
            text = extract_text_ocr(pdf_path)
            print('OCR fallback used')

        sections = split_sections(text)

        data = get_contents(sections)  # assumed to return a dict

        votes = data.get("votes", {})

        row = {
            "pdf_path": pdf_path,
            "name": data.get("name"),
            "previous_votes": votes.get("previous_votes"),
            "previous_power": votes.get("previous_power"),
            "present_votes": votes.get("present_votes"),
            "present_power": votes.get("present_power")
        }


        # Write or append to CSV
        file_exists = os.path.isfile(csv_path)
        with open(csv_path, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        print("Saved:", row)

    except Exception as e:
        print(f"Failed to process {pdf_path}: {e}")



if __name__ == "__main__":


    # process_pdf('sample/02850000.pdf','test.csv')

    for pdf in glob("sample/*.pdf"):

        process_pdf(pdf)


