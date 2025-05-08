import pdfplumber
import pandas as pd
import re

# Path to your PDF
pdf_path = "/Users/samkember/Documents/SK_Investair/ASX_SS_Forms/02903407.pdf"

# List to hold extracted rows
records = []

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            for row in table:
                if not row or "ordinary" not in str(row).lower():
                    continue
                # Try to match relevant rows with expected number of columns
                if len(row) >= 6:
                    holder = row[0].strip()
                    shares_text = row[4]
                    votes_text = row[5]
                    try:
                        shares = int("".join(filter(str.isdigit, shares_text)))
                        votes = int("".join(filter(str.isdigit, votes_text)))
                        records.append({
                            "Holder": holder,
                            "Shares Held": shares,
                            "Votes": votes
                        })
                    except:
                        continue

# Convert to DataFrame
df = pd.DataFrame(records).drop_duplicates()
print(df)