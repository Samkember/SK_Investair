from openai import OpenAI
from pydantic import BaseModel
from typing import List

from get_parties import get_parties

# === Define the Pydantic models ===
class PartyBlock(BaseModel):
    party_name: str
    holding_type: str
    votes: float
    associated_party: str

class PartyList(BaseModel):
    parties: List[PartyBlock]

# === Load your document text ===
with open("02768986_tables.txt", "r") as f:
    document_text = f.read()

# === Define the known parties for this document ===
companies = get_parties()
print("Company names extracted:", companies)

# === Build the prompt ===
party_list_str = "\n".join(f"- {name}" for name in companies)

prompt = f"""
You are **LegalExtract-AI**, an elite corporate finance lawyer and data extraction specialist.

Your task is to analyse the raw text of an Australian Substantial Holding notice (Form 603 / 604 / 605) and return a single clean **JSON array** of all entities that currently hold a *relevant interest* in the securities.

You **must** include **one JSON object per party** listed in `{companies}` (the known list of entities we are extracting info for). Each company must be represented exactly one.

Each JSON object must contain exactly **four fields**:

1. party_name
The full legal name of the entity, as it appears **the first time** in the document. Strip quotation marks and redundant brackets.  

2. holding_type
Choose the most appropriate classification from this fixed list **(based on first matching rule)**:
 - custodian – Holds shares on behalf of another party. Look for names with "Nominees", "Custody", "Citicorp", "HSBC", "BNP Paribas", or "J.P. Morgan Chase". Usually listed as the registered holder.
 - sub_custodian – Acts on behalf of a global custodian. May be described as holding securities for another custodian. Often used for local settlement (e.g. in Australia).
 - prime_broker – Involved in stock lending or margin financing. Look for mentions of Annexure B, stock loans, or counterparties to agreements. Common names: Morgan Stanley, UBS, Goldman Sachs.
 - parent_entity - The entity who is submitting the document.
 - controlled_entity – An entity fully owned or controlled under s608(3)(a). Often described as being 100% owned or having full voting power held by a parent company.
 - associated_entity – A party in which another entity has more than 20% voting power (s608(3)(b)). Look for terms like “has a relevant interest by holding more than 20%” or “can influence board composition”.
 - fund_active_diversified – An actively managed fund with a broad mandate. No reference to “Small Cap” or hedge strategies. May be listed as a managed investment scheme.
 - fund_active_smallcap – An active fund focusing on small- and mid-cap companies. Look for names that include “Small Companies Fund” or “Emerging Companies”.
 - fund_etf_index – A passive fund or ETF. Names often include “Index Fund”, “ETF”, or providers like Vanguard, iShares, or BetaShares. Rarely involved in lending.
 - fund_hedge – A hedge fund or long/short strategy. Look for “Absolute Return”, “Long Short”, “Opportunities Fund”, or cross-references to lending or swaps. May be linked to prime brokers.

  
  
3. associated_party
The name of the party that is related upstream to the current party. For example the `parent_holder` of a `controlled_entity`, or the `prime_broker` of a `fund_lender`. This is used to indicate the relationship between parties.
In the case of the Corporations Act 2001 (Cth), this is the party that has voting power of 100% in another entity.
When custodians are used, the associated party is the controlled entity, not necessarily the `parent_holder`.
If there is no associated party, use an empty string "".


4. votes
The total number of shares or votes now held by the entity.
You **must aggregate all entries of the same holder** before reporting, this is very important.
Format as an integer (no commas or decimals). Do not generate estimates, ensure that the number is exactly as it appears in the document.
You may need to explore the nature of relevant interests to understand how to aggregate votes correctly.
When one entity controls 100% of another entity, the votes are represented in both the controlled entity and the controlling entity. Examine the 'nature of relevant interest' to extract this correctly.

––DOCUMENT START––
{document_text}
"""


# === Run the model and parse into structured Python objects ===
client = OpenAI()

response = client.responses.parse(
    model="gpt-4o-2024-08-06",
    input=[
        {"role": "user", "content": prompt}
    ],
    text_format=PartyList,
    temperature=0.0
)

# === Use the parsed result ===
parsed = response.output_parsed

for party in parsed.parties:
    print(f"Party: {party.party_name}")
    print(f"  Holding Type: {party.holding_type}")
    print(f"  Votes: {party.votes:,.0f}")
    print(f"  Associated Party: {party.associated_party}")
    print()
