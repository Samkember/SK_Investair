from openai import OpenAI
from dotenv import load_dotenv
import json



def get_parties():
    file_path = "02768986_tables.txt"
    with open(file_path, "r") as f:
        file_contents = f.read()


    prompt = """
    Extract the name of any company mentioned in this document, even if they are only mentioned in passing. Format names in all caps.
    """ + file_contents


    # file_path = "02768986_prompt.txt"
    # with open(file_path, "w") as f:
    #     f.write(prompt)


    load_dotenv()
    client = OpenAI()


    # Structured Output mode
    input_messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": prompt
                }
            ]
        }
    ]

    response = client.responses.create(
        model="gpt-4o-mini",
        input=input_messages,
        text={
            "format": {
                "type": "json_schema",
                "name": "parties",
                "schema": {
                    "type": "object",
                    "properties": {
                        "parties": {
                            "type": "array",
                            "items": {
                                "type": "string",
                            }
                        },
                    },
                    "required": ["parties"],
                    "additionalProperties": False
                },
                "strict": True
            }
        },
        temperature=0.0
    )

    response_json = json.loads(response.output_text)
    parties = response_json.get("parties", [])

    return parties