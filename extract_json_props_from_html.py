import os
import uuid
import json
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
from openai import OpenAI

# ------------------- OpenAI Client -------------------
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ------------------- Logging -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ------------------- Paths -------------------
html_folder = Path("/opt/softwares/automations_and_data_pipelines/data/html_outputs")  # folder containing HTML files
json_schema_file = Path("labresult_schema.json")  # external JSON schema file
csv_output = Path(f"lab_results_{uuid.uuid4().hex}.csv")

# Load JSON schema from file
with open(json_schema_file, "r", encoding="utf-8") as f:
    json_schema = json.load(f)

# ------------------- LLM Extraction Function -------------------
def extract_lab_result_from_html(html_text: str) -> dict:
    """
    Uses GPT-4o-mini to extract LabResult JSON from HTML text.
    Returns a dictionary matching the JSON schema loaded from file.
    """
    system_prompt = (
        "You are an assistant that extracts structured lab result data "
        "from HTML medical reports. Return only valid JSON matching the following schema:\n"
        f"{json.dumps(json_schema, indent=2)}"
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Extract lab result from the following HTML:\n\n{html_text}"}
        ],
        response_format={"type": "json_object"},
        temperature=0
    )

    try:
        output_text = response.choices[0].message.content
        return json.loads(output_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from LLM output: {e}\nOutput: {output_text}")

# ------------------- Process HTML Files -------------------
html_files = list(html_folder.glob("*.html"))
logging.info(f"Found {len(html_files)} HTML files to process.")

# Flag to write header only once
write_header = True

for html_file in html_files:
    try:
        logging.info(f"Processing file: {html_file.name}")
        html_text = html_file.read_text(encoding="utf-8")

        # Extract JSON via GPT-4o-mini
        json_record = extract_lab_result_from_html(html_text)

        # Create DataFrame for current row
        df_row = pd.DataFrame([{
            "patient_name": json_record.get("patient_name", ""),
            "json_record": json.dumps(json_record, ensure_ascii=False)
        }])

        # Append to CSV incrementally
        df_row.to_csv(csv_output, mode="a", index=False, header=write_header, encoding="utf-8")
        write_header = False  # after first write, don't write header again

        logging.info(f"Successfully processed {html_file.name}")

    except Exception as e:
        logging.error(f"Error processing {html_file.name}: {e}", exc_info=True)

logging.info(f"All files processed. Results saved to {csv_output}")
