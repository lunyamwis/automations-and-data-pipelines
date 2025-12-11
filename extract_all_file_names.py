from pathlib import Path
from datetime import datetime

root_folder = Path("/media/martin/NO NAME/Removable Disk/")

# Collect docx files (excluding ~$ temp files)
docx_files = [
    f for f in root_folder.rglob("*.docx")
    if not f.name.startswith("~$")
]

# Create a unique filename using timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = Path(f"docx_file_list_{timestamp}.txt")

# Write results to the text file
with open(output_file, "w", encoding="utf-8") as f:
    for path in docx_files:
        f.write(str(path) + "\n")

print(f"Saved {len(docx_files)} file paths to {output_file}")
