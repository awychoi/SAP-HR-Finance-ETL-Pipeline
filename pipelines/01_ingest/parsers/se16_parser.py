import re
import pandas as pd

def is_match(first_line: str, headers: list) -> bool:
    """Detects standard SE16 table dumps."""
    return "Table To Be Searched" in first_line

def parse(first_line: str, headers: list, data: list, file_path) -> tuple:
    """Parses SE16 table data and derives the table name."""
    match = re.search(r'Table To Be Searched\s+([A-Z0-9_]+)', first_line)
    table_name = match.group(1) if match else file_path.stem
    
    df = pd.DataFrame(data, columns=headers)
    return table_name, df