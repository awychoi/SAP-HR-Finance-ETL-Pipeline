import re

def extract_sap_raw_data(file_path):
    """Reads SAP txt file, strips page breaks, returns (first_line, headers, data)."""
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    if not lines:
        return "", [], []

    first_line = lines[0].strip()
    headers = []
    data = []
    is_header_parsed = False

    for line in lines:
        line = line.strip()
        if line.startswith('|') and line.endswith('|'):
            if re.match(r'^\|[-\s]+\|$', line): # Skip divider lines
                continue
                
            row_values = [cell.strip() for cell in line.split('|')[1:-1]]
            
            if not is_header_parsed:
                headers = row_values
                is_header_parsed = True
            else:
                if row_values == headers: # Skip repeating headers
                    continue
                data.append(row_values)

    return first_line, headers, data