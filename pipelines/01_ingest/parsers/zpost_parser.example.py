import pandas as pd

ZPOST_HEADERS = [
    "Position", "Post Title", "Position Pay Group", "Post Type", "Post Type Description",
    "Fund Code", "Description of Fund Code", "Functional Area", "Description of Functional Area", "Reporting number", "Personnel Number",
    "Start date", "End Date", "Number of Days", "Year", "Organizational unit", "Org Unit Desc", "Business Area", "Business Area Description",
    "Personnel Area", "Location Text", "Location subarea",
    "Cost Center", "Standard Cost", "Transfer to Reserves", "Transfer to Fund",
    "Transfer Reserve", "Total Post Cost", "Other Costs", "Other Charges",
    "Local Cost", "Local Cost1", "Local Cost2 (Optional)",
    "Total Non Post Cost", "Total"
]

def is_match(first_line: str, headers: list) -> bool:
    """Detects ZPOST reports by checking the first 10 columns."""
    signature = ['Position', 'Post Title', 'PS Group', 'Post Type', 'Position Type', 
                 'Fund Code', 'Funding Type', 'EE group', 'Description', 'Functional Area']
    return headers[:10] == signature

def parse(first_line: str, headers: list, data: list, file_path) -> tuple:
    """Applies specific headers and cleans monetary formatting."""
    df = pd.DataFrame(data, columns=headers)
    
    if len(headers) == len(ZPOST_HEADERS):
        df.columns = ZPOST_HEADERS

    numeric_keywords = ["Amount", "Cost", "Total", "Reserves", "Fund", "Standard Cost"]
    for col in df.columns:
        if any(keyword in col for keyword in numeric_keywords):
             df[col] = df[col].astype(str).str.replace(',', '', regex=False)

    return "ZPOST", df