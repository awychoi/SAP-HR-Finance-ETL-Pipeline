import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DIR      = PROJECT_ROOT / "data" / "03_gold"
REPORTING_DIR = PROJECT_ROOT / "data" / "04_reporting"

REPORTING_DIR.mkdir(parents=True, exist_ok=True)

def extract_personnel_area_mismatches():
    print("Checking for Personnel Area Mismatches in Gold Layer...")

    # 1. Find the latest Gold file
    gold_files = sorted(GOLD_DIR.glob("gold_hr_financial_summary_*.parquet"))
    
    if not gold_files:
        print("Error: No Gold files found in the directory.")
        return

    latest_gold_file = gold_files[-1]
    
    conn = duckdb.connect()

    # 2. Get the schema of the Gold file to identify ALL date columns dynamically
    schema_df = conn.execute(f"DESCRIBE SELECT * FROM '{latest_gold_file}'").df()
    
    # Isolate columns that need to be cast to string (VARCHAR) to protect Pandas
    date_columns = schema_df[schema_df['column_type'] == 'DATE']['column_name'].tolist()
    
    # 3. Build a dynamic SELECT string that casts every date column to VARCHAR
    date_casts = [f'CAST("{col}" AS VARCHAR) AS "{col}"' for col in date_columns]
    date_cast_str = ",\n            ".join(date_casts)
    
    # Build a comma-separated string of date columns to EXCLUDE from the * wildcard
    date_excludes_str = ", ".join([f'"{col}"' for col in date_columns])

    # 4. Query DuckDB to find the mismatches
    mismatch_query = f"""
        SELECT 
            "Personnel Number",
            
            -- Dynamically cast ALL date columns to VARCHAR to prevent Pandas OutOfBounds errors
            {date_cast_str},
            
            "Actual Personnel Area" AS "PA0001_Personnel_Area",
            "Personnel area" AS "ZPOST_Personnel_Area",
            
            -- Bring in every other column dynamically, excluding the ones explicitly handled
            * EXCLUDE("Personnel Number", "Actual Personnel Area", "Personnel area", {date_excludes_str})
            
        FROM '{latest_gold_file}'
        
        WHERE 
            "Personnel Number" IS NOT NULL 
            AND TRIM("Personnel Number") != ''
            AND "Actual Personnel Area" != "Personnel area"
    """

    try:
        df_mismatches = conn.execute(mismatch_query).df()
        
        # 5. Check if there are any mismatches
        if df_mismatches.empty:
            print(f"Great news! No mismatches found in {latest_gold_file.name}.")
            return
            
        # 6. Save the mismatches to Excel
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = REPORTING_DIR / f"PA_Mismatches_{current_date}.xlsx"
        
        df_mismatches.to_excel(output_file, index=False, engine='openpyxl')
        
        print(f"Found {len(df_mismatches)} mismatched records!")
        print(f"Data successfully exported to: {output_file.name}")

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please verify the column names for ZPOST Personnel Area in your Gold table.")
    finally:
        conn.close()

if __name__ == "__main__":
    extract_personnel_area_mismatches()
