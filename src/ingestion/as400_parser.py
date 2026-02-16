"""
AS400 Fixed-Width File Parser

Parses AS400 CPYTOIMPF-style fixed-width exports into pandas DataFrames.

Author: Vignesh
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import sys

import pandas as pd

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.file_layouts import FileLayout, get_layout, LAYOUTS
from src.utils.date_utils import cyymmdd_to_date, hhmmss_to_time
from src.utils.config import PHYSICAL_FILES_DIR, EXTRACTS_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AS400Parser:
    """Parser for AS400 fixed-width files."""
    
    def __init__(self, input_dir: Path = None):
        self.input_dir = Path(input_dir) if input_dir else PHYSICAL_FILES_DIR
        self.stats = {
            "files_processed": 0,
            "records_parsed": 0,
            "records_failed": 0,
            "parse_errors": []
        }
    
    def parse_file(self, filename: str, layout_name: Optional[str] = None) -> pd.DataFrame:
        """Parse an AS400 fixed-width file into a DataFrame."""
        filepath = self.input_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        if layout_name is None:
            layout_name = filename.upper().replace(".TXT", "")
        
        layout = get_layout(layout_name)
        
        print(f"  Parsing {filename} using layout {layout.name}")
        print(f"  Expected record length: {layout.get_total_width()} chars")
        
        records = []
        errors = []
        line_number = 0
        
        with open(filepath, 'r', encoding='ascii', errors='replace') as f:
            for line in f:
                line_number += 1
                line = line.rstrip('\r\n')
                
                if not line.strip():
                    continue
                
                try:
                    record = self._parse_record(line, layout)
                    records.append(record)
                except Exception as e:
                    errors.append({"line": line_number, "error": str(e)})
                    self.stats["records_failed"] += 1
                    if len(errors) <= 5:
                        logger.warning(f"Line {line_number}: {e}")
        
        df = pd.DataFrame(records)
        
        self.stats["files_processed"] += 1
        self.stats["records_parsed"] += len(records)
        self.stats["parse_errors"].extend(errors)
        
        print(f"  Parsed {len(records)} records, {len(errors)} errors")
        
        return df
    
    def _parse_record(self, line: str, layout: FileLayout) -> Dict[str, Any]:
        """Parse a single fixed-width record."""
        record = {}
        position = 0
        
        for as400_name, width, field_type, decimals, target_name in layout.fields:
            raw_value = line[position:position + width]
            position += width
            parsed_value = self._parse_field(raw_value, field_type, decimals)
            record[target_name] = parsed_value
        
        return record
    
    def _parse_field(self, raw_value: str, field_type: str, decimals: Optional[int]) -> Any:
        """Parse a single field value."""
        if field_type == "char":
            return raw_value.rstrip()
        
        elif field_type == "packed":
            value = raw_value.strip()
            if not value:
                return None
            try:
                if decimals and decimals > 0:
                    clean_value = value.replace(".", "")
                    if clean_value.lstrip("-").isdigit():
                        int_value = int(clean_value)
                        return round(int_value / (10 ** decimals), decimals)
                    else:
                        return float(value)
                else:
                    return int(float(value))
            except ValueError:
                return None
        
        elif field_type == "date":
            return cyymmdd_to_date(raw_value.strip())
        
        elif field_type == "time":
            return hhmmss_to_time(raw_value.strip())
        
        else:
            raise ValueError(f"Unknown field type: {field_type}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get parsing statistics."""
        return self.stats.copy()


def parse_all_files(input_dir: Path = None, output_dir: Path = None) -> Dict[str, pd.DataFrame]:
    """Parse all AS400 files and save to modern formats."""
    input_dir = input_dir or PHYSICAL_FILES_DIR
    output_dir = output_dir or EXTRACTS_DIR
    
    parser = AS400Parser(input_dir)
    results = {}
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\nInput directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    for layout_name in LAYOUTS.keys():
        filename = f"{layout_name}.txt"
        filepath = input_dir / filename
        
        if filepath.exists():
            print(f"\n{'-'*50}")
            print(f"Processing {filename}")
            print(f"{'-'*50}")
            
            try:
                df = parser.parse_file(filename)
                results[layout_name] = df
                
                parquet_path = output_dir / f"{layout_name.lower()}.parquet"
                df.to_parquet(parquet_path, index=False)
                print(f"  Saved: {parquet_path.name}")
                
                csv_path = output_dir / f"{layout_name.lower()}.csv"
                df.to_csv(csv_path, index=False)
                print(f"  Saved: {csv_path.name}")
                
                print(f"\n  Sample (first 2 rows):")
                print(df.head(2).to_string(index=False))
                
            except Exception as e:
                print(f"  FAILED: {e}")
        else:
            print(f"\n  File not found: {filepath}")
    
    stats = parser.get_stats()
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Records parsed:  {stats['records_parsed']}")
    print(f"Records failed:  {stats['records_failed']}")
    
    return results


if __name__ == "__main__":
    print("="*50)
    print("AS400 File Parser")
    print("="*50)
    
    dataframes = parse_all_files()
    
    print("\n" + "="*50)
    print("COMPLETE")
    print("="*50)
    for name, df in dataframes.items():
        print(f"  {name}: {len(df):,} records")