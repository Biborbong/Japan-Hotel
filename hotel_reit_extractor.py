#!/usr/bin/env python3
"""
Complete Robust PDF Extractor for Japanese Hotel REITs
- JHR (Japan Hotel REIT) - Announcement of Monthly Disclosure
- Invincible Investment Corporation - Performance Update

Features:
- Year-based format routing for 10+ years of PDF layout drift
- YoY change extraction for both REITs
- NLP extraction for Invincible forecasts
- Incremental architecture for future runs
- Validation and self-correction
"""

import os
import re
import csv
import logging
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any
from dataclasses import dataclass, field, asdict
import pdfplumber

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('extraction.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_DIR = os.path.join(BASE_DIR, "pdfs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class JHRRecord:
    """Container for JHR extracted metrics."""
    date: str = ""
    occupancy: Optional[float] = None
    adr: Optional[float] = None
    revpar: Optional[float] = None
    revenue: Optional[float] = None
    occupancy_yoy: Optional[float] = None  # Percentage change vs same month
    adr_yoy: Optional[float] = None
    revpar_yoy: Optional[float] = None
    revenue_yoy: Optional[float] = None
    source_file: str = ""
    extraction_method: str = ""


@dataclass
class InvincibleRecord:
    """Container for Invincible extracted metrics."""
    date: str = ""
    occupancy: Optional[float] = None
    adr: Optional[float] = None
    revpar: Optional[float] = None
    revenue: Optional[float] = None
    occupancy_diff: Optional[float] = None  # Difference (pt or %)
    adr_diff: Optional[float] = None
    revpar_diff: Optional[float] = None
    revenue_diff: Optional[float] = None
    next_month_revpar_forecast: Optional[str] = None
    source_file: str = ""
    extraction_method: str = ""


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def normalize_occupancy(val: Optional[float]) -> Optional[float]:
    """Ensure occupancy is in 0-100 percent scale, not 0-1 decimal scale."""
    if val is None:
        return None
    if 0 < val <= 1.0:
        return round(val * 100, 1)
    return val


def clean_number(text: Any) -> Optional[float]:
    """Robust number cleaner."""
    if text is None:
        return None
    
    s = str(text).strip()
    
    if not s or s.lower() in ('n/a', 'na', '-', '--', '—', '', 'n.m.', 'nm', '―'):
        return None
    
    # Handle parentheses for negative numbers
    is_negative = False
    if '(' in s and ')' in s:
        is_negative = True
        s = s.replace('(', '').replace(')', '')
    
    # Remove commas, spaces, % signs
    s = s.replace(',', '').replace(' ', '').replace('%', '')
    
    # Keep only digits, dots, and minus
    s = re.sub(r'[^\d.\-]', '', s)
    
    if not s:
        return None
    
    try:
        value = float(s)
        return -value if is_negative else value
    except ValueError:
        return None


def parse_percentage_change(text: str) -> Optional[float]:
    """Parse percentage change, handling formats like '+2.5%', '-1.3%', '(2.3)%'."""
    if not text:
        return None
    
    text = str(text).strip()
    
    # Handle empty or special cases
    if not text or text in ('―', '-', '--', '—', 'n/a', 'na'):
        return None
    
    # Handle parentheses format: (2.3)% means -2.3%
    is_negative = False
    if '(' in text and ')' in text:
        is_negative = True
        text = text.replace('(', '').replace(')', '')
    
    # Remove % sign and spaces
    text = text.replace('%', '').replace(' ', '').strip()
    
    # Handle + prefix
    if text.startswith('+'):
        text = text[1:]
    elif text.startswith('-'):
        is_negative = True
        text = text[1:]
    
    try:
        value = float(text)
        return -value if is_negative else value
    except ValueError:
        return None


def parse_point_change(text: str) -> Optional[float]:
    """Parse point change, handling formats like '+0.5pt', '-1.2pt'."""
    if not text:
        return None
    
    text = str(text).strip()
    
    if not text or text in ('―', '-', '--', '—', 'n/a', 'na'):
        return None
    
    # Remove 'pt' suffix
    text = text.replace('pt', '').replace('PT', '').strip()
    
    # Handle parentheses
    is_negative = False
    if '(' in text and ')' in text:
        is_negative = True
        text = text.replace('(', '').replace(')', '')
    
    # Handle + prefix
    if text.startswith('+'):
        text = text[1:]
    elif text.startswith('-'):
        is_negative = True
        text = text[1:]
    
    try:
        value = float(text)
        return -value if is_negative else value
    except ValueError:
        return None


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract date in YYYY/MM format from filename."""
    match = re.search(
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})',
        filename, re.IGNORECASE
    )
    if match:
        month_str, year = match.groups()
        month_num = datetime.strptime(month_str[:3], '%b').month
        return f'{year}/{month_num:02d}'
    return None


def flatten_table(table: List) -> List[List[str]]:
    """Flatten a table by joining multi-line cells."""
    if not table:
        return []
    
    result = []
    for row in table:
        if row is None:
            continue
        flat_row = []
        for cell in row:
            if cell is None:
                flat_row.append('')
            else:
                # Replace newlines with spaces
                flat_row.append(str(cell).replace('\n', ' ').strip())
        result.append(flat_row)
    
    return result


# =============================================================================
# JHR EXTRACTOR
# =============================================================================

class JHRExtractor:
    """
    Extractor for Japan Hotel REIT Monthly Disclosure PDFs.
    
    Format evolution:
    - 2012-2017: First hotel's data or "Total of Three Hotel Groups"
    - 2018-2019: "Total of Three Hotel Groups" 
    - 2020+: "Total of the X Hotels with Variable Rent, etc."
    """
    
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.filename = os.path.basename(pdf_path)
        self.date = extract_date_from_filename(self.filename)
        self.year = int(self.date[:4]) if self.date else 2020
    
    def extract_text(self) -> str:
        """Extract all text from PDF."""
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                return "\n".join(page.extract_text() or "" for page in pdf.pages)
        except Exception as e:
            logger.error(f"Error reading {self.filename}: {e}")
            return ""
    
    def extract_tables(self) -> List:
        """Extract all tables from PDF."""
        tables = []
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page in pdf.pages:
                    page_tables = page.extract_tables()
                    if page_tables:
                        tables.extend(page_tables)
        except Exception as e:
            logger.error(f"Error extracting tables from {self.filename}: {e}")
        return tables
    
    def extract(self) -> JHRRecord:
        """Main extraction method."""
        record = JHRRecord()
        record.date = self.date or ""
        record.source_file = self.filename

        if not self.date:
            logger.warning(f"Could not extract date from {self.filename}")
            return record

        # Try table extraction first
        tables = self.extract_tables()
        if tables:
            record = self._extract_from_tables(tables, record)
            if record.revpar is not None:
                return self._validate_occupancy(record)

        # Fallback to text extraction
        text = self.extract_text()
        if text:
            # Check for broken text (spaced characters)
            if re.search(r'\d\s+\d\s*\.\s*\d\s*%', text):
                record = self._extract_broken_text(text, record)
                if record.revpar is not None:
                    # Revenue / YoY may still be missing — use word-position extraction
                    if record.revenue is None or record.adr_yoy is None \
                            or record.revpar_yoy is None:
                        record = self._extract_revenue_word_positions(record)
                    return self._validate_occupancy(record)

            record = self._extract_from_text(text, record)
            # For broken-text PDFs where _extract_broken_text didn't return early,
            # still run word-position extraction to fill missing YoY / occupancy_yoy.
            if re.search(r'\d\s+\d\s*\.\s*\d\s*%', text) and record.revpar is not None:
                if (record.occupancy_yoy is None or record.adr_yoy is None
                        or record.revpar_yoy is None or record.revenue_yoy is None):
                    record = self._extract_revenue_word_positions(record)

        return self._validate_occupancy(record)

    def _validate_occupancy(self, record: JHRRecord) -> JHRRecord:
        """Cross-validate occupancy against RevPAR/ADR. Corrects or infers implausible/missing values."""
        if record.adr and record.revpar:
            implied_occ = (record.revpar / record.adr) * 100
            if record.occupancy is None:
                if 20 <= implied_occ <= 100:
                    logger.info(
                        f"JHR {record.date}: Inferring occupancy {implied_occ:.1f}% from RevPAR/ADR"
                    )
                    record.occupancy = round(implied_occ, 1)
            elif abs(record.occupancy - implied_occ) > 20 and 20 <= implied_occ <= 100:
                logger.warning(
                    f"JHR {record.date}: Correcting occupancy {record.occupancy:.1f}% "
                    f"-> {implied_occ:.1f}% (RevPAR/ADR cross-check)"
                )
                record.occupancy = round(implied_occ, 1)
        return record
    
    def _extract_from_tables(self, tables: List, record: JHRRecord) -> JHRRecord:
        """Extract from PDF tables."""
        
        for table in tables:
            if not table:
                continue
            
            flat_table = flatten_table(table)
            
            # Look for the total row - multiple naming conventions
            for i, row in enumerate(flat_table):
                row_str = ' '.join(str(cell) for cell in row).lower()
                
                # Check for total row indicators
                is_total_row = False
                if 'total of' in row_str and 'hotel' in row_str:
                    is_total_row = True
                elif 'total of three hotel groups' in row_str:
                    is_total_row = True
                elif row_str.strip().startswith('total') and 'hotel' in row_str:
                    is_total_row = True
                
                if is_total_row:
                    # Look back up to 4 rows to capture Occupancy above the "Total of" label:
                    # - 2014-2016 format: label on last Revenue row; each hotel has 4 metric rows
                    #   (Occ, ADR, RevPAR, Revenue) so Occupancy is exactly i-4 from Revenue.
                    # - 2022-2025 format: label on ADR or Revenue row; Occupancy is i-1 to i-3.
                    # - 2017-2018 format: label on Occupancy row itself; i-4 still safe (starts earlier).
                    section_start = max(0, i - 4)
                    record = self._parse_total_section(flat_table[section_start:], record)
                    if record.revpar is not None:
                        record.extraction_method = "table_total"
                        return record
        
        # If no total row found, try to find metrics directly
        for table in tables:
            flat_table = flatten_table(table)
            record = self._parse_metrics_table(flat_table, record)
            if record.revpar is not None:
                record.extraction_method = "table_metrics"
                return record
        
        # Special handling for 2012 format - individual hotels with "Total Total Revenue" row
        for table in tables:
            flat_table = flatten_table(table)
            record = self._parse_2012_format(flat_table, record)
            if record.revpar is not None:
                record.extraction_method = "table_2012_format"
                return record
        
        return record
    
    def _parse_2012_format(self, table: List[List[str]], record: JHRRecord) -> JHRRecord:
        """Parse 2012 format PDFs which list individual hotels without consolidated total."""
        
        # Look for the first hotel's data
        for i, row in enumerate(table):
            row_str = ' '.join(str(cell) for cell in row).lower()
            
            # Look for first occupancy rate (misspelled as "Occupacy" in 2012)
            if 'occupacy rate' in row_str or ('occupancy' in row_str and 'rate' in row_str):
                values = [cell for cell in row if cell and cell.strip()]
                
                # Find the percentage and YoY
                for j, cell in enumerate(values):
                    if '%' in str(cell):
                        val = normalize_occupancy(clean_number(cell))
                        if val and 0 < val <= 100 and record.occupancy is None:
                            record.occupancy = val
                            if record.occupancy_yoy is None:
                                record.occupancy_yoy = self._find_first_pct(values, j + 1)
                            break

                # Look for ADR in next row (also extract YoY via _find_first_pct)
                if i + 1 < len(table):
                    adr_row = table[i + 1]
                    adr_str = ' '.join(str(cell) for cell in adr_row).lower()
                    if 'adr' in adr_str:
                        adr_vals = [c for c in adr_row if c and str(c).strip()]
                        for jj, cell in enumerate(adr_vals):
                            val = clean_number(cell)
                            if val and val > 1000 and record.adr is None:
                                record.adr = val
                                if record.adr_yoy is None:
                                    record.adr_yoy = self._find_first_pct(
                                        adr_vals, jj + 1, base_val=val)
                                break

                # Look for RevPAR in next row
                if i + 2 < len(table):
                    revpar_row = table[i + 2]
                    revpar_str = ' '.join(str(cell) for cell in revpar_row).lower()
                    if 'revpar' in revpar_str:
                        revpar_vals = [c for c in revpar_row if c and str(c).strip()]
                        for jj, cell in enumerate(revpar_vals):
                            val = clean_number(cell)
                            if val and val > 100 and record.revpar is None:
                                record.revpar = val
                                if record.revpar_yoy is None:
                                    record.revpar_yoy = self._find_first_pct(
                                        revpar_vals, jj + 1, base_val=val)
                                break

                # Look for Revenue in next row
                if i + 3 < len(table):
                    rev_row = table[i + 3]
                    rev_str = ' '.join(str(cell) for cell in rev_row).lower()
                    if 'revenue' in rev_str or 'total' in rev_str:
                        rev_vals = [c for c in rev_row if c and str(c).strip()]
                        for jj, cell in enumerate(rev_vals):
                            val = clean_number(cell)
                            if val and val > 50 and record.revenue is None:
                                record.revenue = val
                                if record.revenue_yoy is None:
                                    record.revenue_yoy = self._find_first_pct(
                                        rev_vals, jj + 1, base_val=val)
                                break

                break  # Only use first hotel
        
        return record
    
    def _find_first_pct(self, values: list, start: int, base_val: float = None) -> Optional[float]:
        """Scan values[start:] for the first cell with '%' and return parsed pct change.

        JHR table layout after the metric value is:
          [value, abs_fluctuation, pct_change%, cumulative_total, cum_abs_fluctuation, cum_pct%]
        The absolute fluctuation is a comma-number (no %) so it won't match here.
        COVID-era YoY can reach ±800%, so no tight abs() cap is applied.

        Fallback: for pre-2015 JHR tables, YoY is an ABSOLUTE fluctuation (no %).
        If no % cell is found and base_val is provided, compute YoY% from the
        first small numeric in values[start:start+2] via: fluc / (base - fluc) * 100.
        """
        for k in range(start, len(values)):
            cell = str(values[k])
            if '%' in cell:
                val = parse_percentage_change(cell)
                if val is not None and abs(val) < 1500:  # sanity: exclude obvious garbage
                    return val
        # Fallback: compute from absolute fluctuation (pre-2015 JHR format)
        if base_val and base_val > 50:
            for k in range(start, min(start + 3, len(values))):
                cell = values[k]
                cell_str = str(cell)
                if '%' in cell_str or not cell_str.strip():
                    continue
                fluc = clean_number(cell)
                if fluc is not None and abs(fluc) < base_val * 0.6 and abs(fluc) < 10000:
                    prior = base_val - fluc
                    if prior > 0:
                        return round(fluc / prior * 100, 2)
                    break
        return None

    def _parse_total_section(self, section: List[List[str]], record: JHRRecord) -> JHRRecord:
        """Parse the total section from a table."""

        for row in section:
            row_str = ' '.join(str(cell) for cell in row).lower()

            # Occupancy
            if 'occupancy' in row_str:
                values = [cell for cell in row if cell]
                for j, cell in enumerate(values):
                    if '%' in str(cell):
                        val = normalize_occupancy(clean_number(cell))
                        if val and 0 < val <= 100 and record.occupancy is None:
                            record.occupancy = val
                            # YoY % is at j+2 (j+1 = abs pt-diff), so scan from j+1
                            if record.occupancy_yoy is None:
                                record.occupancy_yoy = self._find_first_pct(values, j + 1)
                        break

            # ADR
            if 'adr' in row_str and 'jpy' in row_str:
                values = [cell for cell in row if cell]
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 1000 and record.adr is None:
                        record.adr = val
                        # YoY % is at j+2 (j+1 = abs fluctuation, no %)
                        # Pass base_val to enable absolute-fluctuation fallback (pre-2015)
                        if record.adr_yoy is None:
                            record.adr_yoy = self._find_first_pct(values, j + 1, base_val=val)
                        break

            # RevPAR
            if 'revpar' in row_str and 'jpy' in row_str:
                values = [cell for cell in row if cell]
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 100 and record.revpar is None:
                        record.revpar = val
                        if record.revpar_yoy is None:
                            record.revpar_yoy = self._find_first_pct(values, j + 1, base_val=val)
                        break

            # Revenue/Sales
            if ('revenue' in row_str or 'sales' in row_str) and 'jpy' in row_str:
                values = [cell for cell in row if cell]
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    # Revenue must be >50 million JPY to exclude YoY% or hotel-count columns
                    if val and val > 50 and record.revenue is None:
                        record.revenue = val
                        if record.revenue_yoy is None:
                            record.revenue_yoy = self._find_first_pct(values, j + 1, base_val=val)
                        break

        return record

    def _extract_revenue_word_positions(self, record: JHRRecord) -> JHRRecord:
        """
        Use word-position extraction for broken-text PDFs (2024+ format).
        Each character is a separate 'word' with its own x,y coordinate.
        Groups chars by y-bucket, sorts by x within each row, then reconstructs
        numbers from adjacent tokens.
        """
        def _reconstruct_pct(tokens: list, pct_idx: int) -> Optional[float]:
            """Given x-sorted token list and index of '%' token, reconstruct YoY%.

            Broken-text format: integer digits then decimal token then '%'.
            Example: ['1','4','.4','%'] → 14.4%
            We look back from '%': decimal token must start with '.', then
            up to 3 single-digit integer tokens.
            """
            if pct_idx < 2:
                return None
            decimal_tok = tokens[pct_idx - 1]
            if not decimal_tok.startswith('.'):
                # '%' token may itself contain the number, e.g. "14.4%"
                inner = tokens[pct_idx].rstrip('%')
                if re.match(r'^\d+\.\d+$', inner):
                    try:
                        return float(inner)
                    except ValueError:
                        pass
                return None
            # Decimal part validated (e.g. '.4')
            decimal_str = decimal_tok
            # Collect integer digits going back from pct_idx-2
            # Limit to 2 steps: JHR broken-text YoY values are always ≤ 2 digits (e.g. 99.9%)
            int_digits = []
            for look in range(1, 3):
                i = pct_idx - 1 - look
                if i < 0:
                    break
                t = tokens[i]
                if re.match(r'^\d$', t):      # single char digit only
                    int_digits.insert(0, t)
                else:
                    break
            int_str = ''.join(int_digits)
            if not int_str:
                return None
            try:
                return float(int_str + decimal_str)
            except ValueError:
                return None

        try:
            from collections import defaultdict
            with pdfplumber.open(self.pdf_path) as pdf:
                for page in pdf.pages[:2]:
                    raw_words = page.extract_words()
                    if not raw_words:
                        continue

                    # Group (x, text) pairs by rounded y-bucket, then sort by x
                    rows_by_y: dict = defaultdict(list)
                    for w in raw_words:
                        y = round(w['top'] / 3) * 3
                        rows_by_y[y].append((w['x0'], w['text']))

                    for y, xy_pairs in sorted(rows_by_y.items()):
                        tokens = [t for _, t in sorted(xy_pairs)]
                        collapsed = ''.join(tokens)
                        cl = collapsed.lower()

                        # ── Revenue: "Sales (JPY MM)" row ──────────────────
                        if record.revenue is None and 'sales' in cl and \
                                ('jpy' in cl or 'mm' in cl) and \
                                not any(kw in cl for kw in ('rooms', 'f&b', 'food', 'other')):
                            m = re.search(r'(\d{1,2},\d{3})', collapsed)
                            if m:
                                val = clean_number(m.group(1))
                                if val and 500 < val < 20000:
                                    record.revenue = val
                                    logger.info(f"JHR {record.date}: revenue={val} word-pos")

                        # Helper: find the first PERCENT token and reconstruct value
                        def _first_pct_in_row(tokens, cl, label_kw):
                            """Find first YoY% after the metric value in this row."""
                            if label_kw not in cl:
                                return None
                            # Find all % token positions
                            for wi, tok in enumerate(tokens):
                                if '%' in tok:
                                    pct_raw = _reconstruct_pct(tokens, wi)
                                    if pct_raw is not None and 0 < pct_raw < 300:
                                        return pct_raw
                            return None

                        # ── ADR YoY ────────────────────────────────────────
                        if record.adr_yoy is None and 'adr' in cl and 'jpy' in cl:
                            val = _first_pct_in_row(tokens, cl, 'adr')
                            if val is not None:
                                record.adr_yoy = val
                                logger.info(f"JHR {record.date}: adr_yoy={val} word-pos")

                        # ── RevPAR YoY ─────────────────────────────────────
                        if record.revpar_yoy is None and 'revpar' in cl and 'jpy' in cl:
                            val = _first_pct_in_row(tokens, cl, 'revpar')
                            if val is not None:
                                record.revpar_yoy = val
                                logger.info(f"JHR {record.date}: revpar_yoy={val} word-pos")

                        # ── Revenue YoY ────────────────────────────────────
                        if record.revenue is not None and record.revenue_yoy is None and \
                                'sales' in cl and ('jpy' in cl or 'mm' in cl) and \
                                not any(kw in cl for kw in ('rooms', 'f&b', 'food', 'other')):
                            val = _first_pct_in_row(tokens, cl, 'sales')
                            if val is not None:
                                record.revenue_yoy = val
                                logger.info(f"JHR {record.date}: revenue_yoy={val} word-pos")

                        # ── Occupancy (value + YoY) ─────────────────────────
                        # Broken-text occupancy row: purely numeric, no alphabetic label.
                        # First % in [20, 100] = occupancy value; second % = YoY pp-change.
                        if record.occupancy_yoy is None and '%' in collapsed and \
                                not any(c.isalpha() for c in collapsed):
                            pct_indices = [wi for wi, t in enumerate(tokens) if '%' in t]
                            occ_set = False
                            for pct_idx in pct_indices:
                                candidate = _reconstruct_pct(tokens, pct_idx)
                                if candidate is not None and 20 <= candidate <= 100:
                                    if record.occupancy is None:
                                        record.occupancy = candidate
                                        logger.info(
                                            f"JHR {record.date}: occupancy={candidate} word-pos")
                                    occ_set = True
                                elif occ_set and candidate is not None:
                                    record.occupancy_yoy = candidate
                                    logger.info(
                                        f"JHR {record.date}: occupancy_yoy={candidate} word-pos")
                                    break

        except Exception as e:
            logger.debug(f"Word-position extraction failed: {e}")
        return record

    def _extract_broken_text(self, text: str, record: JHRRecord) -> JHRRecord:
        """Extract from PDFs with broken/spaced character text (2024+ format)."""

        def fix_spaced_numbers(s):
            # Fix spaced percentages: "7 2 .8 %" -> "72.8%"
            s = re.sub(r'(\d)\s+(\d)\s*\.\s*(\d)\s*%', r'\1\2.\3%', s)
            # Fix spaced 5-digit comma numbers: "1 6 ,3 4 9" -> "16,349"
            s = re.sub(r'(\d)\s*(\d)\s*,\s*(\d)\s*(\d)\s*(\d)',
                       lambda m: m.group(0).replace(' ', ''), s)
            # Fix spaced 4-digit comma numbers: "1 ,8 9 2" -> "1,892"
            s = re.sub(r'(\d)\s*,\s*(\d)\s*(\d)\s*(\d)',
                       lambda m: m.group(0).replace(' ', ''), s)
            # Fix spaced decimals: "1 6 .3" -> "16.3"
            s = re.sub(r'(\d)\s+(\d)\s*\.\s*(\d)', r'\1\2.\3', s)
            return s

        fixed_text = fix_spaced_numbers(text)
        lines = fixed_text.split('\n')

        # Strategy 1: Find "Total of X Hotels" section in fixed text
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if 'total of' in line_lower and 'hotel' in line_lower:
                section = "\n".join(lines[max(0, i - 1):min(len(lines), i + 20)])

                # Occupancy: look for XX.X% in range 20-100 near section start
                occ_candidates = re.findall(r'\b(\d{2}\.?\d*)\s*%', section)
                for occ_str in occ_candidates:
                    try:
                        val = normalize_occupancy(float(occ_str))
                        if val and 20 <= val <= 100 and record.occupancy is None:
                            record.occupancy = val
                            break
                    except ValueError:
                        pass

                # ADR: look for comma-formatted 5-digit numbers after "ADR"
                adr_match = re.search(r'ADR[^J\n]*(?:JPY|Yen)[^0-9\n]*([\d,]+)',
                                      section, re.IGNORECASE)
                if not adr_match:
                    adr_match = re.search(r'ADR[^\n]*([\d]{2},[\d]{3})', section, re.IGNORECASE)
                if adr_match:
                    val = clean_number(adr_match.group(1))
                    if val and val > 1000:
                        record.adr = val

                # RevPAR: similar
                revpar_match = re.search(r'RevPAR[^J\n]*(?:JPY|Yen)[^0-9\n]*([\d,]+)',
                                         section, re.IGNORECASE)
                if not revpar_match:
                    revpar_match = re.search(r'RevPAR[^\n]*([\d]{1,2},[\d]{3})', section, re.IGNORECASE)
                if revpar_match:
                    val = clean_number(revpar_match.group(1))
                    if val and val > 100:
                        record.revpar = val

                if record.revpar is not None:
                    record.extraction_method = "broken_text_fix"
                    return record

        # Strategy 2: Scan fixed text for comma-formatted numbers (ADR, RevPAR)
        # These are correctly assembled after fix_spaced_numbers
        all_comma_nums = re.findall(r'\b(\d{1,2},\d{3})\b', fixed_text)
        for num_str in all_comma_nums:
            val = clean_number(num_str)
            if val and 5000 < val < 60000 and record.adr is None:
                record.adr = val
            elif val and 1000 < val < 30000 and record.adr and val < record.adr and record.revpar is None:
                record.revpar = val
            if record.adr and record.revpar:
                break

        # Strategy 3: Find occupancy in fixed text — prefer pct values in 30–100
        # range that do NOT appear on the same line as "RevPAR" (to avoid YoY prose)
        if record.occupancy is None:
            for line in fixed_text.split('\n'):
                if 'revpar' in line.lower() and ('%' in line) and 'occupan' not in line.lower():
                    continue  # Skip RevPAR YoY lines
                pct_match = re.search(r'\b(\d{2}\.?\d*)\s*%', line)
                if pct_match:
                    try:
                        val = normalize_occupancy(float(pct_match.group(1)))
                        if val and 20 <= val <= 100:
                            record.occupancy = val
                            break
                    except ValueError:
                        pass

        # Strategy 4: Severely broken PDFs — extract from standalone numeric lines.
        # Lines like "7 2 .8", "1 6 ,3", "1 1 ,8" have no words, just spaced digits.
        # After fix_spaced_numbers: "72.8", "16,3" (partial), "11,8" (partial).
        if record.occupancy is None or (record.adr is None and record.revpar is None):
            standalone_nums = []
            for line in lines:
                stripped = line.strip()
                # A line is "standalone numeric" if it only has digits, spaces, commas, dots
                if stripped and re.match(r'^[\d\s.,]+$', stripped) and any(c.isdigit() for c in stripped):
                    # Collapse spaces to get the number
                    num_str = stripped.replace(' ', '')
                    standalone_nums.append(num_str)

            for num_str in standalone_nums:
                # Check for decimal occupancy-style: "72.8" in 20-100 range
                if '.' in num_str and ',' not in num_str and record.occupancy is None:
                    try:
                        val = normalize_occupancy(float(num_str))
                        if val and 20 <= val <= 100:
                            record.occupancy = val
                            continue
                    except ValueError:
                        pass

                # Check for ADR/RevPAR partial (comma-separated, ends with comma or partial digit)
                # "16,3" → 16300 (pad with zeros), "11,8" → 11800
                comma_match = re.match(r'^(\d{1,2}),(\d+)$', num_str)
                if comma_match:
                    major = int(comma_match.group(1))
                    minor = comma_match.group(2)
                    # Reconstruct: pad minor part to 3 digits
                    minor_padded = minor.ljust(3, '0')[:3]
                    try:
                        val = float(f"{major}{minor_padded}")
                        if 5000 < val < 60000 and record.adr is None:
                            record.adr = val
                            record.extraction_method = "broken_text_partial"
                        elif 1000 < val < 30000 and record.adr and val < record.adr and record.revpar is None:
                            record.revpar = val
                    except ValueError:
                        pass

        if record.revpar is not None and not record.extraction_method:
            record.extraction_method = "broken_text_pattern"

        return record
    
    def _parse_metrics_table(self, table: List[List[str]], record: JHRRecord) -> JHRRecord:
        """Parse a metrics table looking for key patterns."""

        for row in table:
            row_str = ' '.join(str(cell) for cell in row).lower()
            values = [cell for cell in row if cell]

            if 'occupancy' in row_str:
                for j, cell in enumerate(values):
                    if '%' in str(cell):
                        val = normalize_occupancy(clean_number(cell))
                        if val and 0 < val <= 100 and record.occupancy is None:
                            record.occupancy = val
                            if record.occupancy_yoy is None:
                                record.occupancy_yoy = self._find_first_pct(values, j + 1)
                        break

            if 'adr' in row_str and 'jpy' in row_str:
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 1000 and record.adr is None:
                        record.adr = val
                        if record.adr_yoy is None:
                            record.adr_yoy = self._find_first_pct(values, j + 1, base_val=val)
                        break

            if 'revpar' in row_str and 'jpy' in row_str:
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 100 and record.revpar is None:
                        record.revpar = val
                        if record.revpar_yoy is None:
                            record.revpar_yoy = self._find_first_pct(values, j + 1, base_val=val)
                        break

            # Revenue/Sales — first parseable numeric > 50 is the monthly total
            if ('revenue' in row_str or 'sales' in row_str) and 'jpy' in row_str:
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 50 and record.revenue is None:
                        record.revenue = val
                        if record.revenue_yoy is None:
                            record.revenue_yoy = self._find_first_pct(values, j + 1, base_val=val)
                        break

        return record

    def _extract_from_text(self, text: str, record: JHRRecord) -> JHRRecord:
        """Extract from PDF text when tables fail."""
        
        # Normalize full-width parentheses
        text = text.replace('（', '(').replace('）', ')')
        
        lines = text.split('\n')
        
        # Strategy based on year
        if self.year >= 2020:
            return self._extract_text_2020_plus(text, lines, record)
        else:
            return self._extract_text_legacy(text, lines, record)
    
    def _extract_text_2020_plus(self, text: str, lines: List[str], record: JHRRecord) -> JHRRecord:
        """Extract from 2020+ format PDFs."""
        
        # Look for "Total of the X Hotels" section
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            if 'total of' in line_lower and 'hotel' in line_lower:
                # Get the section around this line
                section = "\n".join(lines[max(0, i-1):min(len(lines), i+20)])
                
                # Extract metrics using regex
                occ_match = re.search(r'Occupancy rate\s+([\d,]+\.?\d*)\s*%?', section, re.IGNORECASE)
                if occ_match:
                    val = clean_number(occ_match.group(1))
                    if val and 0 < val <= 100:
                        record.occupancy = val
                
                adr_match = re.search(r'ADR\s*\(JPY\)\s+([\d,]+)', section, re.IGNORECASE)
                if adr_match:
                    val = clean_number(adr_match.group(1))
                    if val and val > 1000:
                        record.adr = val
                
                revpar_match = re.search(r'RevPAR\s*\(JPY\)\s+([\d,]+)', section, re.IGNORECASE)
                if revpar_match:
                    val = clean_number(revpar_match.group(1))
                    if val and val > 100:
                        record.revpar = val
                
                rev_match = re.search(r'Revenues?\s*\(JPY[^)]*\)\s+([\d,]+)', section, re.IGNORECASE)
                if rev_match:
                    record.revenue = clean_number(rev_match.group(1))
                
                if record.revpar is not None:
                    record.extraction_method = "text_2020_plus"
                    return record
        
        # Fallback: look for individual metric lines
        return self._extract_text_metric_lines(lines, record)
    
    def _extract_text_legacy(self, text: str, lines: List[str], record: JHRRecord) -> JHRRecord:
        """Extract from legacy format (pre-2020)."""
        
        # Strategy 1: Find "Total of Three Hotel Groups" or similar
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            if 'total of' in line_lower and ('three hotel' in line_lower or 'hotel group' in line_lower):
                section = "\n".join(lines[max(0, i-1):min(len(lines), i+15)])
                
                occ_match = re.search(r'Occupancy rate\s+([\d,]+\.?\d*)\s*%?', section, re.IGNORECASE)
                if occ_match:
                    val = clean_number(occ_match.group(1))
                    if val and 0 < val <= 100:
                        record.occupancy = val
                
                adr_match = re.search(r'ADR\s*\(JPY\)\s+([\d,]+)', section, re.IGNORECASE)
                if adr_match:
                    val = clean_number(adr_match.group(1))
                    if val and val > 1000:
                        record.adr = val
                
                revpar_match = re.search(r'RevPAR\s*\(JPY\)\s+([\d,]+)', section, re.IGNORECASE)
                if revpar_match:
                    val = clean_number(revpar_match.group(1))
                    if val and val > 100:
                        record.revpar = val
                
                if record.revpar is not None:
                    record.extraction_method = "text_legacy_total"
                    return record
        
        # Strategy 2: Find first hotel's data
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            if ('occupancy rate' in line_lower or 'occupacy rate' in line_lower) and record.occupancy is None:
                match = re.search(r'Occupa?n?cy\s+Rate\s+([\d,]+\.?\d*)\s*%', line, re.IGNORECASE)
                if match:
                    val = clean_number(match.group(1))
                    if val and 0 < val <= 100:
                        record.occupancy = val
                        record.extraction_method = "text_legacy_first_hotel"
                        
                        for j in range(i+1, min(len(lines), i+6)):
                            if 'adr' in lines[j].lower() and 'jpy' in lines[j].lower():
                                adr_match = re.search(r'ADR\s*\(.*?\)\s+([\d,]+)', lines[j], re.IGNORECASE)
                                if adr_match:
                                    val = clean_number(adr_match.group(1))
                                    if val and val > 1000:
                                        record.adr = val
                                break
                        
                        for j in range(i+1, min(len(lines), i+6)):
                            if 'revpar' in lines[j].lower() and 'jpy' in lines[j].lower():
                                revpar_match = re.search(r'RevPAR\s*\(.*?\)\s+([\d,]+)', lines[j], re.IGNORECASE)
                                if revpar_match:
                                    val = clean_number(revpar_match.group(1))
                                    if val and val > 100:
                                        record.revpar = val
                                break
                        
                        if record.occupancy and record.adr and record.revpar:
                            return record
                        break
        
        return record
    
    def _extract_text_metric_lines(self, lines: List[str], record: JHRRecord) -> JHRRecord:
        """Extract by finding individual metric lines."""
        
        for line in lines:
            line_lower = line.lower()
            
            if 'occupancy rate' in line_lower and record.occupancy is None:
                match = re.search(r'([\d,]+\.?\d*)\s*%', line)
                if match:
                    val = normalize_occupancy(clean_number(match.group(1)))
                    if val and 0 < val <= 100:
                        record.occupancy = val

            if 'adr' in line_lower and 'jpy' in line_lower and record.adr is None:
                match = re.search(r'ADR\s*\(JPY\)\s+([\d,]+)', line, re.IGNORECASE)
                if match:
                    val = clean_number(match.group(1))
                    if val and val > 1000:
                        record.adr = val

            if 'revpar' in line_lower and 'jpy' in line_lower and record.revpar is None:
                match = re.search(r'RevPAR\s*\(JPY\)\s+([\d,]+)', line, re.IGNORECASE)
                if match:
                    val = clean_number(match.group(1))
                    if val and val > 100:
                        record.revpar = val

        if record.revpar is not None:
            record.extraction_method = "text_metric_lines"
        
        return record


# =============================================================================
# INVINCIBLE EXTRACTOR
# =============================================================================

class InvincibleExtractor:
    """
    Extractor for Invincible Investment Corporation Performance Update PDFs.
    
    Key features:
    - Hotel Properties portfolio data (various names across years)
    - YoY difference column
    - Next month RevPAR forecast in text (NLP extraction)
    """
    
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.filename = os.path.basename(pdf_path)
        self.date = extract_date_from_filename(self.filename)
        self.year = int(self.date[:4]) if self.date else 2020
    
    def extract_text(self) -> str:
        """Extract all text from PDF."""
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                return "\n".join(page.extract_text() or "" for page in pdf.pages)
        except Exception as e:
            logger.error(f"Error reading {self.filename}: {e}")
            return ""
    
    def extract_tables(self) -> List:
        """Extract all tables from PDF."""
        tables = []
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page in pdf.pages:
                    page_tables = page.extract_tables()
                    if page_tables:
                        tables.extend(page_tables)
        except Exception as e:
            logger.error(f"Error extracting tables from {self.filename}: {e}")
        return tables
    
    def extract(self) -> InvincibleRecord:
        """Main extraction method."""
        record = InvincibleRecord()
        record.date = self.date or ""
        record.source_file = self.filename
        
        if not self.date:
            logger.warning(f"Could not extract date from {self.filename}")
            return record
        
        # Extract text for NLP forecast
        text = self.extract_text()
        
        # Extract next month forecast from text
        record.next_month_revpar_forecast = self._extract_forecast_from_text(text)
        
        # Try table extraction
        tables = self.extract_tables()
        if tables:
            record = self._extract_from_tables(tables, record)
            if record.revpar is not None:
                return record
        
        # Fallback to text extraction
        if text:
            record = self._extract_from_text(text, record)
        
        return record
    
    def _extract_forecast_from_text(self, text: str) -> Optional[str]:
        """Extract next month RevPAR forecast from text."""
        if not text:
            return None
        
        # Pattern 1: Full sentence forecast (recent format)
        forecast_match = re.search(
            r'(?:forecasting|forecast|expect)[st]?\s+(?:that\s+)?(?:the\s+)?'
            r'(\w+\s+\d{4})\s+RevPAR\s+(?:will\s+be\s+)?(?:approximately\s+)?'
            r'([\d.]+)\s*%\s*(higher|lower)',
            text, re.IGNORECASE
        )
        if forecast_match:
            month_year = forecast_match.group(1)
            pct = forecast_match.group(2)
            direction = forecast_match.group(3)
            sign = '+' if direction.lower() == 'higher' else '-'
            return f"{month_year}: {sign}{pct}%"
        
        # Pattern 2: Short format forecast
        short_match = re.search(
            r'(\w+\s+\d{4}):\s*([+-][\d.]+)\s*%',
            text, re.IGNORECASE
        )
        if short_match:
            return f"{short_match.group(1)}: {short_match.group(2)}%"
        
        # Pattern 3: "February 2026: +2.0%" format
        pattern3 = re.search(
            r'(\w+\s+\d{4}):\s*([+-]?\s*[\d.]+)\s*%',
            text, re.IGNORECASE
        )
        if pattern3:
            return f"{pattern3.group(1)}: {pattern3.group(2)}%".replace(' ', '')
        
        return None
    
    def _extract_from_tables(self, tables: List, record: InvincibleRecord) -> InvincibleRecord:
        """Extract from PDF tables.

        Processing order:
        1. Tables with "Difference" column first — these contain YoY data.
        2. Remaining area/summary tables as fallback for base metrics only.
        """
        flat_tables = []
        for table in tables:
            if not table:
                continue
            flat = flatten_table(table)
            tstr = ' '.join(' '.join(row) for row in flat).lower()
            # Skip Cayman (USD) and residential-only tables
            if 'cayman' in tstr or 'usd' in tstr:
                continue
            if 'residential' in tstr and 'hotel' not in tstr:
                continue
            # Include tables with occupancy+ADR/RevPAR, OR RevPAR+Revenue
            # (the latter covers continuation tables split across pages)
            has_metrics = (
                ('occupancy' in tstr and ('adr' in tstr or 'revpar' in tstr)) or
                ('revpar' in tstr and ('revenue' in tstr or 'gross revenue' in tstr))
            )
            if has_metrics:
                flat_tables.append((flat, tstr))

        # Sort: tables with a YoY diff column come first
        def _has_diff_col(tstr: str) -> bool:
            return any(k in tstr for k in ('difference', '(a－b)', 'yoy change', 'yoy\nchange'))

        flat_tables.sort(key=lambda x: (0 if _has_diff_col(x[1]) else 1))

        for flat, _ in flat_tables:
            record = self._parse_hotel_table(flat, record)
            if record.revpar is not None:
                record.extraction_method = "table_hotel"
                # Only exit early if we have BOTH base metrics AND their diffs
                # (continuation tables may still provide RevPAR diff or Revenue)
                if (record.adr_diff is not None and record.revpar_diff is not None
                        and record.revenue is not None):
                    return record

        if record.revpar is not None:
            record.extraction_method = "table_hotel"
        return record
    
    def _parse_hotel_table(self, table: List[List[str]], record: InvincibleRecord) -> InvincibleRecord:
        """Parse hotel performance table."""
        
        # First, check if this is an area-based table (has "Total" row at bottom)
        # Format: Area | Occupancy Rate | ADR | RevPAR
        for row in table:
            row_str = ' '.join(str(cell) for cell in row).lower()
            if row_str.strip().startswith('total') and '%' in row_str:
                # This is an area table - extract from Total row
                values = [cell for cell in row if cell and cell.strip()]
                for j, cell in enumerate(values):
                    if '%' in str(cell):
                        val = normalize_occupancy(clean_number(cell))
                        if val and 0 < val <= 100 and record.occupancy is None:
                            record.occupancy = val
                    elif clean_number(cell) and clean_number(cell) > 1000 and record.adr is None:
                        record.adr = clean_number(cell)
                    elif clean_number(cell) and clean_number(cell) > 100 and record.revpar is None:
                        # Check if this could be RevPAR (less than ADR typically)
                        val = clean_number(cell)
                        if record.adr is None or val < record.adr:
                            record.revpar = val
                
                if record.revpar is not None:
                    record.extraction_method = "area_table_total"
                    return record
        
        # Standard row-by-row parsing
        for row in table:
            row_str = ' '.join(str(cell) for cell in row).lower()
            
            # Skip if this looks like a header row
            if 'same month' in row_str or 'previous year' in row_str or 'difference' in row_str:
                continue
            
            # Occupancy Rate row
            if 'occupancy' in row_str and 'rate' in row_str:
                values = [cell for cell in row if cell and cell.strip()]

                # Find the current month value (first percentage)
                for j, cell in enumerate(values):
                    if '%' in str(cell):
                        val = normalize_occupancy(clean_number(cell))
                        if val and 0 < val <= 100:
                            if record.occupancy is None:
                                record.occupancy = val
                            # Always try to extract diff even if occupancy already set
                            if record.occupancy_diff is None:
                                for k in range(j + 1, len(values)):
                                    diff_str = values[k]
                                    if 'pt' in str(diff_str).lower():
                                        diff = parse_point_change(diff_str)
                                        if diff is not None:
                                            record.occupancy_diff = diff
                                            break
                                    elif '%' in str(diff_str):
                                        diff = parse_percentage_change(diff_str)
                                        if diff is not None and abs(diff) < 1500:
                                            record.occupancy_diff = diff
                                            break
                        break
            
            # ADR row
            elif 'adr' in row_str and ('jpy' in row_str or 'yen' in row_str):
                values = [cell for cell in row if cell and cell.strip()]
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 1000:
                        if record.adr is None:
                            record.adr = val
                        # Always try to extract diff even if adr already set
                        if record.adr_diff is None:
                            for k in range(j + 1, len(values)):
                                if '%' in str(values[k]):
                                    diff = parse_percentage_change(values[k])
                                    if diff is not None and abs(diff) < 1500:
                                        record.adr_diff = diff
                                    break  # stop at first % cell regardless
                        break

            # RevPAR row
            elif 'revpar' in row_str and ('jpy' in row_str or 'yen' in row_str):
                values = [cell for cell in row if cell and cell.strip()]
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 100:
                        if record.revpar is None:
                            record.revpar = val
                        # Always try to extract diff even if revpar already set
                        if record.revpar_diff is None:
                            for k in range(j + 1, len(values)):
                                if '%' in str(values[k]):
                                    diff = parse_percentage_change(values[k])
                                    if diff is not None and abs(diff) < 1500:
                                        record.revpar_diff = diff
                                    break
                        break

            # Gross Revenue row — allow merged cells like "Gross Revenue\nRoom Revenue\n..."
            elif ('gross revenue' in row_str or 'revenue' in row_str) and 'jpy' in row_str and \
                    not row_str.strip().startswith(('room revenue', 'non-room revenue', 'f&b', 'food')):
                values = [cell for cell in row if cell and cell.strip()]
                for j, cell in enumerate(values):
                    val = clean_number(cell)
                    if val and val > 50 and record.revenue is None:
                        record.revenue = val
                        for k in range(j + 1, len(values)):
                            if '%' in str(values[k]):
                                diff = parse_percentage_change(values[k])
                                if diff is not None and abs(diff) < 1500:
                                    record.revenue_diff = diff
                                break
                        break
        
        return record
    
    def _extract_from_text(self, text: str, record: InvincibleRecord) -> InvincibleRecord:
        """Extract from PDF text when tables fail."""
        
        lines = text.split('\n')
        
        # Find hotel section
        in_hotel_section = False
        for i, line in enumerate(lines):
            line_lower = line.lower()
            
            # Check for hotel section start
            if 'hotel' in line_lower and ('property' in line_lower or 'portfolio' in line_lower):
                in_hotel_section = True
            
            # Check for end of hotel section
            if 'residential' in line_lower or 'cayman' in line_lower:
                in_hotel_section = False
            
            # Look for metrics
            if 'occupancy' in line_lower and record.occupancy is None:
                match = re.search(r'([\d,]+\.?\d*)\s*%', line)
                if match:
                    val = clean_number(match.group(1))
                    if val and 0 < val <= 100:
                        record.occupancy = val
            
            if 'adr' in line_lower and ('jpy' in line_lower or 'yen' in line_lower) and record.adr is None:
                match = re.search(r'([\d,]+)', line)
                if match:
                    val = clean_number(match.group(1))
                    if val and val > 1000:
                        record.adr = val
            
            if 'revpar' in line_lower and ('jpy' in line_lower or 'yen' in line_lower) and record.revpar is None:
                match = re.search(r'([\d,]+)', line)
                if match:
                    val = clean_number(match.group(1))
                    if val and val > 100:
                        record.revpar = val
        
        if record.revpar is not None:
            record.extraction_method = "text_extraction"
        
        return record


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def get_existing_dates(csv_path: str) -> set:
    """Get set of dates already in CSV."""
    if not os.path.exists(csv_path):
        return set()
    
    dates = set()
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'Date' in row and row['Date']:
                    dates.add(row['Date'])
    except Exception as e:
        logger.warning(f"Error reading existing CSV: {e}")
    
    return dates


def save_jhr_record(record: JHRRecord, csv_path: str):
    """Save a JHR record to CSV."""
    file_exists = os.path.exists(csv_path)
    
    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['Date', 'Occupancy_Rate_Pct', 'ADR_JPY', 'RevPAR_JPY', 'Revenue_JPY_Millions',
                      'Occupancy_YoY_Pct', 'ADR_YoY_Pct', 'RevPAR_YoY_Pct', 'Revenue_YoY_Pct',
                      'Extraction_Method', 'Source_File']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'Date': record.date,
            'Occupancy_Rate_Pct': record.occupancy if record.occupancy is not None else '',
            'ADR_JPY': record.adr if record.adr is not None else '',
            'RevPAR_JPY': record.revpar if record.revpar is not None else '',
            'Revenue_JPY_Millions': record.revenue if record.revenue is not None else '',
            'Occupancy_YoY_Pct': record.occupancy_yoy if record.occupancy_yoy is not None else '',
            'ADR_YoY_Pct': record.adr_yoy if record.adr_yoy is not None else '',
            'RevPAR_YoY_Pct': record.revpar_yoy if record.revpar_yoy is not None else '',
            'Revenue_YoY_Pct': record.revenue_yoy if record.revenue_yoy is not None else '',
            'Extraction_Method': record.extraction_method,
            'Source_File': record.source_file
        })


def save_invincible_record(record: InvincibleRecord, csv_path: str):
    """Save an Invincible record to CSV."""
    file_exists = os.path.exists(csv_path)
    
    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['Date', 'Occupancy_Rate_Pct', 'ADR_JPY', 'RevPAR_JPY', 'Revenue_JPY_Millions',
                      'Occupancy_Diff', 'ADR_Diff_Pct', 'RevPAR_Diff_Pct', 'Revenue_Diff_Pct',
                      'Next_Month_RevPAR_Forecast', 'Extraction_Method', 'Source_File']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'Date': record.date,
            'Occupancy_Rate_Pct': record.occupancy if record.occupancy is not None else '',
            'ADR_JPY': record.adr if record.adr is not None else '',
            'RevPAR_JPY': record.revpar if record.revpar is not None else '',
            'Revenue_JPY_Millions': record.revenue if record.revenue is not None else '',
            'Occupancy_Diff': record.occupancy_diff if record.occupancy_diff is not None else '',
            'ADR_Diff_Pct': record.adr_diff if record.adr_diff is not None else '',
            'RevPAR_Diff_Pct': record.revpar_diff if record.revpar_diff is not None else '',
            'Revenue_Diff_Pct': record.revenue_diff if record.revenue_diff is not None else '',
            'Next_Month_RevPAR_Forecast': record.next_month_revpar_forecast if record.next_month_revpar_forecast else '',
            'Extraction_Method': record.extraction_method,
            'Source_File': record.source_file
        })


def run_extraction(wipe_existing: bool = True, incremental: bool = False):
    """
    Run the full extraction pipeline.
    
    Args:
        wipe_existing: If True, delete existing CSVs and start fresh
        incremental: If True, only process PDFs not already in CSVs
    """
    
    jhr_csv = os.path.join(OUTPUT_DIR, "JHRTH_Extracted_Data.csv")
    inv_csv = os.path.join(OUTPUT_DIR, "Invincible_Extracted_Data.csv")
    
    # Wipe existing data if requested
    if wipe_existing:
        for csv_path in [jhr_csv, inv_csv]:
            if os.path.exists(csv_path):
                os.remove(csv_path)
                logger.info(f"Removed existing {csv_path}")
    
    # Get existing dates for incremental mode
    existing_jhr = get_existing_dates(jhr_csv) if incremental else set()
    existing_inv = get_existing_dates(inv_csv) if incremental else set()
    
    # Get all PDFs
    pdf_files = sorted([f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')])
    
    jhr_count = 0
    inv_count = 0
    jhr_failed = []
    inv_failed = []
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_DIR, pdf_file)
        
        # Determine if JHR or Invincible
        if 'Announcement of Monthly Disclosure' in pdf_file:
            # JHR PDF
            date = extract_date_from_filename(pdf_file)
            if incremental and date in existing_jhr:
                logger.info(f"Skipping {pdf_file} (already in CSV)")
                continue
            
            extractor = JHRExtractor(pdf_path)
            record = extractor.extract()
            
            if record.revpar is not None:
                save_jhr_record(record, jhr_csv)
                jhr_count += 1
                logger.info(f"Extracted JHR {record.date}: Occ={record.occupancy}%, ADR={record.adr}, RevPAR={record.revpar}")
            elif record.occupancy is not None and record.adr is not None:
                # Save partial record — at least occupancy and ADR are present
                record.extraction_method = (record.extraction_method or "partial") + "_no_revpar"
                save_jhr_record(record, jhr_csv)
                jhr_count += 1
                logger.info(f"Partial JHR {record.date}: Occ={record.occupancy}%, ADR={record.adr}, RevPAR=None")
            else:
                jhr_failed.append(pdf_file)
                logger.warning(f"Failed to extract RevPAR from {pdf_file}")
        
        elif 'Performance Update' in pdf_file:
            # Invincible PDF
            date = extract_date_from_filename(pdf_file)
            if incremental and date in existing_inv:
                logger.info(f"Skipping {pdf_file} (already in CSV)")
                continue
            
            extractor = InvincibleExtractor(pdf_path)
            record = extractor.extract()
            
            if record.revpar is not None:
                save_invincible_record(record, inv_csv)
                inv_count += 1
                logger.info(f"Extracted Invincible {record.date}: Occ={record.occupancy}%, ADR={record.adr}, RevPAR={record.revpar}, Forecast={record.next_month_revpar_forecast}")
            else:
                inv_failed.append(pdf_file)
                logger.warning(f"Failed to extract RevPAR from {pdf_file}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Extraction Complete!")
    logger.info(f"JHR: {jhr_count} records extracted, {len(jhr_failed)} failed")
    logger.info(f"Invincible: {inv_count} records extracted, {len(inv_failed)} failed")
    
    if jhr_failed:
        logger.warning(f"\nJHR Failed Files:")
        for f in jhr_failed:
            logger.warning(f"  - {f}")
    
    if inv_failed:
        logger.warning(f"\nInvincible Failed Files:")
        for f in inv_failed:
            logger.warning(f"  - {f}")
    
    return jhr_failed, inv_failed


def validate_csvs():
    """Validate the extracted CSVs for missing data."""
    
    jhr_csv = os.path.join(OUTPUT_DIR, "JHRTH_Extracted_Data.csv")
    inv_csv = os.path.join(OUTPUT_DIR, "Invincible_Extracted_Data.csv")
    
    issues = []
    
    # Validate JHR
    if os.path.exists(jhr_csv):
        with open(jhr_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                date = row.get('Date', '')
                
                if not row.get('Occupancy_Rate_Pct') or row.get('Occupancy_Rate_Pct') in ('', '0', '0.0'):
                    issues.append(f"JHR {date}: Missing Occupancy")
                
                if not row.get('ADR_JPY') or row.get('ADR_JPY') in ('', '0', '0.0'):
                    issues.append(f"JHR {date}: Missing ADR")
                
                if not row.get('RevPAR_JPY') or row.get('RevPAR_JPY') in ('', '0', '0.0'):
                    issues.append(f"JHR {date}: Missing RevPAR")
    
    # Validate Invincible
    if os.path.exists(inv_csv):
        with open(inv_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                date = row.get('Date', '')
                
                if not row.get('Occupancy_Rate_Pct') or row.get('Occupancy_Rate_Pct') in ('', '0', '0.0'):
                    issues.append(f"Invincible {date}: Missing Occupancy")
                
                if not row.get('ADR_JPY') or row.get('ADR_JPY') in ('', '0', '0.0'):
                    issues.append(f"Invincible {date}: Missing ADR")
                
                if not row.get('RevPAR_JPY') or row.get('RevPAR_JPY') in ('', '0', '0.0'):
                    issues.append(f"Invincible {date}: Missing RevPAR")
    
    if issues:
        logger.warning(f"\nValidation Issues Found:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("\nValidation Passed: No missing mandatory fields!")
    
    return issues


def sample_check(num_samples: int = 5):
    """Randomly check samples against source PDFs."""
    
    import random
    
    jhr_csv = os.path.join(OUTPUT_DIR, "JHRTH_Extracted_Data.csv")
    inv_csv = os.path.join(OUTPUT_DIR, "Invincible_Extracted_Data.csv")
    
    if os.path.exists(jhr_csv):
        with open(jhr_csv, 'r', encoding='utf-8') as f:
            reader = list(csv.DictReader(f))
            samples = random.sample(reader, min(num_samples, len(reader)))
            
            logger.info(f"\n{'='*60}")
            logger.info(f"JHR Sample Check:")
            
            for row in samples:
                date = row.get('Date', '')
                source = row.get('Source_File', '')
                logger.info(f"\n  Date: {date}")
                logger.info(f"  Source: {source}")
                logger.info(f"  Occupancy: {row.get('Occupancy_Rate_Pct')}%")
                logger.info(f"  ADR: {row.get('ADR_JPY')} JPY")
                logger.info(f"  RevPAR: {row.get('RevPAR_JPY')} JPY")
                logger.info(f"  Occupancy YoY: {row.get('Occupancy_YoY_Pct')}%")
                logger.info(f"  ADR YoY: {row.get('ADR_YoY_Pct')}%")
                logger.info(f"  RevPAR YoY: {row.get('RevPAR_YoY_Pct')}%")
    
    if os.path.exists(inv_csv):
        with open(inv_csv, 'r', encoding='utf-8') as f:
            reader = list(csv.DictReader(f))
            samples = random.sample(reader, min(num_samples, len(reader)))
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Invincible Sample Check:")
            
            for row in samples:
                date = row.get('Date', '')
                source = row.get('Source_File', '')
                logger.info(f"\n  Date: {date}")
                logger.info(f"  Source: {source}")
                logger.info(f"  Occupancy: {row.get('Occupancy_Rate_Pct')}%")
                logger.info(f"  ADR: {row.get('ADR_JPY')} JPY")
                logger.info(f"  RevPAR: {row.get('RevPAR_JPY')} JPY")
                logger.info(f"  Occupancy Diff: {row.get('Occupancy_Diff')}")
                logger.info(f"  ADR Diff: {row.get('ADR_Diff_Pct')}%")
                logger.info(f"  RevPAR Diff: {row.get('RevPAR_Diff_Pct')}%")
                logger.info(f"  Next Month Forecast: {row.get('Next_Month_RevPAR_Forecast')}")


def download_latest_pdfs(years: list = None) -> dict:
    """
    Download the latest JHR and Invincible PDFs that are not already on disk.

    Scrapes:
    - JHR:        https://www.jhrth.co.jp/en/ir/index-{YEAR}.html
    - Invincible: https://www.invincible-inv.co.jp/en/ir/index.html?year={YEAR}

    Returns a dict with keys "jhr_new", "inv_new", "errors".
    """
    import requests
    from bs4 import BeautifulSoup
    from datetime import datetime
    from urllib.parse import urljoin

    if years is None:
        current_year = datetime.now().year
        years = [current_year, current_year - 1]  # current + prev year for safety

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })

    results = {"jhr_new": [], "inv_new": [], "errors": []}

    # ── JHR ────────────────────────────────────────────────────────────────────
    for year in years:
        url = f"https://www.jhrth.co.jp/en/ir/index-{year}.html"
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"JHR {year} page fetch failed: {e}")
            results["errors"].append(f"JHR {year}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all("a", href=True):
            text = tag.get_text(strip=True)
            href = tag["href"]

            # Must be a direct PDF link (not an anchor / JS link)
            if not href.lower().endswith(".pdf"):
                continue

            # Check that it relates to monthly disclosure
            combined = (text + " " + href).lower()
            if not re.search(r"monthly.{0,30}disclosure|disclosure.{0,10}monthly", combined):
                continue

            # Derive filename from link text (e.g. "Announcement … for February 2026")
            m = re.search(
                r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})",
                text, re.IGNORECASE
            )
            if not m:
                # Try the surrounding <li> or <tr> parent for date context
                parent_text = ""
                for parent in tag.parents:
                    parent_text = parent.get_text(" ", strip=True)
                    if re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", parent_text, re.IGNORECASE):
                        break
                m = re.search(
                    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})",
                    parent_text, re.IGNORECASE
                )

            if m:
                fname = f"Announcement of Monthly Disclosure for {m.group(1).title()} {m.group(2)}.pdf"
            else:
                continue  # Can't determine month → skip to avoid garbage filenames

            dest = os.path.join(PDF_DIR, fname)
            if os.path.exists(dest):
                continue  # Already downloaded

            full_url = urljoin("https://www.jhrth.co.jp", href)
            try:
                dl = session.get(full_url, timeout=60)
                dl.raise_for_status()
                # Verify it is actually a PDF (starts with %PDF)
                if not dl.content[:4] == b"%PDF":
                    logger.warning(f"JHR: response for {fname} is not a PDF, skipping")
                    continue
                with open(dest, "wb") as fh:
                    fh.write(dl.content)
                logger.info(f"Downloaded JHR: {fname}")
                results["jhr_new"].append(fname)
            except Exception as e:
                logger.error(f"Failed to download JHR {fname}: {e}")
                results["errors"].append(f"JHR {fname}: {e}")

    # ── Invincible ─────────────────────────────────────────────────────────────
    for year in years:
        url = f"https://www.invincible-inv.co.jp/en/ir/index.html?year={year}"
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Invincible {year} page fetch failed: {e}")
            results["errors"].append(f"INV {year}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all("a", href=True):
            text = tag.get_text(strip=True)
            href = tag["href"]
            if "performance update" not in text.lower():
                continue

            m = re.search(
                r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})",
                text, re.IGNORECASE
            )
            if m:
                fname = f"Performance Update for {m.group(1).title()} {m.group(2)}.pdf"
            else:
                continue  # Can't determine month → skip

            dest = os.path.join(PDF_DIR, fname)
            if os.path.exists(dest):
                continue

            full_url = urljoin("https://www.invincible-inv.co.jp", href)
            try:
                dl = session.get(full_url, timeout=60)
                dl.raise_for_status()
                with open(dest, "wb") as fh:
                    fh.write(dl.content)
                logger.info(f"Downloaded Invincible: {fname}")
                results["inv_new"].append(fname)
            except Exception as e:
                logger.error(f"Failed to download INV {fname}: {e}")
                results["errors"].append(f"INV {fname}: {e}")

    logger.info(
        f"Download complete — JHR new: {len(results['jhr_new'])}, "
        f"INV new: {len(results['inv_new'])}, errors: {len(results['errors'])}"
    )
    return results


if __name__ == "__main__":
    import sys

    incremental = "--incremental" in sys.argv or "-i" in sys.argv
    skip_download = "--no-download" in sys.argv
    wipe = not incremental  # Wipe by default unless incremental mode

    logger.info("Starting Hotel REIT Data Extraction Pipeline")
    logger.info(f"Mode: {'Incremental' if incremental else 'Full Extract'}, Wipe Existing: {wipe}")

    # Step 0: Download any new PDFs from source websites
    if not skip_download:
        logger.info("Checking for new PDFs from source websites...")
        dl_results = download_latest_pdfs()
        if dl_results["jhr_new"] or dl_results["inv_new"]:
            logger.info(
                f"  New PDFs: {len(dl_results['jhr_new'])} JHR, "
                f"{len(dl_results['inv_new'])} Invincible"
            )

    jhr_failed, inv_failed = run_extraction(wipe_existing=wipe, incremental=incremental)
    issues = validate_csvs()
    sample_check(5)

    logger.info("\nPipeline Complete!")
