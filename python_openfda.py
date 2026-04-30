import requests
import json
import csv
import time
import logging
from collections import Counter
from typing import List, Dict, Any

# ==========================================
# Configuration and Setup
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

API_URL = "https://api.fda.gov/drug/event.json"
DRUG_NAME = "Furosemide"
TARGET_RECORD_COUNT = 500
PAGE_LIMIT = 100  # Max limit per request without an API key is 100
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds

# ==========================================
# Helper Functions
# ==========================================
def fetch_fda_page(skip: int, limit: int) -> Dict[str, Any]:
    """
    Fetches a single page of results from the OpenFDA API.
    Handles HTTP errors and implements a retry mechanism for transient failures.
    """
    params = {
        'search': f'patient.drug.medicinalproduct:"{DRUG_NAME}"',
        'limit': limit,
        'skip': skip
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching records skip={skip}, limit={limit} (Attempt {attempt+1}/{MAX_RETRIES})")
            response = requests.get(API_URL, params=params, timeout=10)
            
            if response.status_code == 429:
                logger.warning("Rate limit hit. Sleeping before retry...")
                time.sleep(RETRY_DELAY * 2)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            # OpenFDA returns 404 if skip is beyond available results
            if response.status_code == 404:
                logger.info("No more records found (404). Reached end of available data.")
                return {}
            logger.error(f"HTTP Error: {e}")
            time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            time.sleep(RETRY_DELAY)
            
    logger.error("Max retries exceeded.")
    return {}

def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parses and normalizes a nested OpenFDA JSON record into a flat dictionary.
    Handles missing or inconsistent fields robustly.
    """
    patient = record.get('patient', {})
    
    # 1. Safety Report ID
    safety_report_id = record.get('safetyreportid', 'UNKNOWN')
    
    # 2. Seriousness
    seriousness = record.get('seriousness', 'Unknown')
    if seriousness == '1':
        seriousness = 'Serious'
    elif seriousness == '2':
        seriousness = 'Non-Serious'
        
    # 3. Report Date
    report_date = record.get('receiptdate', 'Unknown')
    if report_date != 'Unknown' and len(report_date) == 8:
        # Format YYYYMMDD to YYYY-MM-DD
        report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:]}"
        
    # 4. Patient Age
    patient_age = patient.get('patientonsetage', 'Unknown')
    
    # 5. Patient Sex (1=Male, 2=Female, 0/other=Unknown)
    sex_code = patient.get('patientsex', '0')
    sex_mapping = {'1': 'Male', '2': 'Female'}
    patient_sex = sex_mapping.get(str(sex_code), 'Unknown')
    
    # 6. Drug Name (Extract suspect drugs or just use target name if missing)
    drugs = patient.get('drug', [])
    drug_names = [d.get('medicinalproduct', '') for d in drugs if 'medicinalproduct' in d]
    drug_name = "; ".join(drug_names) if drug_names else DRUG_NAME
    
    # 7. Reaction (Adverse Events)
    reactions = patient.get('reaction', [])
    reaction_terms = [r.get('reactionmeddrapt', '') for r in reactions if 'reactionmeddrapt' in r]
    reaction_str = "; ".join(reaction_terms) if reaction_terms else 'Unknown'
    
    return {
        'safety_report_id': safety_report_id,
        'drug_name': drug_name,
        'reaction': reaction_str,
        'seriousness': seriousness,
        'patient_age': patient_age,
        'patient_sex': patient_sex,
        'report_date': report_date
    }

def deduplicate_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Removes duplicate records based on safety_report_id."""
    seen = set()
    unique_records = []
    for r in records:
        if r['safety_report_id'] not in seen:
            seen.add(r['safety_report_id'])
            unique_records.append(r)
    return unique_records

def analyze_data(records: List[Dict[str, Any]]) -> None:
    """Analyzes the cleaned records to generate basic insights."""
    if not records:
        logger.warning("No records to analyze.")
        return
        
    total_records = len(records)
    
    # 1. Summary Statistics: Seriousness
    serious_cases = sum(1 for r in records if r.get('seriousness') == 'Serious')
    serious_pct = (serious_cases / total_records) * 100
    
    # 2. Extract and count all reactions
    all_reactions = []
    for r in records:
        rx_str = r.get('reaction', '')
        if rx_str and rx_str != 'Unknown':
            # Split concatenated reactions
            all_reactions.extend([rx.strip() for rx in rx_str.split(';')])
            
    reaction_counts = Counter(all_reactions)
    unique_reactions = len(reaction_counts)
    top_5_reactions = reaction_counts.most_common(5)
    
    # 3. Analyze Age Groups
    age_groups = {'0-18': 0, '19-64': 0, '65+': 0, 'Unknown': 0}
    for r in records:
        age_str = r.get('patient_age', 'Unknown')
        if age_str != 'Unknown':
            try:
                age = float(age_str)
                if age <= 18:
                    age_groups['0-18'] += 1
                elif age <= 64:
                    age_groups['19-64'] += 1
                else:
                    age_groups['65+'] += 1
            except ValueError:
                age_groups['Unknown'] += 1
        else:
            age_groups['Unknown'] += 1
            
    # Print Output
    print("\n" + "="*50)
    print(" SUMMARY STATISTICS ")
    print("="*50)
    print(f"Total Records Analyzed: {total_records}")
    print(f"Serious Cases: {serious_cases} ({serious_pct:.1f}%)")
    print(f"Unique Reactions Identified: {unique_reactions}")
    
    print("\n" + "="*50)
    print(" INSIGHTS ")
    print("="*50)
    print("Top 5 Most Common Adverse Reactions:")
    for i, (rx, count) in enumerate(top_5_reactions, 1):
        print(f"  {i}. {rx} ({count} cases)")
        
    print("\nAge Group Distribution:")
    for group, count in age_groups.items():
        if count > 0:
            print(f"  {group} years: {count} cases")
            
    # Conclusion
    print("\n" + "="*50)
    print(" CONCLUSION ")
    print("="*50)
    top_reaction = top_5_reactions[0][0] if top_5_reactions else "None"
    print(f"Based on the {total_records} recent records for {DRUG_NAME}, a high proportion ({serious_pct:.1f}%) ")
    print(f"of reported cases are classified as serious. The most frequently reported adverse event is {top_reaction}, ")
    print("suggesting it is a predominant safety signal in the available dataset.")
    print("="*50 + "\n")


# ==========================================
# Main Execution
# ==========================================
def main():
    logger.info(f"Starting ingestion for drug: {DRUG_NAME}")
    
    all_raw_results = []
    normalized_data = []
    skip = 0
    
    # Pagination Loop
    while len(normalized_data) < TARGET_RECORD_COUNT:
        data = fetch_fda_page(skip=skip, limit=PAGE_LIMIT)
        
        results = data.get('results', [])
        if not results:
            logger.info("Empty results returned. Stopping ingestion.")
            break
            
        all_raw_results.extend(results)
        
        # Process records
        for record in results:
            flat_record = normalize_record(record)
            normalized_data.append(flat_record)
            
        skip += PAGE_LIMIT
        
        # Respect rate limits even on success
        time.sleep(0.5)
        
        # Check if fewer records were returned than requested
        if len(results) < PAGE_LIMIT:
            logger.info("Received fewer records than limit. End of dataset reached.")
            break

    # Truncate to exact target if we overshot
    if len(normalized_data) > TARGET_RECORD_COUNT:
        normalized_data = normalized_data[:TARGET_RECORD_COUNT]
        all_raw_results = all_raw_results[:TARGET_RECORD_COUNT]

    # Handle duplicates
    initial_count = len(normalized_data)
    normalized_data = deduplicate_records(normalized_data)
    if len(normalized_data) < initial_count:
        logger.info(f"Removed {initial_count - len(normalized_data)} duplicate records.")

    logger.info(f"Successfully processed {len(normalized_data)} unique records.")

    # ==========================================
    # Output File Generation
    # ==========================================
    # 1. Save Raw JSON
    raw_file = f"openfda_{DRUG_NAME}_raw.json"
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(all_raw_results, f, indent=2)
    logger.info(f"Saved raw API response to {raw_file}")

    # 2. Save Cleaned CSV
    if normalized_data:
        csv_file = f"openfda_{DRUG_NAME}_cleaned.csv"
        headers = list(normalized_data[0].keys())
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(normalized_data)
        logger.info(f"Saved normalized tabular data to {csv_file}")
    else:
        logger.warning("No data parsed to write to CSV.")

    # 3. Analyze Data
    logger.info("Starting data analysis...")
    analyze_data(normalized_data)

    logger.info("Ingestion pipeline completed successfully.")

if __name__ == "__main__":
    main()
