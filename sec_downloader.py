import requests
import json
import os
import re
from bs4 import BeautifulSoup, CData
import time
import pandas as pd
from io import StringIO
import copy

# --- Constants ---
HEADERS = {
    'User-Agent': 'MK Consulting mkenealy@outlook.com',
    'Accept-Encoding': 'gzip, deflate'
}
BASE_URL = "https://data.sec.gov"
FILING_URL = "https://www.sec.gov/Archives/edgar/data"
RATE_LIMIT_DELAY = 0.11


class SecEdgarScraper:
    """
    A class to find, download, and parse SEC filings.
    VERSION 11: Final robust version with iXBRL-aware table parsing.
    """
    def __init__(self):
        self.cik_map = self._get_cik_map()

    def _get_cik_map(self):
        print("Fetching the latest company CIK map from the SEC...")
        try:
            response = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS)
            response.raise_for_status()
            all_companies = response.json().values()
            cik_map = {company['title'].upper(): {'cik': str(company['cik_str']), 'title': company['title']} for company in all_companies}
            print(f"Successfully loaded CIK map for {len(cik_map)} companies.")
            return cik_map
        except Exception as e:
            print(f"Error fetching CIK map: {e}")
            return None

    def get_cik(self, company_name):
        if not self.cik_map:
            return None, None
        search_term = company_name.upper()
        matches = [data for name, data in self.cik_map.items() if search_term in name]
        
        if len(matches) == 1:
            found_company = matches[0]
            print(f"Found a match: '{found_company['title']}' (CIK: {found_company['cik']})")
            return found_company['title'], found_company['cik']
        elif len(matches) > 1:
            print(f"Found multiple potential matches for '{company_name}'. Please be more specific.")
            for company in matches[:10]:
                print(f"  - {company['title']}")
            return None, None
        else:
            print(f"Could not find any company matching '{company_name}'.")
            return None, None
    
    def get_filings(self, cik, company_name):
        print(f"\nFetching filings for {company_name} (CIK: {cik})...")
        padded_cik = cik.zfill(10)
        url = f"{BASE_URL}/submissions/CIK{padded_cik}.json"
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            filings_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve filing data for CIK {cik}: {e}")
            return

        safe_company_name = re.sub(r'[\\/*?:"<>|]', "", company_name).replace(" ", "_")
        save_path = os.path.abspath(os.path.join("sec_filings", safe_company_name))
        os.makedirs(save_path, exist_ok=True)
        print(f"Saving reports to: {save_path}")

        recent_filings = filings_data.get('filings', {}).get('recent', {})
        reports_to_download = []
        for i, form in enumerate(recent_filings.get('form', [])):
            if form in ['10-K', '10-Q']:
                reports_to_download.append({
                    'form': form,
                    'accession_number': recent_filings['accessionNumber'][i],
                    'date': recent_filings['filingDate'][i]
                })

        if not reports_to_download:
            print("No 10-K or 10-Q filings found.")
            return
            
        print(f"Found {len(reports_to_download)} 10-K/10-Q reports to process.")

        for report in reports_to_download:
            self._download_and_parse_report(cik, report, save_path, safe_company_name)

    def _extract_mda_text(self, soup, output_filename):
        mda_text = ""
        start_tag = None
        mda_pattern = re.compile(r"management.{1,5}s discussion and analysis", re.IGNORECASE)
        stop_pattern = re.compile(r"quantitative and qualitative disclosures about market risk|financial statements and supplementary data", re.IGNORECASE)

        potential_headers = soup.find_all(text=mda_pattern)
        for text_node in potential_headers:
            if not text_node.find_parent('a'):
                start_tag = text_node.find_parent()
                break
        
        if not start_tag:
            print("    - Could not find a valid MD&A section header.")
            return

        for elem in start_tag.find_all_next(['p', 'div', 'h1', 'h2', 'h3', 'h4']):
            if elem.name in ['h1', 'h2', 'h3', 'h4'] and stop_pattern.search(elem.get_text(strip=True)):
                break
            mda_text += elem.get_text(strip=True) + "\n\n"
        
        if mda_text:
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(mda_text)
            print(f"    - Successfully extracted MD&A to {output_filename}")
        else:
            print("    - MD&A section was found but no text could be extracted.")

    def _extract_tables_to_excel(self, soup, output_filename):
        tables = soup.find_all('table')
        if not tables:
            print("    - No <table> tags found in the document.")
            return
        
        valid_dfs = []
        for i, table in enumerate(tables):
            table_copy = copy.copy(table)

            for tag in table_copy.find_all(re.compile(r'.*:.*')):
                tag.replace_with(tag.get_text())

            for tag in table_copy.find_all(['div', 'span', 'p', 'font']):
                tag.unwrap()
            
            try:
                table_html = str(table_copy).replace('&nbsp;', ' ')
                html_io = StringIO(table_html)
                
                df_list = pd.read_html(html_io, flavor='bs4')
                
                if df_list:
                    df = df_list[0]
                    df.dropna(how='all', axis=0, inplace=True)
                    df.dropna(how='all', axis=1, inplace=True)
                    
                    if not df.empty and df.shape[0] > 1 and df.shape[1] > 0:
                        valid_dfs.append(df)
            except ValueError:
                continue
            except Exception as e:
                print(f"      - Could not parse table {i+1} due to error: {e}")
                continue
        
        if valid_dfs:
            try:
                with pd.ExcelWriter(output_filename) as writer:
                    for i, df in enumerate(valid_dfs):
                        df.to_excel(writer, sheet_name=f'Table_{i + 1}', index=False)
                print(f"    - Successfully extracted {len(valid_dfs)} tables to {output_filename}")
            except Exception as e:
                print(f"    - An error occurred while writing the Excel file: {e}")
        else:
            print("    - Found <table> tags, but none could be successfully parsed into data.")

    def _download_and_parse_report(self, cik, report_info, save_path, company_name_safe):
        form_type = report_info['form']
        accession_number = report_info['accession_number']
        filing_date = report_info['date']
        
        base_filename = os.path.join(save_path, f"{company_name_safe}_{form_type}_{filing_date}")
        mda_filename = f"{base_filename}_MDA.txt"
        tables_filename = f"{base_filename}_Tables.xlsx"

        if os.path.exists(mda_filename) and os.path.exists(tables_filename):
            print(f"  - Skipping {form_type} from {filing_date} (all files already exist).")
            return

        print(f"  + Processing {form_type} from {filing_date}...")
        
        accession_clean = accession_number.replace('-', '')
        filing_url = f"{FILING_URL}/{cik}/{accession_clean}/{accession_number}.txt"
        
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = requests.get(filing_url, headers=HEADERS)
            response.raise_for_status()
            full_text = response.text
        except requests.exceptions.RequestException as e:
            print(f"    - Failed to download filing: {e}")
            return

        doc_start_pattern = re.compile(r'<DOCUMENT>')
        doc_end_pattern = re.compile(r'</DOCUMENT>')
        type_pattern = re.compile(r'<TYPE>[^\n]+')
        main_doc_html = ""
        
        doc_start_indices = [m.end() for m in doc_start_pattern.finditer(full_text)]
        doc_end_indices = [m.start() for m in doc_end_pattern.finditer(full_text)]
        
        for i in range(len(doc_start_indices)):
            doc_content = full_text[doc_start_indices[i]:doc_end_indices[i]]
            type_match = type_pattern.search(doc_content)
            if type_match and form_type in type_match.group(0):
                if len(doc_content) > len(main_doc_html):
                    main_doc_html = doc_content
        
        if not main_doc_html:
            print(f"    - Could not find a suitable document of type {form_type} in the filing.")
            return

        soup = BeautifulSoup(main_doc_html, 'lxml')

        self._extract_mda_text(soup, mda_filename)
        self._extract_tables_to_excel(soup, tables_filename)


def main():
    scraper = SecEdgarScraper()
    if scraper.cik_map is None:
        print("Exiting due to failure in loading CIK map.")
        return
    print(f"\n--- Script is running in this directory: {os.getcwd()} ---")
    print("--- The 'sec_filings' folder will be created here. ---\n")
    while True:
        company_name_input = input("Enter the company name to fetch reports for (or type 'exit' to quit): ")
        if company_name_input.lower() == 'exit':
            break
        official_name, cik = scraper.get_cik(company_name_input)
        if cik:
            scraper.get_filings(cik, official_name)

if __name__ == "__main__":
    main()