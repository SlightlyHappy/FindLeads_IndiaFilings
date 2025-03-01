import asyncio
import os
import pandas as pd
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

async def process_company(company_name):
    # Form the search URL for the company name
    base_search_url = "https://www.indiafilings.com/check-company-name-availability/name?search_text="
    search_url = f"{base_search_url}{company_name}"
    
    async with AsyncWebCrawler() as crawler:
        # Crawl the search query page
        search_result = await crawler.arun(url=search_url)
        search_soup = BeautifulSoup(search_result.html, 'html.parser')
        
        # Find the anchor element that holds the detailed company information
        a_element = search_soup.find('a', {
            'class': 'block no-underline hover:no-underline trademark-card'
        })
        if not a_element:
            print(f"No detailed result found for {company_name}")
            return None
        
        # Extract the detailed URL from the href attribute
        detail_url = a_element.get('href')
        
        # Now visit the detailed company page
        detail_result = await crawler.arun(url=detail_url)
        detail_soup = BeautifulSoup(detail_result.html, 'html.parser')
        
        # Extract data from the first table
        table1 = detail_soup.find('table', {
            'class': 'card min-w-full table-border align-middle border-collapse text-sm'
        })
        data1 = {}
        if table1:
            for row in table1.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) == 2:
                    key = cols[0].text.strip()
                    value = cols[1].text.strip()
                    data1[key] = value
        df1 = pd.DataFrame([data1])
        
        # Extract data from the second table
        table2 = detail_soup.find('table', {
            'class': 'table table-row-dashed table-border align-middle fs-6 gy-4 my-0 pb-3'
        })
        
        # Initialize empty lists for each column
        din_pan_list = []
        name_list = []
        begin_date_list = []
        
        if table2:
            for row in table2.find_all('tr')[1:]:  # Skip the header row
                cols = row.find_all('td')
                if len(cols) == 3:
                    din_pan_list.append(cols[0].text.strip())
                    name_list.append(cols[1].text.strip())
                    begin_date_list.append(cols[2].text.strip())
        
        # Create a single row DataFrame with comma-separated values
        if din_pan_list:
            data2 = {
                'DIN/PAN': ', '.join(din_pan_list),
                'Name': ', '.join(name_list),
                'Begin Date': ', '.join(begin_date_list)
            }
            df2 = pd.DataFrame([data2])
        else:
            df2 = pd.DataFrame()
        
        # Combine the two DataFrames side by side
        final_df = pd.concat([df1, df2], axis=1)
        return final_df

async def main():
    # Read company names from companies.txt (one company name per line)
    try:
        with open('companies.txt', 'r') as file:
            companies = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print("The file 'companies.txt' was not found.")
        return

    output_csv = "companies_output.csv"
    header_written = False  # To track if the CSV header has been written

    for company in companies:
        print(f"Processing: {company}")
        df = await process_company(company)
        if df is not None:
            # Write the DataFrame immediately to a CSV file
            if not header_written:
                # Write in write mode the first time, including header
                df.to_csv(output_csv, index=False, mode='w')
                header_written = True
            else:
                # Append without writing the header for subsequent entries
                df.to_csv(output_csv, index=False, mode='a', header=False)
        else:
            print(f"Skipping {company} due to missing details.")
    
    print(f"Data extraction completed. Output written to {output_csv}.")

if __name__ == "__main__":
    asyncio.run(main())
