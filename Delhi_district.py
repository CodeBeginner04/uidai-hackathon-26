import requests
import pandas as pd
import time
from io import StringIO


# Complete dictionary of all states and their districts
STATE_DISTRICTS = {
   
    'Delhi': [
        'Central Delhi', 'East Delhi', 'Najafgarh', 'New Delhi', 'North Delhi',
        'North East','North East *', 'North East Delhi', 'North West Delhi', 'Shahdara',
        'South Delhi', 'South East Delhi', 'South West Delhi', 'West Delhi'
    ]
    
}


def download_state_district_data(state, district, api_key):
    """Download data for a specific state and district"""
    headers = {'accept': 'text/csv'}
    
    params = {
        'api-key': api_key,
        'format': 'csv',
        'offset': '0',
        'limit': '1000',
        'filters[state]': state,
        'filters[district]': district,
    }
    
    try:
        response = requests.get(
            'https://api.data.gov.in/resource/ecd49b12-3084-4521-8f7e-ca8bf72069ba',
            params=params,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error {response.status_code} for {state} - {district}")
            return None
    except Exception as e:
        print(f"Exception for {state} - {district}: {str(e)}")
        return None

def download_all_data(api_key):
    """Download data for all states and districts"""
    all_data = []
    total_combinations = sum(len(districts) for districts in STATE_DISTRICTS.values())
    current = 0
    
    print(f"Starting download for {total_combinations} state-district combinations...\n")
    
    for state, districts in STATE_DISTRICTS.items():
        print(f"\n{'='*60}")
        print(f"Processing State: {state} ({len(districts)} districts)")
        print(f"{'='*60}")
        
        for district in districts:
            current += 1
            print(f"[{current}/{total_combinations}] Downloading: {state} - {district}...", end=' ')
            
            csv_data = download_state_district_data(state, district, api_key)
            
            if csv_data:
                try:
                    df = pd.read_csv(StringIO(csv_data))
                    if not df.empty:
                        df['_state'] = state
                        df['_district'] = district
                        all_data.append(df)
                        print(f"✓ ({len(df)} records)")
                    else:
                        print("✓ (0 records)")
                except Exception as e:
                    print(f"✗ Parse error: {str(e)}")
            else:
                print("✗ Download failed")
            
            # Rate limiting - be respectful to the API
            time.sleep(0.5)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n{'='*60}")
        print(f"Download Complete!")
        print(f"{'='*60}")
        print(f"Total records downloaded: {len(combined_df)}")
        print(f"Total columns: {len(combined_df.columns)}")
        
        # Save to CSV
        output_file = 'delhi_all_districts_data.csv'

        combined_df.to_csv(output_file, index=False)
        print(f"\nData saved to: {output_file}")
        
        # Display summary statistics
        print(f"\nRecords per state:")
        print(combined_df['_state'].value_counts().to_string())
        
        return combined_df
    else:
        print("\nNo data was downloaded successfully.")
        return None

# Main execution
if __name__ == "__main__":
    API_KEY = '579b464db66ec23bdd0000013f472cd72ae444c244ff4c681a0fd0e7'
    
    print("India Data Gov - Bulk Data Download")
    print("="*60)
    
    result_df = download_all_data(API_KEY)
    
    if result_df is not None:
        print("\n" + "="*60)
        print("Sample of downloaded data:")
        print("="*60)
        print(result_df.head(10))
        print(f"\nShape: {result_df.shape}")
        print(f"\nColumns: {list(result_df.columns)}")