# Sample Data Generator for Assessment
# File: scripts/generate_data.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_sample_data(num_records=1200):
    """Generate sample advertising emissions data with realistic values and intentional quality issues."""
    
    # Set random seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Define realistic values based on actual data format
    domains = [
        'fox32chicago.com', 'cnn.com', 'espn.com', 'nytimes.com', 'washingtonpost.com',
        'usatoday.com', 'weather.com', 'yahoo.com', 'msn.com', 'reuters.com',
        'bloomberg.com', 'forbes.com', 'techcrunch.com', 'engadget.com', 'theverge.com'
    ]
    
    formats = ['banner', 'video', 'display', 'native', 'audio']
    countries = ['United States', 'United Kingdom', 'Germany', 'France', 'Japan', 
                'Canada', 'Australia', 'Brazil', 'India', 'China']
    ad_sizes = ['300x250', '728x90', '320x50', '160x600', '300x600', '970x250']
    devices = ['desktop', 'mobile', 'tablet']
    domain_coverage_types = ['modeled', 'measured']
    
    # Generate base data
    data = []
    start_date = datetime.now() - timedelta(days=30)
    
    for i in range(num_records):
        # Generate date in MM/DD/YYYY format
        date = start_date + timedelta(days=random.randint(0, 30))
        
        # Generate base emissions (smaller values to match actual data)
        ad_selection = np.random.gamma(2, 0.1) * 0.1  # 0.1-0.8 range
        creative_dist = np.random.gamma(2, 0.005) * 0.01  # 0.01-0.05 range  
        media_dist = np.random.gamma(2, 0.008) * 0.01  # 0.01-0.08 range
        
        # Calculate total (with some intentional errors for data quality testing)
        if random.random() < 0.05:  # 5% of records have incorrect totals
            total_emissions = ad_selection + creative_dist + media_dist + random.uniform(-0.01, 0.01)
        else:
            total_emissions = ad_selection + creative_dist + media_dist
        
        # Generate domain coverage type
        domain_coverage = random.choice(domain_coverage_types)
        
        # Introduce some null values
        domain = random.choice(domains) if random.random() > 0.01 else None
        country = random.choice(countries) if random.random() > 0.005 else None
        
        record = {
            'date': date.strftime('%m/%d/%Y'),  # MM/DD/YYYY format
            'domain': domain,
            'format': random.choice(formats),
            'country': country,
            'ad_size': random.choice(ad_sizes),
            'device': random.choice(devices),
            'adSelectionEmissions': round(ad_selection, 8),
            'creativeDistributionEmissions': round(creative_dist, 8),
            'mediaDistributionEmissions': round(media_dist, 8),
            'totalEmissions': round(total_emissions, 8),
            'domainCoverage': domain_coverage
        }
        
        data.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add some duplicate records for testing
    duplicates = df.sample(n=10).copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    
    return df

def save_sample_data():
    """Generate and save sample data to CSV."""
    # Create data directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    
    # Generate data
    df = generate_sample_data()
    
    # Save to CSV
    filepath = 'data/raw/advertising_emissions.csv'
    df.to_csv(filepath, index=False)
    
    print(f"Sample data generated and saved to {filepath}")
    print(f"Generated {len(df)} records")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Unique domains: {df['domain'].nunique()}")
    print(f"Null values: {df.isnull().sum().sum()}")
    
    # Show sample data
    print("\nSample records:")
    print(df.head())
    
    return df

if __name__ == "__main__":
    save_sample_data()
