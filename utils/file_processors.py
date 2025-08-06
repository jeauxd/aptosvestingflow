import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import io

class FileProcessor:
    """Handles all file processing operations"""
    
    @staticmethod
    def load_csv_file(uploaded_file):
        """Load and validate CSV file"""
        try:
            if uploaded_file is not None:
                # Read the CSV file
                df = pd.read_csv(uploaded_file)
                return df
            return None
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
            return None
    
    @staticmethod
    def validate_anchorage_file(df):
        """Validate Anchorage Transaction Report format"""
        required_columns = [
            'End Time', 'Type', 'Asset Type', 'Asset Quantity (Before Fee)',
            'Value (USD)', 'Fee Quantity', 'Fee Value (USD)', 'Fee Asset Type',
            'Source Addresses', 'Destination Address'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Missing required columns: {', '.join(missing_columns)}")
            return False
        
        return True
    
    @staticmethod
    def validate_wallets_list(df):
        """Validate Wallets List format"""
        required_columns = ['ID', 'Name', 'Addresses']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Missing required columns in Wallets List: {', '.join(missing_columns)}")
            return False
        
        return True
    
    @staticmethod
    def validate_vesting_pairs(df):
        """Validate Vesting Wallet Pairs format"""
        required_columns = ['Beneficiary Wallet', 'Originating Wallet']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Missing required columns in Vesting Wallet Pairs: {', '.join(missing_columns)}")
            return False
        
        return True
    
    @staticmethod
    def validate_bitwave_file(df):
        """Validate Bitwave Transactions Export format"""
        required_columns = ['id', 'dateTime', 'walletId', 'amount']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Missing required columns in Bitwave Export: {', '.join(missing_columns)}")
            return False
        
        return True
    
    @staticmethod
    def create_download_link(df, filename, link_text="Download CSV"):
        """Create a download link for a DataFrame"""
        csv = df.to_csv(index=False)
        csv_bytes = csv.encode('utf-8')
        
        return st.download_button(
            label=link_text,
            data=csv_bytes,
            file_name=filename,
            mime='text/csv'
        )
    
    @staticmethod
    def parse_date(date_string):
        """Parse date string into datetime object"""
        try:
            # Handle different date formats
            formats = ['%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S']
            
            for fmt in formats:
                try:
                    return datetime.strptime(str(date_string), fmt)
                except ValueError:
                    continue
            
            # If none of the formats work, try pandas
            return pd.to_datetime(date_string)
            
        except Exception as e:
            st.warning(f"Could not parse date: {date_string}")
            return None
    
    @staticmethod
    def filter_date_range(df, date_column, start_date, end_date):
        """Filter DataFrame by date range"""
        try:
            df[date_column] = pd.to_datetime(df[date_column])
            
            mask = (df[date_column] >= start_date) & (df[date_column] <= end_date)
            return df[mask]
            
        except Exception as e:
            st.error(f"Error filtering by date range: {str(e)}")
            return df