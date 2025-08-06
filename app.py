import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import uuid
import io

# Page configuration
st.set_page_config(
    page_title="Aptos Vesting Flow",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# ID GENERATOR CLASS
# ============================================================================

class UniqueIDGenerator:
    """Generates unique IDs that are never repeated across sessions"""
    
    def __init__(self):
        self.counter_file = "data/id_counter.json"
        self.load_counter()
    
    def load_counter(self):
        """Load the current counter from file, or start at 1 if file doesn't exist"""
        if 'id_counter' not in st.session_state:
            try:
                if os.path.exists(self.counter_file):
                    with open(self.counter_file, 'r') as f:
                        data = json.load(f)
                        st.session_state['id_counter'] = data.get('counter', 1)
                else:
                    st.session_state['id_counter'] = 1
            except:
                st.session_state['id_counter'] = 1
    
    def save_counter(self):
        """Save the current counter to file"""
        try:
            os.makedirs(os.path.dirname(self.counter_file), exist_ok=True)
            with open(self.counter_file, 'w') as f:
                json.dump({'counter': st.session_state['id_counter']}, f)
        except:
            pass  # If we can't save, we'll just continue with session state
    
    def get_next_id(self):
        """Get the next unique ID and increment counter"""
        current_id = st.session_state['id_counter']
        st.session_state['id_counter'] += 1
        self.save_counter()
        
        # Create a unique ID using timestamp and counter
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = f"VT{timestamp}{current_id:06d}"
        
        return unique_id

# Global ID generator instance
id_generator = UniqueIDGenerator()

# ============================================================================
# FILE PROCESSOR FUNCTIONS
# ============================================================================

def load_csv_file(uploaded_file):
    """Load and validate CSV file"""
    try:
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            return df
        return None
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None

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

def validate_wallets_list(df):
    """Validate Wallets List format"""
    required_columns = ['ID', 'Name']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        st.error(f"Missing required columns in Wallets List: {', '.join(missing_columns)}")
        return False
    
    return True

def validate_vesting_pairs(df):
    """Validate Vesting Wallet Pairs format"""
    required_columns = ['Beneficiary Wallet', 'Originating Wallet']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        st.error(f"Missing required columns in Vesting Wallet Pairs: {', '.join(missing_columns)}")
        return False
    
    return True

def validate_bitwave_file(df):
    """Validate Bitwave Transactions Export format"""
    required_columns = ['id', 'dateTime', 'walletId', 'amount']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        st.error(f"Missing required columns in Bitwave Export: {', '.join(missing_columns)}")
        return False
    
    return True

def process_stage_1(anchorage_df, wallets_df):
    """Stage 1: Vesting Outflows per Anchorage File"""
    try:
        st.write(f"Debug: Total rows in Anchorage file: {len(anchorage_df)}")
        # Show first few Type values to see exact formatting
st.write(f"Debug: First 10 Type values: {list(anchorage_df['Type'].head(10))}")
st.write(f"Debug: Unique transaction types: {sorted(anchorage_df['Type'].unique())}")
        
        # Show unique transaction types
        unique_types = anchorage_df['Type'].unique()
        st.write(f"Debug: Unique transaction types found: {list(unique_types)}")
        
        # Filter for Balance Adjustment transactions
        balance_adjustments = anchorage_df[anchorage_df['Type'] == 'Balance Adjustment'].copy()
        st.write(f"Debug: Found {len(balance_adjustments)} Balance Adjustment transactions")
        
        if balance_adjustments.empty:
            # Try case-insensitive match
            balance_adjustments = anchorage_df[anchorage_df['Type'].str.lower() == 'balance adjustment'].copy()
            st.write(f"Debug: Found {len(balance_adjustments)} balance adjustment transactions (case insensitive)")
        
        if balance_adjustments.empty:
            st.warning("No Balance Adjustment transactions found in the data.")
            return pd.DataFrame()
        
        # Parse dates
        balance_adjustments['End Time'] = pd.to_datetime(balance_adjustments['End Time'])
        balance_adjustments['Date'] = balance_adjustments['End Time'].dt.date
        
        # Group by date and destination address
        grouped = balance_adjustments.groupby(['Date', 'Destination Address']).agg({
            'Asset Quantity (Before Fee)': 'sum',
            'Value (USD)': 'sum'
        }).reset_index()
        
        st.write(f"Debug: After grouping, found {len(grouped)} unique date/address combinations")
        
        # Replace destination addresses with wallet names where possible
        grouped['Wallet Name'] = grouped['Destination Address'].copy()
        
        # Create wallets lookup dictionary (Address -> Name)
        if not wallets_df.empty and 'Addresses' in wallets_df.columns and 'Name' in wallets_df.columns:
            wallets_lookup = dict(zip(wallets_df['Addresses'], wallets_df['Name']))
            
            # Replace addresses with wallet names
            for addr, name in wallets_lookup.items():
                if pd.notna(addr):
                    grouped.loc[grouped['Destination Address'] == addr, 'Wallet Name'] = name
        
        # Reorder columns
        result = grouped[['Date', 'Wallet Name', 'Asset Quantity (Before Fee)', 'Value (USD)']]
        result = result.sort_values(['Date', 'Wallet Name'])
        
        st.write(f"Debug: Final result has {len(result)} rows")
        
        return result
        
    except Exception as e:
        st.error(f"Error in Stage 1 processing: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# STAGE PROCESSING FUNCTIONS
# ============================================================================

def process_stage_1(anchorage_df, wallets_df):
    """Stage 1: Vesting Outflows per Anchorage File"""
    try:
        # Filter for Balance Adjustment transactions
        balance_adjustments = anchorage_df[anchorage_df['Type'] == 'Balance Adjustment'].copy()
        
        if balance_adjustments.empty:
            st.warning("No Balance Adjustment transactions found in the data.")
            return pd.DataFrame()
        
        # Parse dates
        balance_adjustments['End Time'] = pd.to_datetime(balance_adjustments['End Time'])
        balance_adjustments['Date'] = balance_adjustments['End Time'].dt.date
        
        # Group by date and destination address
        grouped = balance_adjustments.groupby(['Date', 'Destination Address']).agg({
            'Asset Quantity (Before Fee)': 'sum',
            'Value (USD)': 'sum'
        }).reset_index()
        
        # Replace destination addresses with wallet names where possible
        grouped['Wallet Name'] = grouped['Destination Address'].copy()
        
        # Create wallets lookup dictionary (Address -> Name)
        if not wallets_df.empty and 'Addresses' in wallets_df.columns and 'Name' in wallets_df.columns:
            wallets_lookup = dict(zip(wallets_df['Addresses'], wallets_df['Name']))
            
            # Replace addresses with wallet names
            for addr, name in wallets_lookup.items():
                if pd.notna(addr):
                    grouped.loc[grouped['Destination Address'] == addr, 'Wallet Name'] = name
        
        # Reorder columns
        result = grouped[['Date', 'Wallet Name', 'Asset Quantity (Before Fee)', 'Value (USD)']]
        result = result.sort_values(['Date', 'Wallet Name'])
        
        return result
        
    except Exception as e:
        st.error(f"Error in Stage 1 processing: {str(e)}")
        return pd.DataFrame()

def get_withdrawal_account_id(wallet_name, wallets_df):
    """Get account ID for withdrawal row"""
    try:
        # Remove "Aptos" prefix if present
        search_name = wallet_name.replace("Aptos ", "") if wallet_name.startswith("Aptos ") else wallet_name
        search_name = search_name + " vesting tokens"
        
        # Search for matching name in wallets list
        match = wallets_df[wallets_df['Name'] == search_name]
        
        if match.empty:
            st.error(f"{wallet_name} is missing a vesting tokens wallet")
            return None
        
        return match.iloc[0]['ID']
        
    except Exception as e:
        st.error(f"Error finding withdrawal account ID for {wallet_name}: {str(e)}")
        return None

def get_deposit_account_id(wallet_name, wallets_df, vesting_pairs_df):
    """Get account ID for deposit row"""
    try:
        # Find originating wallet in vesting pairs
        originating_match = vesting_pairs_df[vesting_pairs_df['Originating Wallet'] == wallet_name]
        
        if originating_match.empty:
            st.error(f"No Originating Wallet Match in the Vesting Wallet Pairs table for {wallet_name}")
            return None
        
        beneficiary_wallet = originating_match.iloc[0]['Beneficiary Wallet']
        
        # Find beneficiary wallet in wallets list
        beneficiary_match = wallets_df[wallets_df['Name'] == beneficiary_wallet]
        
        if beneficiary_match.empty:
            st.error(f"No Beneficiary Wallet Match in the Wallets list for {beneficiary_wallet}")
            return None
        
        return beneficiary_match.iloc[0]['ID']
        
    except Exception as e:
        st.error(f"Error finding deposit account ID for {wallet_name}: {str(e)}")
        return None

def process_stage_2(stage1_df, wallets_df, vesting_pairs_df):
    """Stage 2: Creating Vesting Transfers to Beneficiary Wallets"""
    try:
        if stage1_df.empty:
            st.error("No Stage 1 data available for Stage 2 processing")
            return pd.DataFrame()
        
        output_rows = []
        
        for _, row in stage1_df.iterrows():
            date = row['Date']
            wallet_name = row['Wallet Name']
            amount = row['Asset Quantity (Before Fee)']
            cost = row['Value (USD)']
            
            # Format time as 12:00 PM
            time_formatted = datetime.combine(date, datetime.min.time().replace(hour=12)).strftime('%m/%d/%Y %H:%M:%S')
            
            # Get unique IDs for both rows
            withdrawal_id = id_generator.get_next_id()
            deposit_id = id_generator.get_next_id()
            
            # Create withdrawal row
            withdrawal_account_id = get_withdrawal_account_id(wallet_name, wallets_df)
            if withdrawal_account_id:
                blockchain_id = f"{withdrawal_account_id}.vestingdistribute.{date.strftime('%m%d%y')}"
                
                withdrawal_row = {
                    'id': withdrawal_id,
                    'remoteContactId': '',
                    'amount': amount,
                    'amountTicker': 'APT',
                    'cost': cost,
                    'costTicker': 'USD',
                    'fee': '',
                    'feeTicker': '',
                    'time': time_formatted,
                    'blockchainId': blockchain_id,
                    'memo': '',
                    'transactionType': 'withdrawal',
                    'accountId': withdrawal_account_id,
                    'contactId': '',
                    'categoryId': '',
                    'taxExempt': 'FALSE',
                    'tradeId': '',
                    'description': 'vesting distribution per Anchorage report',
                    'fromAddress': '',
                    'toAddress': '',
                    'groupId': ''
                }
                output_rows.append(withdrawal_row)
            
            # Create deposit row
            deposit_account_id = get_deposit_account_id(wallet_name, wallets_df, vesting_pairs_df)
            if deposit_account_id:
                blockchain_id = f"{deposit_account_id}.vestingdistribute.{date.strftime('%m%d%y')}"
                
                deposit_row = {
                    'id': deposit_id,
                    'remoteContactId': '',
                    'amount': amount,
                    'amountTicker': 'APT',
                    'cost': cost,
                    'costTicker': 'USD',
                    'fee': '',
                    'feeTicker': '',
                    'time': time_formatted,
                    'blockchainId': blockchain_id,
                    'memo': '',
                    'transactionType': 'deposit',
                    'accountId': deposit_account_id,
                    'contactId': '',
                    'categoryId': '',
                    'taxExempt': 'FALSE',
                    'tradeId': '',
                    'description': 'vesting distribution per Anchorage report',
                    'fromAddress': '',
                    'toAddress': '',
                    'groupId': ''
                }
                output_rows.append(deposit_row)
        
        return pd.DataFrame(output_rows)
        
    except Exception as e:
        st.error(f"Error in Stage 2 processing: {str(e)}")
        return pd.DataFrame()

def get_stage2_deposit_amount(stage2_df, account_id, date):
    """Get the deposit amount from Stage 2 for matching account and date"""
    try:
        # Filter for deposit transactions with matching account ID
        deposits = stage2_df[
            (stage2_df['transactionType'] == 'deposit') & 
            (stage2_df['accountId'] == account_id)
        ]
        
        if deposits.empty:
            return None
        
        # Find matching date (convert stage2 time to date)
        for _, deposit_row in deposits.iterrows():
            deposit_date = datetime.strptime(deposit_row['time'], '%m/%d/%Y %H:%M:%S').date()
            if deposit_date == date:
                return deposit_row['amount']
        
        return None
        
    except Exception as e:
        st.error(f"Error getting Stage 2 deposit amount: {str(e)}")
        return None

def calculate_bitwave_amount(bitwave_df, account_id, date, stage2_amount):
    """Calculate amount from Bitwave data based on criteria"""
    try:
        # Filter Bitwave data for matching wallet ID
        wallet_transactions = bitwave_df[bitwave_df['walletId'] == account_id]
        
        if wallet_transactions.empty:
            return None
        
        # Convert date to datetime for comparison
        base_date = datetime.combine(date, datetime.min.time())
        end_date = base_date + timedelta(days=10)
        
        # Filter for date range (after base date, within 10 days)
        wallet_transactions['dateTime'] = pd.to_datetime(wallet_transactions['dateTime'])
        date_filtered = wallet_transactions[
            (wallet_transactions['dateTime'] > base_date) & 
            (wallet_transactions['dateTime'] <= end_date)
        ]
        
        if date_filtered.empty:
            return None
        
        # Filter for amounts greater than Stage 2 deposit amount
        amount_filtered = date_filtered[date_filtered['amount'] > stage2_amount]
        
        if amount_filtered.empty:
            return None
        
        # Use the first matching transaction
        bitwave_amount = amount_filtered.iloc[0]['amount']
        calculated_amount = bitwave_amount - stage2_amount
        
        # Store the matched bitwave transaction for Stage 4
        if 'stage3_matched_transactions' not in st.session_state:
            st.session_state['stage3_matched_transactions'] = []
        
        st.session_state['stage3_matched_transactions'].append({
            'id': amount_filtered.iloc[0]['id'],
            'bitwave_amount': bitwave_amount,
            'stage2_amount': stage2_amount,
            'calculated_amount': calculated_amount
        })
        
        return calculated_amount
        
    except Exception as e:
        st.error(f"Error calculating Bitwave amount: {str(e)}")
        return None

def get_wallet_name_from_id(account_id, wallets_df):
    """Get wallet name from account ID"""
    try:
        match = wallets_df[wallets_df['ID'] == account_id]
        if not match.empty:
            return match.iloc[0]['Name']
        return f"Unknown Wallet ({account_id})"
    except:
        return f"Unknown Wallet ({account_id})"

def process_stage_3(stage1_df, stage2_df, bitwave_df, wallets_df, vesting_pairs_df):
    """Stage 3: Vesting Staking Rewards Import"""
    try:
        if stage1_df.empty:
            st.error("No Stage 1 data available for Stage 3 processing")
            return pd.DataFrame(), pd.DataFrame()
        
        output_rows = []
        display_rows = []
        
        for _, row in stage1_df.iterrows():
            date = row['Date']
            wallet_name = row['Wallet Name']
            
            # Get deposit account ID (same logic as Stage 2 deposit)
            account_id = get_deposit_account_id(wallet_name, wallets_df, vesting_pairs_df)
            if not account_id:
                continue
            
            # Find corresponding Stage 2 deposit amount
            stage2_deposit_amount = get_stage2_deposit_amount(stage2_df, account_id, date)
            if stage2_deposit_amount is None:
                continue
            
            # Calculate amount from Bitwave data
            calculated_amount = calculate_bitwave_amount(
                bitwave_df, account_id, date, stage2_deposit_amount
            )
            
            if calculated_amount is None or calculated_amount <= 0:
                continue
            
            # Format time as 12:00 PM
            time_formatted = datetime.combine(date, datetime.min.time().replace(hour=12)).strftime('%m/%d/%Y %H:%M:%S')
            
            # Get unique ID
            unique_id = id_generator.get_next_id()
            
            # Create blockchain ID
            blockchain_id = f"{account_id}.vestingstakingrewards.{date.strftime('%m%d%y')}"
            
            # Create output row
            output_row = {
                'id': unique_id,
                'remoteContactId': '',
                'amount': calculated_amount,
                'amountTicker': 'APT',
                'cost': '',
                'costTicker': '',
                'fee': '',
                'feeTicker': '',
                'time': time_formatted,
                'blockchainId': blockchain_id,
                'memo': '',
                'transactionType': 'deposit',
                'accountId': account_id,
                'contactId': 'nFc4OUI5w6wSa6zFKQVj.526',
                'categoryId': 'nFc4OUI5w6wSa6zFKQVj.265',
                'taxExempt': 'FALSE',
                'tradeId': '',
                'description': 'staking reward from vesting distribution per Anchorage report',
                'fromAddress': '',
                'toAddress': '',
                'groupId': ''
            }
            output_rows.append(output_row)
            
            # Create display row (get wallet name from account ID)
            display_wallet_name = get_wallet_name_from_id(account_id, wallets_df)
            display_row = {
                'Date': date,
                'Wallet Name': display_wallet_name,
                'Amount': calculated_amount
            }
            display_rows.append(display_row)
        
        return pd.DataFrame(output_rows), pd.DataFrame(display_rows)
        
    except Exception as e:
        st.error(f"Error in Stage 3 processing: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def process_stage_4():
    """Stage 4: Ignore synced in vesting/staking transactions"""
    try:
        if 'stage3_matched_transactions' not in st.session_state:
            st.warning("No Stage 3 transactions found. Please run Stage 3 first.")
            return pd.DataFrame()
        
        output_rows = []
        
        for transaction in st.session_state['stage3_matched_transactions']:
            output_rows.append({
                'transactionID': transaction['id'],
                'action': 'ignore'
            })
        
        return pd.DataFrame(output_rows)
        
    except Exception as e:
        st.error(f"Error in Stage 4 processing: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

def initialize_session_state():
    """Initialize session state variables"""
    if 'wallets_list' not in st.session_state:
        st.session_state['wallets_list'] = pd.DataFrame()
    if 'vesting_pairs' not in st.session_state:
        st.session_state['vesting_pairs'] = pd.DataFrame()
    if 'stage1_data' not in st.session_state:
        st.session_state['stage1_data'] = pd.DataFrame()
    if 'stage2_data' not in st.session_state:
        st.session_state['stage2_data'] = pd.DataFrame()
    if 'stage3_csv_data' not in st.session_state:
        st.session_state['stage3_csv_data'] = pd.DataFrame()
    if 'stage3_display_data' not in st.session_state:
        st.session_state['stage3_display_data'] = pd.DataFrame()
    if 'stage4_data' not in st.session_state:
        st.session_state['stage4_data'] = pd.DataFrame()

def load_initial_data():
    """Load initial reference data files"""
    try:
        # Load wallets list if exists
        if os.path.exists('data/wallets_list.csv'):
            wallets_df = pd.read_csv('data/wallets_list.csv')
            if not wallets_df.empty and len(wallets_df.columns) > 3:
                st.session_state['wallets_list'] = wallets_df
        
        # Load vesting pairs if exists  
        if os.path.exists('data/vesting_wallet_pairs.csv'):
            pairs_df = pd.read_csv('data/vesting_wallet_pairs.csv')
            if not pairs_df.empty and len(pairs_df.columns) >= 2:
                st.session_state['vesting_pairs'] = pairs_df
                
    except Exception as e:
        st.error(f"Error loading initial data: {str(e)}")

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application function"""
    initialize_session_state()
    load_initial_data()
    
    # Header
    st.title("🔄 Aptos Vesting Flow")
    st.markdown("Process Anchorage transaction reports through 4 stages to generate vesting and staking reward CSV outputs.")
    
    # Sidebar for file management
    with st.sidebar:
        st.header("📁 File Management")
        
        # Wallets List Management
        st.subheader("Wallets List")
        if not st.session_state['wallets_list'].empty:
            st.success(f"✅ Loaded ({len(st.session_state['wallets_list'])} wallets)")
        else:
            st.warning("⚠️ No wallets list loaded")
        
        uploaded_wallets = st.file_uploader("Update Wallets List", type=['csv'], key='wallets_upload')
        if uploaded_wallets:
            wallets_df = load_csv_file(uploaded_wallets)
            if wallets_df is not None and validate_wallets_list(wallets_df):
                st.session_state['wallets_list'] = wallets_df
                st.success("Wallets list updated successfully!")
                st.rerun()
        
        st.markdown("---")
        
        # Vesting Pairs Management
        st.subheader("Vesting Wallet Pairs")
        if not st.session_state['vesting_pairs'].empty:
            st.success(f"✅ Loaded ({len(st.session_state['vesting_pairs'])} pairs)")
        else:
            st.warning("⚠️ No vesting pairs loaded")
        
        uploaded_pairs = st.file_uploader("Update Vesting Wallet Pairs", type=['csv'], key='pairs_upload')
        if uploaded_pairs:
            pairs_df = load_csv_file(uploaded_pairs)
            if pairs_df is not None and validate_vesting_pairs(pairs_df):
                st.session_state['vesting_pairs'] = pairs_df
                st.success("Vesting wallet pairs updated successfully!")
                st.rerun()
    
    # Main content area with tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "Stage 1: Vesting Outflows", 
        "Stage 2: Vesting Transfers", 
        "Stage 3: Staking Rewards", 
        "Stage 4: Ignore List"
    ])
    
    # Stage 1 Tab
    with tab1:
        st.header("📊 Stage 1 - Vesting Outflows per Anchorage File")
        
        # File upload
        uploaded_anchorage = st.file_uploader("Upload Anchorage Transaction Report", type=['csv'], key='anchorage_upload')
        
        if uploaded_anchorage:
            anchorage_df = load_csv_file(uploaded_anchorage)
            
            if anchorage_df is not None and validate_anchorage_file(anchorage_df):
                if st.button("Process Stage 1", key='process_stage1'):
                    with st.spinner("Processing Stage 1..."):
                        stage1_result = process_stage_1(anchorage_df, st.session_state['wallets_list'])
                        st.session_state['stage1_data'] = stage1_result
                        st.success("Stage 1 processing completed!")
        
       # Display results - FIXED VERSION
        if 'stage1_data' in st.session_state and not st.session_state['stage1_data'].empty:
            st.subheader("📋 Stage 1 Results")
            st.dataframe(st.session_state['stage1_data'], use_container_width=True)
            
            # Download button
            create_download_link(
                st.session_state['stage1_data'], 
                f"stage1_vesting_outflows_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "📥 Download Stage 1 CSV"
            )
        
        # Debug info - remove this later
        if 'stage1_data' in st.session_state:
            st.write(f"Debug: Stage 1 data has {len(st.session_state['stage1_data'])} rows")
    
    # Stage 2 Tab
    with tab2:
        st.header("💸 Stage 2 - Creating Vesting Transfers to Beneficiary Wallets")
        
        if st.session_state['stage1_data'].empty:
            st.warning("⚠️ Please complete Stage 1 first")
        elif st.session_state['wallets_list'].empty:
            st.warning("⚠️ Please upload Wallets List")
        elif st.session_state['vesting_pairs'].empty:
            st.warning("⚠️ Please upload Vesting Wallet Pairs")
        else:
            if st.button("Process Stage 2", key='process_stage2'):
                with st.spinner("Processing Stage 2..."):
                    stage2_result = process_stage_2(
                        st.session_state['stage1_data'],
                        st.session_state['wallets_list'],
                        st.session_state['vesting_pairs']
                    )
                    st.session_state['stage2_data'] = stage2_result
                    st.success("Stage 2 processing completed!")
        
        # Display results
        if not st.session_state['stage2_data'].empty:
            st.subheader("📋 Stage 2 Results")
            st.info(f"Generated {len(st.session_state['stage2_data'])} transaction rows")
            st.dataframe(st.session_state['stage2_data'].head(10), use_container_width=True)
            
            if len(st.session_state['stage2_data']) > 10:
                st.info("Showing first 10 rows. Download CSV for complete data.")
            
            # Download button
            create_download_link(
                st.session_state['stage2_data'],
                f"stage2_vesting_transfers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "📥 Download Stage 2 CSV"
            )
    
    # Stage 3 Tab  
    with tab3:
        st.header("🏆 Stage 3 - Vesting Staking Rewards Import")
        
        # File upload for Bitwave
        uploaded_bitwave = st.file_uploader("Upload Bitwave Transactions Export", type=['csv'], key='bitwave_upload')
        
        if st.session_state['stage1_data'].empty:
            st.warning("⚠️ Please complete Stage 1 first")
        elif st.session_state['stage2_data'].empty:
            st.warning("⚠️ Please complete Stage 2 first")
        elif uploaded_bitwave is None:
            st.warning("⚠️ Please upload Bitwave Transactions Export")
        else:
            bitwave_df = load_csv_file(uploaded_bitwave)
            
            if bitwave_df is not None and validate_bitwave_file(bitwave_df):
                if st.button("Process Stage 3", key='process_stage3'):
                    with st.spinner("Processing Stage 3..."):
                        stage3_csv, stage3_display = process_stage_3(
                            st.session_state['stage1_data'],
                            st.session_state['stage2_data'],
                            bitwave_df,
                            st.session_state['wallets_list'],
                            st.session_state['vesting_pairs']
                        )
                        st.session_state['stage3_csv_data'] = stage3_csv
                        st.session_state['stage3_display_data'] = stage3_display
                        st.success("Stage 3 processing completed!")
        
        # Display results
        if not st.session_state['stage3_display_data'].empty:
            st.subheader("📋 Stage 3 Results")
            st.dataframe(st.session_state['stage3_display_data'], use_container_width=True)
            
            # Download buttons
            col1, col2 = st.columns(2)
            with col1:
                create_download_link(
                    st.session_state['stage3_display_data'],
                    f"stage3_display_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "📥 Download Display Table"
                )
            with col2:
                if not st.session_state['stage3_csv_data'].empty:
                    create_download_link(
                        st.session_state['stage3_csv_data'],
                        f"stage3_staking_rewards_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        "📥 Download Stage 3 CSV"
                    )
    
    # Stage 4 Tab
    with tab4:
        st.header("🚫 Stage 4 - Ignore Synced in Vesting/Staking Transactions")
        
        if 'stage3_matched_transactions' not in st.session_state or not st.session_state.get('stage3_matched_transactions'):
            st.warning("⚠️ Please complete Stage 3 first to generate transaction IDs")
        else:
            if st.button("Process Stage 4", key='process_stage4'):
                with st.spinner("Processing Stage 4..."):
                    stage4_result = process_stage_4()
                    st.session_state['stage4_data'] = stage4_result
                    st.success("Stage 4 processing completed!")
        
        # Display results
        if not st.session_state['stage4_data'].empty:
            st.subheader("📋 Stage 4 Results")
            st.dataframe(st.session_state['stage4_data'], use_container_width=True)
            
            # Download button
            create_download_link(
                st.session_state['stage4_data'],
                f"stage4_ignore_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "📥 Download Stage 4 CSV"
            )
    
    # Footer
    st.markdown("---")
    st.markdown("**💡 Tip:** Process stages in order (1→2→3→4) for best results. Upload reference files before starting.")

if __name__ == "__main__":
    main()