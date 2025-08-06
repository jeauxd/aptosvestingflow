import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.id_generator import get_id_generator

class StageProcessor:
    """Handles all stage processing operations"""
    
    @staticmethod
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
    
    @staticmethod
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
                withdrawal_id = get_id_generator().get_next_id()
                deposit_id = get_id_generator().get_next_id()
                
                # Create withdrawal row
                withdrawal_account_id = StageProcessor._get_withdrawal_account_id(wallet_name, wallets_df)
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
                deposit_account_id = StageProcessor._get_deposit_account_id(wallet_name, wallets_df, vesting_pairs_df)
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
    
    @staticmethod
    def _get_withdrawal_account_id(wallet_name, wallets_df):
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
    
    @staticmethod
    def _get_deposit_account_id(wallet_name, wallets_df, vesting_pairs_df):
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
    
    @staticmethod
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
                account_id = StageProcessor._get_deposit_account_id(wallet_name, wallets_df, vesting_pairs_df)
                if not account_id:
                    continue
                
                # Find corresponding Stage 2 deposit amount
                stage2_deposit_amount = StageProcessor._get_stage2_deposit_amount(stage2_df, account_id, date)
                if stage2_deposit_amount is None:
                    continue
                
                # Calculate amount from Bitwave data
                calculated_amount = StageProcessor._calculate_bitwave_amount(
                    bitwave_df, account_id, date, stage2_deposit_amount
                )
                
                if calculated_amount is None or calculated_amount <= 0:
                    continue
                
                # Format time as 12:00 PM
                time_formatted = datetime.combine(date, datetime.min.time().replace(hour=12)).strftime('%m/%d/%Y %H:%M:%S')
                
                # Get unique ID
                unique_id = get_id_generator().get_next_id()
                
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
                display_wallet_name = StageProcessor._get_wallet_name_from_id(account_id, wallets_df)
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
    
    @staticmethod
    def _get_stage2_deposit_amount(stage2_df, account_id, date):
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
    
    @staticmethod
    def _calculate_bitwave_amount(bitwave_df, account_id, date, stage2_amount):
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
    
    @staticmethod
    def _get_wallet_name_from_id(account_id, wallets_df):
        """Get wallet name from account ID"""
        try:
            match = wallets_df[wallets_df['ID'] == account_id]
            if not match.empty:
                return match.iloc[0]['Name']
            return f"Unknown Wallet ({account_id})"
        except:
            return f"Unknown Wallet ({account_id})"
    
    @staticmethod
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