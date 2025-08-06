import uuid
import streamlit as st
from datetime import datetime
import os
import json

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
    
    def get_multiple_ids(self, count):
        """Get multiple unique IDs at once"""
        ids = []
        for _ in range(count):
            ids.append(self.get_next_id())
        return ids

# Create global instance
def get_id_generator():
    return UniqueIDGenerator()