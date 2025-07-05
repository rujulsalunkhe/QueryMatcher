import sqlite3
import pandas as pd
import json
import re
from typing import Dict, List, Tuple, Any, Optional
from config import config

class SchemaAnalyzer:
    def __init__(self, db_file: str, table_name: str):
        self.db_file = db_file
        self.table_name = table_name
        self.schema_info = {}
        
    def analyze_schema(self) -> Dict:
        """Analyze database schema and identify column types and patterns"""
        conn = sqlite3.connect(self.db_file)
        
        # Get column information
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.table_name})")
        columns_info = cursor.fetchall()
        
        # Get sample data
        df = pd.read_sql_query(f"SELECT * FROM {self.table_name} LIMIT 1000", conn)
        
        schema = {
            'table_name': self.table_name,
            'columns': {},
            'primary_keys': [],
            'text_columns': [],
            'numeric_columns': [],
            'categorical_columns': [],
            'date_columns': [],
            'id_columns': [],
            'code_columns': []
        }
        
        for col_info in columns_info:
            col_name = col_info[1]
            col_type = col_info[2]
            is_pk = col_info[5]
            
            if is_pk:
                schema['primary_keys'].append(col_name)
            
            # Analyze column content
            col_analysis = self._analyze_column(df[col_name], col_name, col_type)
            schema['columns'][col_name] = col_analysis
            
            # Categorize columns
            if col_analysis['category'] == 'text':
                schema['text_columns'].append(col_name)
            elif col_analysis['category'] == 'numeric':
                schema['numeric_columns'].append(col_name)
            elif col_analysis['category'] == 'categorical':
                schema['categorical_columns'].append(col_name)
            elif col_analysis['category'] == 'date':
                schema['date_columns'].append(col_name)
            elif col_analysis['category'] == 'id':
                schema['id_columns'].append(col_name)
            elif col_analysis['category'] == 'code':
                schema['code_columns'].append(col_name)
        
        conn.close()
        self.schema_info = schema
        return schema
    
    def _analyze_column(self, series: pd.Series, col_name: str, col_type: str) -> Dict:
        """Analyze individual column characteristics"""
        analysis = {
            'name': col_name,
            'sql_type': col_type,
            'category': 'unknown',
            'patterns': [],
            'sample_values': [],
            'unique_count': 0,
            'null_count': 0,
            'avg_length': 0
        }
        
        # Basic stats
        analysis['unique_count'] = series.nunique()
        analysis['null_count'] = series.isnull().sum()
        
        # Get sample non-null values
        non_null_values = series.dropna()
        if len(non_null_values) > 0:
            analysis['sample_values'] = non_null_values.head(5).tolist()
            
            # Check if it's string data
            if non_null_values.dtype == 'object':
                str_values = non_null_values.astype(str)
                analysis['avg_length'] = str_values.str.len().mean()
                
                # Detect patterns and categories
                analysis['category'] = self._categorize_text_column(str_values, col_name)
                analysis['patterns'] = self._detect_patterns(str_values)
            
            # Check if it's numeric
            elif pd.api.types.is_numeric_dtype(non_null_values):
                analysis['category'] = 'numeric'
                
                # Check if it looks like an ID (sequential integers)
                if col_name.lower().endswith('id') or 'id' in col_name.lower():
                    analysis['category'] = 'id'
            
            # Check if it's datetime
            elif pd.api.types.is_datetime64_any_dtype(non_null_values):
                analysis['category'] = 'date'
        
        return analysis
    
    def _categorize_text_column(self, str_values: pd.Series, col_name: str) -> str:
        """Categorize text columns based on content and name"""
        col_lower = col_name.lower()
        
        # Check column name patterns
        if any(keyword in col_lower for keyword in ['id', 'key', 'number', 'code', 'sku']):
            if any(keyword in col_lower for keyword in ['description', 'desc', 'name', 'title']):
                return 'text'  # It's a descriptive field
            return 'id' if 'id' in col_lower else 'code'
        
        # Check for description/text fields
        if any(keyword in col_lower for keyword in ['description', 'desc', 'name', 'title', 'comment', 'note', 'detail']):
            return 'text'
        
        # Check content patterns
        sample_str = str_values.iloc[0] if len(str_values) > 0 else ""
        
        # Check for codes (alphanumeric patterns)
        if re.match(r'^[A-Z]{2,}-\d+$', sample_str):
            return 'code'
        
        # Check average length and unique values
        avg_len = str_values.str.len().mean()
        unique_ratio = str_values.nunique() / len(str_values)
        
        if avg_len > config.TEXT_COLUMNS_MIN_LENGTH and unique_ratio > 0.5:
            return 'text'
        elif str_values.nunique() < config.MAX_UNIQUE_VALUES_FOR_CATEGORICAL:
            return 'categorical'
        else:
            return 'text'
    
    def _detect_patterns(self, str_values: pd.Series) -> List[str]:
        """Detect common patterns in string data"""
        patterns = []
        sample_values = str_values.head(10).tolist()
        
        for value in sample_values:
            value_str = str(value)
            
            # Common patterns
            if re.match(r'^[A-Z]{2,}-\d+$', value_str):
                patterns.append('CODE-NUMBER')
            elif re.match(r'^\d{4}-\d{2}-\d{2}', value_str):
                patterns.append('DATE-YYYY-MM-DD')
            elif re.match(r'^[A-Z]{2,}\d+$', value_str):
                patterns.append('LETTERS-NUMBERS')
            elif re.match(r'^\d+$', value_str):
                patterns.append('NUMBERS-ONLY')
            elif re.match(r'^[A-Za-z\s]+$', value_str):
                patterns.append('LETTERS-ONLY')
        
        return list(set(patterns))
    
    def save_schema(self, filename: str = None):
        """Save schema analysis to JSON file"""
        if filename is None:
            filename = config.SCHEMA_FILE
        
        with open(filename, 'w') as f:
            json.dump(self.schema_info, f, indent=2, default=str)
    
    def load_schema(self, filename: str = None) -> Dict:
        """Load schema from JSON file"""
        if filename is None:
            filename = config.SCHEMA_FILE
        
        with open(filename, 'r') as f:
            self.schema_info = json.load(f)
        return self.schema_info
