import json
from typing import Dict, List
from config import config

class TemplateGenerator:
    def __init__(self, schema_info: Dict):
        self.schema = schema_info
        
    def generate_templates(self) -> List[Dict]:
        """Generate templates dynamically based on schema"""
        templates = []
        
        # Basic templates for all columns
        templates.extend(self._generate_basic_templates())
        
        # Specific templates for different column types
        templates.extend(self._generate_numeric_templates())
        templates.extend(self._generate_text_templates())
        templates.extend(self._generate_date_templates())
        templates.extend(self._generate_categorical_templates())
        
        # Generic templates
        templates.extend(self._generate_generic_templates())
        
        return templates
    
    def _generate_basic_templates(self) -> List[Dict]:
        """Generate basic CRUD templates"""
        templates = []
        
        for col_name in self.schema['columns'].keys():
            col_lower = col_name.lower().replace('_', ' ')
            
            # Get/Show templates
            templates.extend([
                {
                    "template": f"what is the {col_lower} of {{ITEM}}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                },
                {
                    "template": f"show {col_lower} of {{ITEM}}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                },
                {
                    "template": f"get {col_lower} for {{ITEM}}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                }
            ])
        
        return templates
    
    def _generate_numeric_templates(self) -> List[Dict]:
        """Generate templates for numeric columns"""
        templates = []
        
        for col_name in self.schema['numeric_columns']:
            col_lower = col_name.lower().replace('_', ' ')
            
            # Price/Cost specific templates
            if any(keyword in col_lower for keyword in ['price', 'cost', 'amount', 'value']):
                templates.extend([
                    {
                        "template": f"how much does {{ITEM}} cost",
                        "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                    },
                    {
                        "template": f"how much is {{ITEM}}",
                        "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                    },
                    {
                        "template": f"price of {{ITEM}}",
                        "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                    }
                ])
            
            # Quantity specific templates
            if any(keyword in col_lower for keyword in ['quantity', 'count', 'stock', 'amount']):
                templates.extend([
                    {
                        "template": f"how many {{ITEM}} do we have",
                        "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                    },
                    {
                        "template": f"available stock of {{ITEM}}",
                        "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                    },
                    {
                        "template": f"check stock of {{ITEM}}",
                        "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                    }
                ])
        
        return templates
    
    def _generate_text_templates(self) -> List[Dict]:
        """Generate templates for text columns"""
        templates = []
        
        for col_name in self.schema['text_columns']:
            col_lower = col_name.lower().replace('_', ' ')
            
            templates.extend([
                {
                    "template": f"describe {{ITEM}}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                },
                {
                    "template": f"tell me about {{ITEM}}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                }
            ])
        
        return templates
    
    def _generate_date_templates(self) -> List[Dict]:
        """Generate templates for date columns"""
        templates = []
        
        for col_name in self.schema['date_columns']:
            col_lower = col_name.lower().replace('_', ' ')
            
            templates.extend([
                {
                    "template": f"when was {{ITEM}} {col_lower.replace('date', '').strip()}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                }
            ])
        
        return templates
    
    def _generate_categorical_templates(self) -> List[Dict]:
        """Generate templates for categorical columns"""
        templates = []
        
        for col_name in self.schema['categorical_columns']:
            col_lower = col_name.lower().replace('_', ' ')
            
            templates.extend([
                {
                    "template": f"what {col_lower} is {{ITEM}}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                },
                {
                    "template": f"{{ITEM}} {col_lower}",
                    "sql": f"SELECT {col_name} FROM {self.schema['table_name']} WHERE {{COL}} = ?"
                }
            ])
        
        return templates
    
    def _generate_generic_templates(self) -> List[Dict]:
        """Generate generic templates"""
        return [
            {
                "template": "{ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            },
            {
                "template": "show me {ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            },
            {
                "template": "give me all details for {ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            },
            {
                "template": "tell me everything about {ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            },
            {
                "template": "show all information for {ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            },
            {
                "template": "details of {ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            },
            {
                "template": "information about {ITEM}",
                "sql": f"SELECT * FROM {self.schema['table_name']} WHERE {{COL}} = ?"
            }
        ]
    
    def save_templates(self, filename: str = None):
        """Save generated templates to JSON file"""
        if filename is None:
            filename = config.TEMPLATE_FILE
        
        templates = self.generate_templates()
        with open(filename, 'w') as f:
            json.dump(templates, f, indent=2)
        
        return templates