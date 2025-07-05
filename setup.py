import os
import sys
import sqlite3
import pandas as pd
import re
from schema_analyzer import SchemaAnalyzer
from template_generator import TemplateGenerator
from config import config

def setup_system(csv_file: str = None, db_file: str = None, table_name: str = None):
    """
    Setup the generic query system with a new dataset
    """
    if not csv_file and not db_file:
        print("Error: Either csv_file or db_file must be provided")
        return False
    
    # Update config if provided
    if db_file:
        config.DB_FILE = db_file
    if table_name:
        config.TABLE_NAME = table_name
    
    print("Setting up Generic Query System...")
    
    # Step 1: Create database from CSV if needed
    if csv_file:
        print(f"1. Loading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        
        # Clean column names: lowercase, replace any non-alphanumeric with underscore,
        # then strip leading/trailing underscores, and ensure it doesn’t start with a digit.
        clean_cols = []
        for col in df.columns:
            # lowercase and replace sequences of non-word chars with underscore
            c = re.sub(r'\W+', '_', col.strip().lower())
            # remove leading/trailing underscores
            c = c.strip('_')
            # if it starts with a digit, prefix an underscore
            if re.match(r'^\d', c):
                c = '_' + c
            clean_cols.append(c)
        df.columns = clean_cols
        
        # Create SQLite database
        conn = sqlite3.connect(config.DB_FILE)
        df.to_sql(config.TABLE_NAME, conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"   Database created: {config.DB_FILE}")
        print(f"   Table created: {config.TABLE_NAME}")
        print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Step 2: Analyze schema
    print("2. Analyzing database schema...")
    analyzer = SchemaAnalyzer(config.DB_FILE, config.TABLE_NAME)
    schema = analyzer.analyze_schema()
    analyzer.save_schema()
    
    print(f"   Schema analysis complete")
    print(f"   Text columns: {len(schema['text_columns'])}")
    print(f"   Numeric columns: {len(schema['numeric_columns'])}")
    print(f"   Categorical columns: {len(schema['categorical_columns'])}")
    print(f"   ID/Code columns: {len(schema['id_columns']) + len(schema['code_columns'])}")
    
    # Step 3: Generate templates
    print("3. Generating query templates...")
    generator = TemplateGenerator(schema)
    templates = generator.save_templates()
    
    print(f"   Generated {len(templates)} templates")
    
    # Step 4: Test database connection
    print("4. Testing database connection...")
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {config.TABLE_NAME}")
        count = cursor.fetchone()[0]
        conn.close()
        print(f"   Database test successful: {count} records found")
    except Exception as e:
        print(f"   Database test failed: {e}")
        return False
    
    print("\n✅ Setup complete!")
    print("\nNext steps:")
    print("1. Start Redis server: redis-server")
    print("2. Run the application: python app.py")
    print("3. Test with: curl -X POST http://localhost:4000/query -H 'Content-Type: application/json' -d '{\"userInput\": \"your query here\"}'")
    
    return True

def show_schema_info():
    """Display schema information"""
    try:
        analyzer = SchemaAnalyzer(config.DB_FILE, config.TABLE_NAME)
        schema = analyzer.load_schema()
        
        print("\n📊 Schema Information:")
        print(f"Table: {schema['table_name']}")
        print(f"Total columns: {len(schema['columns'])}")
        
        print("\n📝 Column Categories:")
        for category in ['text_columns', 'numeric_columns', 'categorical_columns', 'id_columns', 'code_columns', 'date_columns']:
            columns = schema.get(category, [])
            if columns:
                print(f"  {category.replace('_', ' ').title()}: {', '.join(columns)}")
        
        print("\n🔍 Sample Data Patterns:")
        for col_name, col_info in schema['columns'].items():
            if col_info.get('sample_values'):
                print(f"  {col_name}: {col_info['sample_values'][:3]}")
    
    except FileNotFoundError:
        print("Schema file not found. Please run setup first.")
    except Exception as e:
        print(f"Error loading schema: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python setup.py <csv_file> [table_name]")
        print("  python setup.py --info  # Show schema information")
        print("  python setup.py --help  # Show this help")
        sys.exit(1)
    
    if sys.argv[1] == "--info":
        show_schema_info()
    elif sys.argv[1] == "--help":
        print("Generic Query System Setup")
        print("==========================")
        print()
        print("This script sets up a generic query system that can work with any CSV dataset.")
        print()
        print("Usage:")
        print("  python setup.py <csv_file> [table_name]")
        print("    - csv_file: Path to your CSV file")
        print("    - table_name: Optional table name (default: main_table)")
        print()
        print("  python setup.py --info")
        print("    - Show current schema information")
        print()
        print("Example:")
        print("  python setup.py inventory.csv products")
        print("  python setup.py sales_data.csv")
    else:
        csv_file = sys.argv[1]
        table_name = sys.argv[2] if len(sys.argv) > 2 else "main_table"
        
        if not os.path.exists(csv_file):
            print(f"Error: CSV file '{csv_file}' not found")
            sys.exit(1)
        
        success = setup_system(csv_file=csv_file, table_name=table_name)
        if not success:
            sys.exit(1)
