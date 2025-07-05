# Generic Dynamic Query System

A flexible, AI-powered query system that can adapt to any CSV dataset and provide natural language querying capabilities.

## Features

- **Dynamic Schema Analysis**: Automatically analyzes your dataset to understand column types, patterns, and relationships
- **Template Generation**: Creates query templates based on your data structure
- **Semantic Matching**: Uses SBERT embeddings for intelligent query understanding
- **Multi-query Support**: Handles multiple queries in a single request
- **Lightweight**: Uses efficient models that run smoothly on laptops
- **Cost-Effective**: All processing happens locally, no API costs

## Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Setup Your Dataset

```bash
# Setup with your CSV file
python setup.py your_data.csv

# Or specify a custom table name
python setup.py your_data.csv my_table
```

### 3. Start Redis Server

```bash
redis-server
```

### 4. Run the Application

```bash
python app.py
```

### 5. Test the System

```bash
curl -X POST http://localhost:4000/query \
  -H 'Content-Type: application/json' \
  -d '{"userInput": "show me details about product ABC"}'
```

## Usage Examples

### Basic Queries
- "what is the price of item X"
- "how many items do we have"
- "show me all details for product Y"

### Multi-part Queries
- "show price of item A and quantity of item B"
- "tell me about product X and when was it last updated"

### Flexible Item Matching
- Product codes: "PI-1234"
- Descriptions: "red bicycle"
- Names: "iPhone 13"

## API Endpoints

### POST /query
Main query endpoint that accepts natural language queries.

**Request:**
```json
{
  "userInput": "what is the price of item ABC"
}
```

**Response:**
```json
{
  "any_hit": true,
  "queries": [
    {
      "query": "what is the price of item ABC",
      "hit": true,
      "template": "what is the price of {ITEM}",
      "score": 0.95,
      "result": {
        "ProductDescription": "Item ABC Description",
        "ProductPrice": 29.99
      }
    }
  ]
}
```

### GET /schema
Returns the analyzed schema information.

### GET /templates
Returns all generated query templates.

## Configuration

Edit `config.py` to customize:

- **MODEL_NAME**: Change the SBERT model (default: 'all-mpnet-base-v2')
- **Thresholds**: Adjust matching sensitivity
- **Database settings**: Change file paths and table names
- **Redis configuration**: Modify Redis connection settings

## Architecture

1. **Schema Analyzer**: Analyzes your dataset to understand column types and patterns
2. **Template Generator**: Creates query templates based on schema analysis
3. **Generic Matcher**: Handles slot extraction and intent matching using SBERT
4. **Flask API**: Provides REST endpoints for querying

## Supported Column Types

- **Text Columns**: Product descriptions, names, comments
- **Numeric Columns**: Prices, quantities, measurements
- **Categorical Columns**: Categories, types, statuses
- **ID/Code Columns**: Product codes, SKUs, identifiers
- **Date Columns**: Timestamps, dates

## Performance Optimization

- **FAISS indexing** for fast similarity search
- **Redis caching** for frequent queries
- **Efficient embeddings** with normalized vectors
- **Batch processing** for multiple queries

## Troubleshooting

### Common Issues

1. **"No template match found"**
   - Check if your query contains identifiable items
   - Verify the item exists in your dataset
   - Try more specific descriptions

2. **Redis connection errors**
   - Ensure Redis server is running
   - Check Redis configuration in config.py

3. **Database errors**
   - Verify the CSV file was processed correctly
   - Check database file permissions

### Getting Help

```bash
# Show schema information
python setup.py --info

# Show help
python setup.py --help
```

## Advanced Usage

### Custom Templates

You can manually edit the generated `templates.json` file to add custom query patterns.

### Multiple Datasets

To work with multiple datasets, create separate configurations:

```python
# config_sales.py
config.DB_FILE = 'sales.db'
config.TABLE_NAME = 'sales_data'
config.TEMPLATE_FILE = 'sales_templates.json'
```

### Scaling

For production use:
- Use PostgreSQL instead of SQLite
- Implement connection pooling
- Add query result caching
- Use multiple Redis instances

## License

MIT License - feel free to use and modify for your needs.
