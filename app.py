
import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import multiprocessing as mp
mp.set_start_method("forkserver", force=True)

from flask import Flask, request, jsonify
import sqlite3
import re
from generic_matcher import GenericMatcher
from config import config
from watcher import start_watcher 

app = Flask(__name__)

# Initialize the matcher
matcher = GenericMatcher()

# Start watching schema and template files
start_watcher(reload_callback=matcher.reload)

def fetch_results(match_dict):
    """
    Execute SQL query and return results
    """
    sql = match_dict['sql']
    # Always fetch * for complete information
    if not sql.strip().upper().startswith('SELECT *'):
        sql_parts = sql.split()
        if len(sql_parts) > 1:
            sql_all = sql.replace(sql_parts[1], '*', 1)
        else:
            sql_all = sql
    else:
        sql_all = sql

    conn = sqlite3.connect(config.DB_FILE)
    cur = conn.cursor()

    results = []
    slots = match_dict['slot']
    
    # Handle list of slots (for description matches) or single slot
    if isinstance(slots, list):
        for slot_val, slot_score in slots:
            cur.execute(sql_all, (slot_val,))
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                result_dict = dict(zip(cols, row))
                result_dict["slot_score"] = round(slot_score, 3)
                results.append(result_dict)
    else:
        cur.execute(sql_all, (slots,))
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            results.append(dict(zip(cols, row)))

    conn.close()
    return results

@app.route('/query', methods=['POST'])
def query_multi():
    data = request.get_json(force=True)
    q = data.get('userInput', '').strip()
    if not q:
        return jsonify({"error": "Empty userInput"}), 400

    # Split on conjunctions to support multiple requests
    parts = [p.strip() for p in re.split(r'\b(?:and|then|also)\b|[,;]', q, flags=re.IGNORECASE) if p.strip()]

    all_responses = []
    any_hit = False

    for part in parts:
        m = matcher.match_with_sbert(part)
        if not m:
            all_responses.append({
                "query": part,
                "hit": False,
                "message": "no template match found"
            })
        else:
            any_hit = True
            rows = fetch_results(m)

            # Determine if scalar response
            requested_col = m['sql'].split()[1] if len(m['sql'].split()) > 1 else '*'
            is_scalar = ('*' not in m['sql']) and len(rows) == 1

            if is_scalar and rows:
                row = rows[0]
                # Try to include a description field if available
                desc_field = None
                for text_col in matcher.schema['text_columns']:
                    if text_col in row:
                        desc_field = text_col
                        break
                
                if desc_field:
                    result = {
                        desc_field: row[desc_field],
                        requested_col: row[requested_col]
                    }
                else:
                    result = {requested_col: row[requested_col]}
            else:
                result = rows

            all_responses.append({
                "query": part,
                "hit": True,
                "template": m['template'],
                "score": m.get('score'),
                "result": result,
                "column_matched": m.get('column'),
                "is_description_match": m.get('is_description_match', False)
            })

    return jsonify({
        "any_hit": any_hit,
        "queries": all_responses
    }), 200

@app.route('/schema', methods=['GET'])
def get_schema():
    """Endpoint to get schema information"""
    return jsonify(matcher.schema)

@app.route('/templates', methods=['GET'])
def get_templates():
    """Endpoint to get available templates"""
    templates_data = []
    for i, template in enumerate(matcher.templates):
        templates_data.append({
            "template": template,
            "sql": matcher.sql_templates[i]
        })
    return jsonify(templates_data)

if __name__ == '__main__':
   app.run(host='0.0.0.0', port=4000, debug=True, use_reloader=False)
