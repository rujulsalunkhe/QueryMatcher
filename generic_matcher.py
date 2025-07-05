import json
import redis
import sqlite3
import re
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer, util
from typing import Dict, List, Tuple, Optional, Any, Union
from config import config

class GenericMatcher:
    def __init__(self, schema_file: str = None, template_file: str = None):
        self.schema_file = schema_file or config.SCHEMA_FILE
        self.template_file = template_file or config.TEMPLATE_FILE
        
        # Load configuration
        self.model = SentenceTransformer(config.MODEL_NAME)
        self.redis_client = redis.Redis.from_url(config.REDIS_URL)
        
        # Load schema and templates
        self.schema = self._load_schema()
        self.templates, self.sql_templates = self._load_templates()
        
        
        # Initialize FAISS index
        self._init_intent_index()
        self._init_description_embeddings()
        
        # Cache for categorical values
        self._categorical_cache = {}
        self._load_categorical_values()
    
    def _load_schema(self) -> Dict:
        """Load schema information"""
        with open(self.schema_file, 'r') as f:
            return json.load(f)
    
    def _load_templates(self) -> Tuple[List[str], List[str]]:
        """Load templates and SQL statements"""
        with open(self.template_file, 'r') as f:
            entries = json.load(f)
        
        templates = [e['template'] for e in entries]
        sql_templates = [e['sql'] for e in entries]
        
        return templates, sql_templates
    
    
    def _init_intent_index(self):
        """Initialize FAISS index for intent matching"""
        intent_phrases = [t.replace('{ITEM}', '').strip().lower() for t in self.templates]
        intent_emb = self.model.encode(intent_phrases, convert_to_tensor=True)
        intent_norm = intent_emb / intent_emb.norm(dim=1, keepdim=True)
        
        self.intent_idx = faiss.IndexFlatIP(config.INDEX_DIM)
        self.intent_idx.add(intent_norm.cpu().numpy().astype('float32'))
    
    def _init_description_embeddings(self):
        """Initialize embeddings for text columns"""
        self.desc_embeddings = {}
        self.desc_values = {}
        
        conn = sqlite3.connect(config.DB_FILE)
        
        for col_name in self.schema['text_columns']:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT DISTINCT "{col_name}" '
                f'FROM "{self.schema["table_name"]}" '
                f'WHERE "{col_name}" IS NOT NULL'
            )
            values = [row[0] for row in cursor.fetchall()]
            
            if values:
                embeddings = self.model.encode(values, convert_to_tensor=True)
                embeddings_norm = embeddings / embeddings.norm(dim=1, keepdim=True)
                
                self.desc_values[col_name] = values
                self.desc_embeddings[col_name] = embeddings_norm
        
        conn.close()
    
    def _load_categorical_values(self):
        """Load categorical values for exact matching"""
        conn = sqlite3.connect(config.DB_FILE)
        
        for col_name in self.schema['categorical_columns'] + self.schema['code_columns'] + self.schema['id_columns']:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT {col_name} FROM {self.schema['table_name']} WHERE {col_name} IS NOT NULL")
            values = {str(row[0]).lower(): str(row[0]) for row in cursor.fetchall()}
            self._categorical_cache[col_name] = values
        
        conn.close()

    def reload(self):
        """Reload schema and templates dynamically"""
        try:
            print("♻️ Reloading GenericMatcher...")
            self.schema = self._load_schema()
            self.templates, self.sql_templates = self._load_templates()
            self._init_intent_index()
            self._init_description_embeddings()
            self._load_categorical_values()
            print("✅ Reload complete")
        except Exception as e:
            print(f"❌ Error during reload: {e}")
    
    def extract_slot(self, query: str) -> Tuple[Any, str, bool]:
        """
        Extract slot value from query
        Returns: (slot_value, column_name, is_description_match)
        """
        query_lower = query.lower()
        
        # 1. Try exact matches for codes and IDs
        for col_name in self.schema['code_columns'] + self.schema['id_columns']:
            patterns = self.schema['columns'][col_name].get('patterns', [])
            
            # Try pattern matching
            for pattern in patterns:
                if pattern == 'CODE-NUMBER':
                    matches = re.findall(r'\b[A-Z]{2,}-\d+\b', query, re.IGNORECASE)
                    if matches:
                        return matches[0], col_name, False
                elif pattern == 'LETTERS-NUMBERS':
                    matches = re.findall(r'\b[A-Z]{2,}\d+\b', query, re.IGNORECASE)
                    if matches:
                        return matches[0], col_name, False
            
            # Try exact value matching
            if col_name in self._categorical_cache:
                for value_lower, original_value in self._categorical_cache[col_name].items():
                    if value_lower in query_lower:
                        return original_value, col_name, False
        
        # 2. Try categorical columns
        for col_name in self.schema['categorical_columns']:
            if col_name in self._categorical_cache:
                for value_lower, original_value in self._categorical_cache[col_name].items():
                    if value_lower in query_lower:
                        return original_value, col_name, False
        
        # 3. Try semantic matching on text columns
        best_match = None
        best_score = config.DESC_SLOT_THRESHOLD
        best_col = None
        
        for col_name in self.schema['text_columns']:
            if col_name in self.desc_embeddings:
                query_emb = self.model.encode([query], convert_to_tensor=True)
                query_norm = query_emb / query_emb.norm(dim=1, keepdim=True)
                
                similarities = util.cos_sim(query_norm, self.desc_embeddings[col_name])[0].cpu().numpy()
                max_idx = np.argmax(similarities)
                max_score = float(similarities[max_idx])
                
                if max_score > best_score:
                    best_score = max_score
                    best_match = [(self.desc_values[col_name][max_idx], max_score)]
                    best_col = col_name
        
        if best_match:
            return best_match, best_col, True
        
        return None, None, False
    
    def match_with_sbert(self, user_query: str) -> Optional[Dict]:
        """
        Main matching function
        """
        slot, col, is_desc = self.extract_slot(user_query)
        if not slot:
            self.redis_client.rpush(config.MISS_LOG_LIST, json.dumps({"query": user_query}))
            return None
        
        if is_desc:
            slot_val = slot[0][0]  # Get the actual description value
        else:
            slot_val = slot
        
        # Generate cleaned query for intent matching
        escaped_slot = re.escape(str(slot_val))
        gen_q = re.sub(escaped_slot, '{ITEM}', user_query, flags=re.IGNORECASE)
        clean_q = gen_q.replace('{ITEM}', '').strip().lower()
        
        # FAISS lookup for intent
        q_emb = self.model.encode([clean_q], convert_to_tensor=True)
        q_norm = q_emb / q_emb.norm(dim=1, keepdim=True)
        D, I = self.intent_idx.search(q_norm.cpu().numpy().astype('float32'), k=1)
        
        intent_score = float(D[0][0])
        idx = int(I[0][0])
        
        if intent_score < config.INTENT_THRESHOLD:
            self.redis_client.rpush(config.MISS_LOG_LIST, json.dumps({
                "query": user_query, "intent_score": intent_score
            }))
            return None
        
        # Replace {COL} with actual column name
        sql_query = self.sql_templates[idx].replace("{COL}", col)
        
        return {
            "template": self.templates[idx],
            "sql": sql_query,
            "slot": slot,
            "score": intent_score if not is_desc else slot,
            "column": col,
            "is_description_match": is_desc
        }