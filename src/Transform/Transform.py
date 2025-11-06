import re
from decimal import Decimal, InvalidOperation

class Transform:
    def __init__(self):
        self.schema = {
            'document_id': str,
            'name': str,
            'description': str,
            'line_items': list
        }
    
    def transform(self, extracted_data):
        """Normalizes and cleans extracted data from multiple documents"""
        transformed = {}
        for filename, data in extracted_data.items():
            transformed[filename] = self._clean_document(data)
        return transformed
    
    def _clean_document(self, doc):
        """Cleans and normalizes a single document"""
        return {
            'document_id': self._clean_text(doc.get('document_id', doc.get('id', doc.get('number', 'N/A')))),
            'name': self._clean_text(doc.get('name', doc.get('contractor', doc.get('supplier', doc.get('client', 'N/A'))))),
            'description': self._clean_text(doc.get('description', doc.get('desc', 'N/A'))),
            'line_items': self._clean_line_items(doc.get('line_items', doc.get('items', [])))
        }
    
    def _clean_text(self, text):
        """Removes extra spaces and normalizes text"""
        if not text or text == 'N/A':
            return 'N/A'
        return re.sub(r'\s+', ' ', str(text).strip())
    
    def _clean_line_items(self, items):
        """Normalizes line items, ensuring correct numeric types"""
        if not items or not isinstance(items, list):
            return []
        
        cleaned = []
        for item in items:
            if not isinstance(item, dict):
                continue
            
            cleaned_item = {
                'item': self._clean_text(item.get('item', item.get('description', item.get('name', 'N/A')))),
                'quantity': self._to_decimal(item.get('quantity', item.get('qty', 0))),
                'unit_price': self._to_decimal(item.get('unit_price', item.get('price', item.get('unit', 0)))),
                'total': self._to_decimal(item.get('total', item.get('amount', 0)))
            }
            cleaned.append(cleaned_item)
        
        return cleaned
    
    def _to_decimal(self, value):
        """Converts values to Decimal, handling errors"""
        if isinstance(value, (int, float, Decimal)):
            return float(Decimal(str(value)))
        
        if isinstance(value, str):
            try:
                cleaned = re.sub(r'[^\d.,\-]', '', value).replace(',', '.')
                return float(Decimal(cleaned))
            except (InvalidOperation, ValueError):
                return 0.0
        
        return 0.0
