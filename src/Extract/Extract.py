import boto3
import json
import base64

class Extract:
    def __init__(self, bucket='alvorada-bucket', region='us-east-1'):
        self.bucket = bucket
        self.s3 = boto3.client('s3')
        self.bedrock = boto3.client('bedrock-runtime', region_name=region)
    
    def extract_text_from_images(self, images=['000', '001']):
        results = {}
        for i in images:
            img = base64.b64encode(self.s3.get_object(Bucket=self.bucket, Key=f'{i}.jpg')['Body'].read()).decode()
            body = json.dumps({
                "messages": [{
                    "role": "user",
                    "content": [
                        {"image": {"format": "jpeg", "source": {"bytes": img}}},
                        {"text": """Extract the following information from this invoice/receipt document and return as JSON:
                            - document_id: invoice number, receipt number, or document ID
                            - name: company name, contractor, client, or supplier name
                            - description: main product or service description
                            - line_items: array of products/services with:
                            * item: product/service name or description (NOT codes or numbers)
                            * quantity: number of units
                            * unit_price: price per unit
                            * total: total amount for this line

                            IMPORTANT: 
                            - For 'item', extract the PRODUCT NAME or SERVICE DESCRIPTION, ignore item codes/SKU numbers
                            
                            - Ignore numeric codes unless they are the only identifier

                            Return only valid JSON, no additional text."""}
                    ]
                }],
                "inferenceConfig": {"max_new_tokens": 2000}
            })
            response = self.bedrock.invoke_model(modelId='amazon.nova-lite-v1:0', body=body)
            text = json.loads(response['body'].read())['output']['message']['content'][0]['text']
            
            # Extract only the first valid JSON object
            start = text.find('{')
            if start == -1:
                continue
            
            brace_count = 0
            for idx in range(start, len(text)):
                if text[idx] == '{':
                    brace_count += 1
                elif text[idx] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        results[f'{i}.jpg'] = json.loads(text[start:idx+1])
                        break
        return results
