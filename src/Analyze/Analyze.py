import boto3
import json
import os
import psycopg2

class Analyze:
    def __init__(self, region='us-east-1'):
        self.bedrock = boto3.client('bedrock-runtime', region_name=region)
        self.conn = psycopg2.connect(
            host=os.getenv('RDS_HOST'),
            database=os.getenv('RDS_DATABASE', 'postgres'),
            user=os.getenv('RDS_USER', 'admin'),
            password=os.getenv('RDS_PASSWORD'),
            port=os.getenv('RDS_PORT', 5432),
            sslmode='require'
        )
    
    def analyze(self):
        cursor = self.conn.cursor()
        
        cursor.execute('SELECT * FROM documents')
        docs = cursor.fetchall()
        
        cursor.execute('SELECT * FROM line_items')
        items = cursor.fetchall()
        
        cursor.close()
        self.conn.close()
        
        prompt = f"""Generate a simple LaTeX report with this data:

            Documents: {docs}
            Line items: {items}

            Include: total documents, total value, top 5 suppliers, top 5 items.

            RULES:
            - The title needs to be "Executive Report"
            - It needs to be an executive report, easy to understand and concise.
            - Use ONLY article class and basic tabular.
            - ALL created tables in the latex code must have MAXIMUM 3 columns
            - Replace special characters: # with NUM, & with AND, $ with USD, % with PCT
            - Keep it simple and clean
            Return ONLY compilable LaTeX code."""
                    
        body = json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"max_new_tokens": 3000}
        })
        
        response = self.bedrock.invoke_model(modelId='amazon.nova-pro-v1:0', body=body)
        latex = json.loads(response['body'].read())['output']['message']['content'][0]['text']
        
        return latex.strip('```latex\n').strip('```').strip()
    
    def save_report(self, latex_code, filename='reports/report.tex'):
        """Saves LaTeX report and compiles to PDF"""
        import subprocess
        import glob
        os.makedirs('reports', exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(latex_code)
        print(f"LaTeX report saved at: {filename}")
        
        try:
            result = subprocess.run(['pdflatex', '-output-directory=reports', filename], 
                                  capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                print(f"Error compiling LaTeX:")
                print(result.stdout[-500:])
            else:
                print(f"PDF generated at: reports/report.pdf")
                
                # Clean up auxiliary files
                for file in glob.glob('reports/*'):
                    if not file.endswith(('.tex', '.pdf')):
                        os.remove(file)
                print("Auxiliary files removed")
        except Exception as e:
            print(f"PDF compilation error: {e}")
            print(".tex report saved, but PDF was not generated")
