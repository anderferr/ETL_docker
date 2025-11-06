from flask import Flask, jsonify, render_template_string, request
from src.Extract.Extract import Extract
from src.Transform.Transform import Transform
from src.Load.Load import Load
from src.Analyze.Analyze import Analyze
import boto3
import json
import os
import threading
import time

app = Flask(__name__)

def run_etl(images):
    """Executes ETL pipeline for a list of images"""
    print(f"\n=== Processing {len(images)} images ===")
    
    extractor = Extract()
    extracted_data = extractor.extract_text_from_images(images)
    
    transformer = Transform()
    transformed_data = transformer.transform(extracted_data)
    
    loader = Load()
    loader.load(transformed_data)
    loader.close()
    
    analyzer = Analyze()
    latex_report = analyzer.analyze()
    analyzer.save_report(latex_report)
    
    print("=== ETL completed ===\n")

def poll_sqs():
    """Polls SQS queue to detect new files"""
    sqs = boto3.client('sqs', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    queue_url = os.getenv('SQS_QUEUE_URL')
    
    if not queue_url:
        print("SQS_QUEUE_URL not configured. Polling disabled.")
        return
    
    print(f"Starting SQS polling: {queue_url}")
    
    while True:
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
        
        if 'Messages' in response:
            images = []
            for msg in response['Messages']:
                try:
                    body = json.loads(msg['Body'])
                    if 'Records' in body:
                        key = body['Records'][0]['s3']['object']['key']
                        if key.endswith('.jpg'):
                            images.append(key.replace('.jpg', ''))
                except Exception as e:
                    print(f"Error processing message: {e}")
                finally:
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])
            
            if images:
                run_etl(images)

@app.route('/')
def index():
    """Home page with navigation"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ETL Pipeline</title>
        <style>
            body { font-family: Arial; max-width: 600px; margin: 50px auto; padding: 20px; }
            a { display: inline-block; margin: 5px; padding: 10px 20px; background: #ff9e1d; 
                color: white; text-decoration: none; border-radius: 4px; }
            a:hover { opacity: 0.8; }
        </style>
    </head>
    <body>
        <h1>ETL Pipeline</h1>
        <p><a href="/health">Health Check</a></p>
        <p><a href="/trigger">Trigger ETL</a></p>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route('/health')
def health():
    """Health check with system status"""
    try:
        import psycopg2
        db_status = "Connected"
        try:
            conn = psycopg2.connect(
                host=os.getenv('RDS_HOST'),
                database=os.getenv('RDS_DATABASE', 'postgres'),
                user=os.getenv('RDS_USER', 'admin'),
                password=os.getenv('RDS_PASSWORD'),
                port=os.getenv('RDS_PORT', 5432),
                sslmode='require',
                connect_timeout=3
            )
            conn.close()
        except:
            db_status = "Disconnected"
    except:
        db_status = "Error"
    
    sqs_status = "Enabled" if os.getenv('SQS_QUEUE_URL') else "Disabled"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Health Check</title>
        <style>
            body {{ font-family: Arial; max-width: 600px; margin: 50px auto; padding: 20px; }}
            a {{ display: inline-block; margin: 5px; padding: 10px 20px; background: #ff9e1d; 
                 color: white; text-decoration: none; border-radius: 4px; }}
            a:hover {{ opacity: 0.8; }}
        </style>
    </head>
    <body>
        <h1>Health Check</h1>
        <p>Flask: Running</p>
        <p>Database: {db_status}</p>
        <p>SQS: {sqs_status}</p>
        <p>Region: {os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}</p>
        <p><a href="/">Back</a></p>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route('/trigger', methods=['GET', 'POST'])
def trigger():
    """Manual ETL trigger endpoint"""
    if request.method == 'POST':
        try:
            run_etl(['000', '001'])
            message = "Success! ETL completed."
        except Exception as e:
            message = f"Error: {str(e)}"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ETL Result</title>
            <style>
                body {{ font-family: Arial; max-width: 600px; margin: 50px auto; padding: 20px; }}
                a {{ display: inline-block; margin: 5px; padding: 10px 20px; background: #ff9e1d; 
                     color: white; text-decoration: none; border-radius: 4px; }}
                a:hover {{ opacity: 0.8; }}
            </style>
        </head>
        <body>
            <h1>ETL Result</h1>
            <p>{message}</p>
            <p><a href="/trigger">Trigger Again</a> <a href="/">Back</a></p>
        </body>
        </html>
        """
        return render_template_string(html)
    
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Trigger ETL</title>
        <style>
            body { font-family: Arial; max-width: 600px; margin: 50px auto; padding: 20px; }
            button { padding: 12px 24px; font-size: 16px; background: #ff9e1d; color: white; 
                     border: none; border-radius: 4px; cursor: pointer; }
            button:hover { opacity: 0.8; }
            a { display: inline-block; margin: 5px; padding: 10px 20px; background: #ff9e1d; 
                color: white; text-decoration: none; border-radius: 4px; }
            a:hover { opacity: 0.8; }
        </style>
    </head>
    <body>
        <h1>Trigger ETL</h1>
        <form method="POST">
            <button type="submit">TRIGGER</button>
        </form>
        <p><a href="/">Back</a></p>
    </body>
    </html>
    """
    return render_template_string(html)

if __name__ == '__main__':
    threading.Thread(target=poll_sqs, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
