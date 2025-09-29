#!/usr/bin/env python3
"""
Web UI for File Upload to Kafka
Provides a web interface for uploading documents and videos to Kafka topics
"""

import os
import json
import base64
import hashlib
import mimetypes
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from werkzeug.utils import secure_filename
from kafka import KafkaProducer
from kafka.errors import KafkaError
import magic
import cv2
import numpy as np

app = Flask(__name__)
app.secret_key = 'flink_kafka_learning_project'

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {
    'documents': {'txt', 'pdf', 'doc', 'docx', 'rtf'},
    'videos': {'mp4', 'avi', 'mov', 'mkv', 'wmv', 'flv', 'webm'},
    'images': {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff'}
}

# Ensure upload directory exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(os.path.join(UPLOAD_FOLDER, 'documents'), exist_ok=True)
os.makedirs(os.path.join(UPLOAD_FOLDER, 'videos'), exist_ok=True)
os.makedirs(os.path.join(UPLOAD_FOLDER, 'images'), exist_ok=True)

# Kafka producer for file uploads
kafka_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    acks='all',
    retries=3
)

def allowed_file(filename, file_type):
    """Check if file extension is allowed for the given type"""
    if '.' not in filename:
        return False
    ext = filename.rsplit('.', 1)[1].lower()
    return ext in ALLOWED_EXTENSIONS.get(file_type, set())

def get_file_type(filename):
    """Determine file type based on extension"""
    if '.' not in filename:
        return 'unknown'
    ext = filename.rsplit('.', 1)[1].lower()
    
    if ext in ALLOWED_EXTENSIONS['documents']:
        return 'document'
    elif ext in ALLOWED_EXTENSIONS['videos']:
        return 'video'
    elif ext in ALLOWED_EXTENSIONS['images']:
        return 'image'
    else:
        return 'unknown'

def calculate_file_hash(file_path):
    """Calculate MD5 hash of file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def extract_document_text(file_path, file_type):
    """Extract text from document files"""
    try:
        if file_type == 'pdf':
            import PyPDF2
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text()
                return text
        elif file_type in ['doc', 'docx']:
            from docx import Document
            doc = Document(file_path)
            text = ""
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            return text
        elif file_type == 'txt':
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        else:
            return "Text extraction not supported for this file type"
    except Exception as e:
        return f"Error extracting text: {str(e)}"

def extract_video_metadata(file_path):
    """Extract metadata from video files"""
    try:
        cap = cv2.VideoCapture(file_path)
        if not cap.isOpened():
            return None
        
        metadata = {
            'fps': cap.get(cv2.CAP_PROP_FPS),
            'frame_count': int(cap.get(cv2.CAP_PROP_FRAME_COUNT)),
            'width': int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            'height': int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
            'duration': cap.get(cv2.CAP_PROP_FRAME_COUNT) / cap.get(cv2.CAP_PROP_FPS) if cap.get(cv2.CAP_PROP_FPS) > 0 else 0
        }
        cap.release()
        return metadata
    except Exception as e:
        return {'error': f"Error extracting video metadata: {str(e)}"}

def extract_image_metadata(file_path):
    """Extract metadata from image files"""
    try:
        image = cv2.imread(file_path)
        if image is None:
            return None
        
        height, width, channels = image.shape
        return {
            'width': width,
            'height': height,
            'channels': channels,
            'total_pixels': width * height
        }
    except Exception as e:
        return {'error': f"Error extracting image metadata: {str(e)}"}

def send_file_to_kafka(file_path, filename, file_type, metadata=None):
    """Send file data to appropriate Kafka topic"""
    try:
        # Read file content
        with open(file_path, 'rb') as file:
            file_content = file.read()
        
        # Encode file content as base64
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        
        # Create message
        message = {
            'file_id': f"{file_type}_{int(datetime.now().timestamp() * 1000)}",
            'filename': filename,
            'file_type': file_type,
            'file_size': len(file_content),
            'file_hash': calculate_file_hash(file_path),
            'upload_timestamp': datetime.now().isoformat(),
            'file_content': file_base64,
            'metadata': metadata or {}
        }
        
        # Determine topic based on file type
        topic_map = {
            'document': 'uploaded_documents',
            'video': 'uploaded_videos',
            'image': 'uploaded_images'
        }
        
        topic = topic_map.get(file_type, 'uploaded_files')
        
        # Send to Kafka
        future = kafka_producer.send(topic, value=message, key=filename)
        record_metadata = future.get(timeout=10)
        
        return {
            'success': True,
            'topic': topic,
            'partition': record_metadata.partition,
            'offset': record_metadata.offset,
            'file_id': message['file_id']
        }
        
    except KafkaError as e:
        return {'success': False, 'error': f"Kafka error: {str(e)}"}
    except Exception as e:
        return {'success': False, 'error': f"Error processing file: {str(e)}"}

@app.route('/')
def index():
    """Main page with upload interface"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file selected'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    
    if file:
        filename = secure_filename(file.filename)
        file_type = get_file_type(filename)
        
        if file_type == 'unknown':
            return jsonify({'success': False, 'error': 'Unsupported file type'})
        
        # Save file to appropriate directory
        upload_dir = os.path.join(UPLOAD_FOLDER, f"{file_type}s")
        file_path = os.path.join(upload_dir, filename)
        file.save(file_path)
        
        # Extract metadata based on file type
        metadata = {}
        if file_type == 'document':
            metadata['text_content'] = extract_document_text(file_path, filename.split('.')[-1].lower())
            metadata['mime_type'] = mimetypes.guess_type(file_path)[0]
        elif file_type == 'video':
            metadata.update(extract_video_metadata(file_path))
            metadata['mime_type'] = mimetypes.guess_type(file_path)[0]
        elif file_type == 'image':
            metadata.update(extract_image_metadata(file_path))
            metadata['mime_type'] = mimetypes.guess_type(file_path)[0]
        
        # Send to Kafka
        result = send_file_to_kafka(file_path, filename, file_type, metadata)
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': f'File uploaded successfully to topic {result["topic"]}',
                'file_id': result['file_id'],
                'filename': filename,
                'file_type': file_type,
                'file_size': os.path.getsize(file_path),
                'metadata': metadata
            })
        else:
            return jsonify({'success': False, 'error': result['error']})
    
    return jsonify({'success': False, 'error': 'Upload failed'})

@app.route('/status')
def status():
    """Check Kafka connection status"""
    try:
        # Try to get metadata to check connection
        kafka_producer.metrics()
        return jsonify({'status': 'connected', 'bootstrap_servers': 'localhost:9092'})
    except Exception as e:
        return jsonify({'status': 'disconnected', 'error': str(e)})

@app.route('/topics')
def list_topics():
    """List available Kafka topics"""
    try:
        from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
        admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
        metadata = admin_client.describe_cluster()
        return jsonify({'topics': list(metadata.topics.keys())})
    except Exception as e:
        return jsonify({'error': str(e), 'topics': []})

if __name__ == '__main__':
    print("Starting File Upload Web UI")
    print("==========================")
    print("Access the UI at: http://localhost:5000")
    print("Make sure Kafka is running on localhost:9092")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
