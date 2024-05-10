from flask import Flask, request, jsonify
import os
import traceback
import torch
import logging
from logging.handlers import RotatingFileHandler
import sys
from flask import render_template
# from dotenv import load_dotenv
from transformers import AutoTokenizer, AutoModelForSequenceClassification
# Append the directory of `main.py` to sys.path to locate config.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
from config import Config


app = Flask(__name__)
# Load configuration from Config class

app.config.from_object(Config)
# Configure logging

# Load environment variables from .env file
# load_dotenv()

if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('Application startup')

# Load model and tokenizer
model_path =app.config['MODEL_PATH']
# model_path = os.environ['MODEL_PATH']
# print(os.getcwd())
# print("model_path::",model_path)
# model_path = os.getenv('MODEL_PATH')

# print("Files in model directory:", os.listdir(model_path))
tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(model_path)
model.eval()

# @app.route('/predict', methods=['POST'])
# def predict():
#     try:
#
#         texts = request.json.get('texts')
#         if not texts or not isinstance(texts, list):
#             app.logger.error('Invalid input type for texts')
#             return jsonify({'error': "Input 'texts' must be a non-empty list of strings."}), 400
#
#         inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=256)
#         with torch.no_grad():
#             logits = model(**inputs).logits
#         predictions = logits.argmax(-1).tolist()
#
#         app.logger.info(f'Prediction successful!')
#         print(predictions)
#         return jsonify({'predictions': predictions}), 200
#     except Exception as e:
#         app.logger.error('Failed to predict', exc_info=True)
#         app.logger.error(traceback.format_exc())  # Log the full traceback
#         return jsonify({'error': str(e)}), 500

@app.route('/predict', methods=['POST', 'GET'])
def predict():
    try:
        if request.method == 'POST':
            data = request.json
            texts = data.get('texts')
        elif request.method == 'GET':
            texts = request.args.getlist('texts')  # Assumes texts are passed as multiple "texts" query parameters

        if not texts or not isinstance(texts, list):
            app.logger.error('Invalid input type for texts')
            return jsonify({'error': "Input 'texts' must be a non-empty list of strings."}), 400

        # Assuming `tokenizer` and `model` are defined elsewhere and ready to use
        inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=256)
        with torch.no_grad():
            logits = model(**inputs).logits
        predictions = logits.argmax(-1).tolist()

        app.logger.info(f'Prediction successful!')
        return jsonify({'predictions': predictions}), 200

    except Exception as e:
        app.logger.error('Failed to predict', exc_info=True)
        app.logger.error(traceback.format_exc())  # Log the full traceback
        return jsonify({'error': str(e)}), 500



@app.route('/index', methods=['GET'])
def home():

    try:
        app.logger.info('visit home page', exc_info=True)
        return render_template('index.html'),200
    except Exception as e:
        app.logger.error('Failed to visit homepage', exc_info=True)
        app.logger.error(traceback.format_exc())  # Log the full traceback
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    debug = os.getenv('DEBUG')

    app.run(host=host, port=port, debug=debug)
