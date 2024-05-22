
from flask_restx import Api, Resource, fields

from flask import Flask, request, jsonify
import os

import torch
import logging
from logging.handlers import RotatingFileHandler
import sys

from flask import render_template
import traceback
from transformers import AutoTokenizer, AutoModelForSequenceClassification
# Append the directory of `main.py` to sys.path to locate config.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('Application startup')

# Create API with Flask-Restx, specify custom endpoint for Swagger UI
api = Api(app, version='1.0', title='Natural History Study Article Prediction API',
          description='This API leverages a transformer-based machine learning model for Natural History Study article prediction.', doc='/article_prediction_api')

# Define namespace
ns = api.namespace('article_prediction_api/v1', description='Prediction operations')

# Model definition for input texts
sample_text="""Historically, surgical correction has been the treatment of choice for benign biliary strictures (BBS). 
Self-expandable metallic stents (MSs) have been useful for inoperable malignant biliary strictures; however, 
their use for BBS is controversial and their natural history unknown. To test our hypothesis that MSs provide 
only short-term benefit, we examined the long-term outcome of MSs for the treatment of BBS. Our goal was to 
develop a rational approach for treating BBS. Between July 1990 and December 1995, 15 patients had MSs placed for 
BBS and have been followed up for a mean of 86.3 months (range, 55-120 months). The mean age of the patients was 66.6 years 
and 12 were women. Stents were placed for surgical injury in 5 patients and underlying disease in 10 patients (lithiasis, 7; 
pancreatitis, 2; and primary sclerosing cholangitis, 1). One or more MSs (Gianturco-Rosch "Z" for 4 patients and Wallstents 
for 11 patients) were placed by percutaneous, endoscopic, or combined approaches. We considered patients to have a good clinical 
outcome if the stent remained patent, they required 2 or fewer invasive interventions, and they had no biliary dilation on subsequent 
imaging. Metallic stents were successfully placed in all 15 patients, and the mean patency rate was 30.6 months (range, 7-120 months). 
Five patients (33%) had a good clinical result with stent patency from 55 to 120 months. Ten patients (67%) required more than 2 radiologic 
and/or endoscopic procedures for recurrent cholangitis and/or obstruction (range, 7-120 months). Five of the 10 patients developed complete 
stent obstruction at 8, 9, 10, 15, and 120 months and underwent surgical removal of the stent and bilioenteric anastomosis. Four of these 
5 patients had strictures from surgical injuries. The patient who had surgical removal 10 years after MS placement developed cholangiocarcinoma. 
Surgical repair remains the treatment of choice for BBS. Metallic stents should only be considered for poor surgical candidates, intrahepatic 
biliary strictures, or failed attempts at surgical repair. Most patients with MSs will develop recurrent cholangitis or stent obstruction and 
require intervention. Chronic inflammation and obstruction may predispose the patient to cholangiocarcinoma."""


text_input = api.model('TextInput', {
    'texts': fields.List(fields.String, required=True, description='List of texts to predict', example=["This is am example text!",
                                                                                                        sample_text ])
})

# Load model and tokenizer
model_path = app.config['MODEL_PATH']
tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(model_path)
model.eval()

@ns.route('/predict')
class Predict(Resource):
    @api.expect(text_input, validate=True)
    def post(self):
        data = request.get_json(force=True)  # This ensures the data is treated as JSON
        if 'texts' not in data or not isinstance(data['texts'], list):
            return {'error': 'Invalid input. "texts" must be a list.'}, 400
        # if not all(isinstance(text, str) and text.strip() for text in data['texts']):
        #     return {'error': 'Invalid input. All "texts" must be non-empty strings.'}, 400


        try:
            if not data['texts']:
                return {'error': "Texts field cannot be empty."}, 400  # Right place to handle empty list
            inputs = tokenizer(data['texts'], return_tensors="pt", padding=True, truncation=True, max_length=256)
            with torch.no_grad():
                logits = model(**inputs).logits
            predictions = logits.argmax(-1).tolist()
            app.logger.info("Prediction successful")
            return {'predictions': predictions}, 200
        except Exception as e:
            app.logger.error("Error during prediction: {}".format(str(e)))
            app.logger.error(traceback.format_exc())  # Log the full traceback
            return {'error': "Unable to process due to error"}, 500



# @app.route('/')
# def home():

    # try:
    #     app.logger.info('visit home page', exc_info=True)
    #     return render_template('index.html'),200
    # except Exception as e:
    #     app.logger.error('Failed to visit homepage', exc_info=True)
    #     app.logger.error(traceback.format_exc())  # Log the full traceback
    #     # return jsonify({'error': str(e)}), 500
    #     return jsonify({'error': "Unable to process due to error"}), 500

if __name__ == '__main__':
    # Using configurations from the Config class
    app.run(host=app.config['HOST'], port=app.config['PORT'], debug=app.config['DEBUG'])

