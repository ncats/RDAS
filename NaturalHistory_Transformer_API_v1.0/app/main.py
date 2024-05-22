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
descriptions="""

This API utilizes a transformer-based machine learning model to predict the relevance of articles for Natural History Studies. It is specifically designed to handle batches of article abstracts and assess their relevance to the study of rare diseases.

Input:

The API requires a JSON object with a key named "texts" that contains a list of article abstracts as strings. Each string should represent the abstract of a research article. The API evaluates each abstract individually to generate predictions.

Output:

The response is a JSON object with a key "predictions" that contains an array of integers. Each integer represents a category or outcome for each abstract provided in the input. Predictions are returned in the same order as the input texts.

A prediction of '0' indicates that the article is either not primarily experimental or not focused on the natural history of a disease.
A prediction of '1' indicates that the article's primary contribution is relevant to the study of the natural history of a rare disease.

Error Handling:

If the "texts" list is empty, improperly formatted, or not provided, the API will return a 400 Bad Request error, detailing the nature of the input error with an appropriate message.
The API accepts empty strings within the list and provides a prediction result for each, including empty strings.

For more information, please visit our Github repository at: https://github.com/ncats/RDAS/tree/minghui_development/NaturalHistory_Transformer_API_v1.0.
"""

api = Api(app, version='1.0', title='Natural History Study Article Prediction API',
          description=descriptions, doc='/article_prediction_api')

# Define namespace
ns = api.namespace('article_prediction_api/v1', description='Prediction operations')

# Model definition for input texts
sample_text="""The natural history, prognostication and optimal treatment of Richter transformation developed from chronic 
lymphocytic leukemia (CLL) are not well defined. We report the clinical characteristics and outcomes of a large series of 
biopsy-confirmed Richter transformation (diffuse large B-cell lymphoma or high grade B-cell lymphoma, n=204) cases diagnosed 
from 1993 to 2018. After a median follow up of 67.0 months, the median overall survival (OS) was 12.0 months. Patients who 
received no prior treatment for CLL had significantly better OS (median 46.3 vs. 7.8 months; P<0.001). Patients with elevated 
lactate dehydrogenase (median 6.2 vs. 39.9 months; P<0.0001) or TP53 disruption (median 8.3 vs. 12.8 months; P=0.046) had worse
 OS than those without. Immunoglobulin heavy chain variable region gene mutation, cell of origin, Myc/Bcl-2 double expression and
  MYC/BCL2/BCL6 double-/triple-hit status were not associated with OS. In multivariable Cox regression, elevated lactate dehydrogenase
   [Hazard ratio (HR) 2.3, 95% Confidence Interval (CI): 1.3-4.1; P=0.01], prior CLL treatment (HR 2.0, 95%CI: 1.2-3.5; P=0.01), 
   and older age (HR 1.03, 95%CI: 1.01-1.05; P=0.01) were associated with worse OS. Twenty-four (12%) patients underwent stem cell 
   transplant (20 autologous and 4 allogeneic), and had a median post-transplant survival of 55.4 months. In conclusion, the overall 
   outcome of Richter transformation is poor. Richter transformation developed in patients with untreated CLL has significantly better 
   survival. Stem cell transplant may benefit select patients.
"""


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

