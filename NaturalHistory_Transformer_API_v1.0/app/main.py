# from fastapi import FastAPI, HTTPException, Request
# from pydantic import BaseModel
# from typing import List
# import os
# import torch
# import logging
# from logging.handlers import RotatingFileHandler
# import sys
# import traceback
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from fastapi.responses import JSONResponse
#
# # Append the directory of `main.py` to sys.path to locate config.py
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
# from config import Config
#
# app = FastAPI(
#     title='Natural History Study Article Prediction API',
#     description="""
#     This API utilizes a transformer-based machine learning model to predict the relevance of articles for Natural History Studies. It is specifically designed to handle batches of article abstracts and assess their relevance to the study of rare diseases.
#
#     Input:
#
#     The API requires a JSON object with a key named "texts" that contains a list of article abstracts as strings. Each string should represent the abstract of a research article. The API evaluates each abstract individually to generate predictions.
#
#     Output:
#
#     The response is a JSON object with a key "predictions" that contains an array of integers. Each integer represents a category or outcome for each abstract provided in the input. Predictions are returned in the same order as the input texts.
#
#     A prediction of '0' indicates that the article is either not primarily experimental or not focused on the natural history of a disease.
#     A prediction of '1' indicates that the article's primary contribution is relevant to the study of the natural history of a rare disease.
#
#     Error Handling:
#
#     If the "texts" list is empty, improperly formatted, or not provided, the API will return a 400 Bad Request error, detailing the nature of the input error with an appropriate message.
#     The API accepts empty strings within the list and provides a prediction result for each, including empty strings.
#
#     For more information, please visit our Github repository at: https://github.com/ncats/RDAS/tree/minghui_development/NaturalHistory_Transformer_API_v1.0.
#     """,
#     version="1.0"
# )
#
# # Configure logging
# if not os.path.exists('logs'):
#     os.mkdir('logs')
# file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
# file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
# logging.getLogger().addHandler(file_handler)
# logging.getLogger().setLevel(logging.INFO)
# logging.info('Application startup')
#
# # Load model and tokenizer
# model_path = Config.MODEL_PATH
# print(f"Loading model from: {model_path}")
# tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
# model = AutoModelForSequenceClassification.from_pretrained(model_path)
# model.eval()
#
#
# class TextInput(BaseModel):
#     texts: List[str]
#
#
# @app.post("/article_prediction_api/v1/predict")
# async def predict(input: TextInput):
#     if not input.texts:
#         raise HTTPException(status_code=400, detail="Texts field cannot be empty.")
#
#     try:
#         inputs = tokenizer(input.texts, return_tensors="pt", padding=True, truncation=True, max_length=256)
#         with torch.no_grad():
#             logits = model(**inputs).logits
#         predictions = logits.argmax(-1).tolist()
#         logging.info("Prediction successful")
#         return JSONResponse(content={'predictions': predictions}, status_code=200)
#     except Exception as e:
#         logging.error("Error during prediction: {}".format(str(e)))
#         logging.error(traceback.format_exc())  # Log the full traceback
#         raise HTTPException(status_code=500, detail="Unable to process due to error")
#
#
# if __name__ == '__main__':
#     import uvicorn
#
#     uvicorn.run(app, host=Config.HOST, port=Config.PORT)


# from fastapi import FastAPI, HTTPException, Request
# from pydantic import BaseModel
# from typing import List
# import os
# import torch
# import logging
# from logging.handlers import RotatingFileHandler
# import sys
# import traceback
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from fastapi.responses import JSONResponse
#
# # Append the directory of `main.py` to sys.path to locate config.py
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
# from config import Config
#
# app = FastAPI(
#     title='Natural History Study Article Prediction API',
#     description="""
#     This API utilizes a transformer-based machine learning model to predict the relevance of articles for Natural History Studies. It is specifically designed to handle batches of article abstracts and assess their relevance to the study of rare diseases.
#
#     Input:
#
#     The API requires a JSON object with a key named "texts" that contains a list of article abstracts as strings. Each string should represent the abstract of a research article. The API evaluates each abstract individually to generate predictions.
#
#     Output:
#
#     The response is a JSON object with a key "predictions" that contains an array of integers. Each integer represents a category or outcome for each abstract provided in the input. Predictions are returned in the same order as the input texts.
#
#     A prediction of '0' indicates that the article is either not primarily experimental or not focused on the natural history of a disease.
#     A prediction of '1' indicates that the article's primary contribution is relevant to the study of the natural history of a rare disease.
#
#     Error Handling:
#
#     If the "texts" list is empty, improperly formatted, or not provided, the API will return a 400 Bad Request error, detailing the nature of the input error with an appropriate message.
#     The API accepts empty strings within the list and provides a prediction result for each, including empty strings.
#
#     For more information, please visit our Github repository at: https://github.com/ncats/RDAS/tree/minghui_development/NaturalHistory_Transformer_API_v1.0.
#     """,
#     version="1.0"
# )
#
# # Configure logging
# if not os.path.exists('logs'):
#     os.mkdir('logs')
# file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
# file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
# logging.getLogger().addHandler(file_handler)
# logging.getLogger().setLevel(logging.INFO)
# logging.info('Application startup')
#
# # Load model and tokenizer
# model_path = Config.MODEL_PATH
# print(f"Loading model from: {model_path}")
# tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
# model = AutoModelForSequenceClassification.from_pretrained(model_path)
# model.eval()
#
#
# class TextInput(BaseModel):
#     texts: List[str]
#
#
# @app.post("/article_prediction_api/v1/predict")
# async def predict(input: TextInput):
#     if not input.texts:
#         raise HTTPException(status_code=400, detail="Texts field cannot be empty.")
#
#     try:
#         # Replace the placeholder with the example string
#         input.texts = ["test string1" if text == "string" else text for text in input.texts]
#
#         inputs = tokenizer(input.texts, return_tensors="pt", padding=True, truncation=True, max_length=256)
#         with torch.no_grad():
#             logits = model(**inputs).logits
#         predictions = logits.argmax(-1).tolist()
#         logging.info("Prediction successful")
#         return JSONResponse(content={'predictions': predictions}, status_code=200)
#     except Exception as e:
#         logging.error("Error during prediction: {}".format(str(e)))
#         logging.error(traceback.format_exc())  # Log the full traceback
#         raise HTTPException(status_code=500, detail="Unable to process due to error")
#
#
# if __name__ == '__main__':
#     import uvicorn
#
#     uvicorn.run(app, host=Config.HOST, port=Config.PORT)

############################################################################################
# from fastapi import FastAPI, HTTPException, Request
# from pydantic import BaseModel
# from typing import List
# import os
# import torch
# import logging
# from logging.handlers import RotatingFileHandler
# import sys
# import traceback
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from fastapi.responses import JSONResponse
#
# # Append the directory of `main.py` to sys.path to locate config.py
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
# from config import Config
#
# app = FastAPI(
#     title='Natural History Study Article Prediction API',
#     description="""
#     This API utilizes a transformer-based machine learning model to predict the relevance of articles for Natural History Studies. It is specifically designed to handle batches of article abstracts and assess their relevance to the study of rare diseases.
#
#     Input:
#
#     The API requires a JSON object with a key named "texts" that contains a list of article abstracts as strings. Each string should represent the abstract of a research article. The API evaluates each abstract individually to generate predictions.
#
#     Output:
#
#     The response is a JSON object with a key "predictions" that contains an array of integers. Each integer represents a category or outcome for each abstract provided in the input. Predictions are returned in the same order as the input texts.
#
#     A prediction of '0' indicates that the article is either not primarily experimental or not focused on the natural history of a disease.
#     A prediction of '1' indicates that the article's primary contribution is relevant to the study of the natural history of a rare disease.
#
#     Error Handling:
#
#     If the "texts" list is empty, improperly formatted, or not provided, the API will return a 400 Bad Request error, detailing the nature of the input error with an appropriate message.
#     The API accepts empty strings within the list and provides a prediction result for each, including empty strings.
#
#     For more information, please visit our Github repository at: https://github.com/ncats/RDAS/tree/minghui_development/NaturalHistory_Transformer_API_v1.0.
#     """,
#     version="1.0"
# )
#
# # Configure logging
# if not os.path.exists('logs'):
#     os.mkdir('logs')
# file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
# file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
# logging.getLogger().addHandler(file_handler)
# logging.getLogger().setLevel(logging.INFO)
# logging.info('Application startup')
#
# # Load model and tokenizer
# model_path = Config.MODEL_PATH
# print(f"Loading model from: {model_path}")
# tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
# model = AutoModelForSequenceClassification.from_pretrained(model_path)
# model.eval()
#
#
# class TextInput(BaseModel):
#     texts: List[str]
#
#     class Config:
#         schema_extra = {
#             "example": {
#                 "texts": [
#                     "This is a sample abstract for testing.",
#                     "The natural history, prognostication and optimal treatment of Richter transformation developed from chronic \nlymphocytic leukemia (CLL) are not well defined. We report the clinical characteristics and outcomes of a large series of \nbiopsy-confirmed Richter transformation (diffuse large B-cell lymphoma or high grade B-cell lymphoma, n=204) cases diagnosed \nfrom 1993 to 2018. After a median follow up of 67.0 months, the median overall survival (OS) was 12.0 months. Patients who \nreceived no prior treatment for CLL had significantly better OS (median 46.3 vs. 7.8 months; P<0.001). Patients with elevated \nlactate dehydrogenase (median 6.2 vs. 39.9 months; P<0.0001) or TP53 disruption (median 8.3 vs. 12.8 months; P=0.046) had worse\n OS than those without. Immunoglobulin heavy chain variable region gene mutation, cell of origin, Myc/Bcl-2 double expression and\n  MYC/BCL2/BCL6 double-/triple-hit status were not associated with OS. In multivariable Cox regression, elevated lactate dehydrogenase\n   [Hazard ratio (HR) 2.3, 95% Confidence Interval (CI): 1.3-4.1; P=0.01], prior CLL treatment (HR 2.0, 95%CI: 1.2-3.5; P=0.01), \n   and older age (HR 1.03, 95%CI: 1.01-1.05; P=0.01) were associated with worse OS. Twenty-four (12%) patients underwent stem cell \n   transplant (20 autologous and 4 allogeneic), and had a median post-transplant survival of 55.4 months. In conclusion, the overall \n   outcome of Richter transformation is poor. Richter transformation developed in patients with untreated CLL has significantly better \n   survival. Stem cell transplant may benefit select patients.\n"
#
#                 ]
#             }
#         }
#
#
# @app.post("/article_prediction_api/v1/predict")
# async def predict(input: TextInput):
#     if not input.texts:
#         raise HTTPException(status_code=400, detail="Texts field cannot be empty.")
#
#     try:
#         inputs = tokenizer(input.texts, return_tensors="pt", padding=True, truncation=True, max_length=256)
#         with torch.no_grad():
#             logits = model(**inputs).logits
#         predictions = logits.argmax(-1).tolist()
#         logging.info("Prediction successful")
#         return JSONResponse(content={'predictions': predictions}, status_code=200)
#     except Exception as e:
#         logging.error("Error during prediction: {}".format(str(e)))
#         logging.error(traceback.format_exc())  # Log the full traceback
#         raise HTTPException(status_code=500, detail="Unable to process due to error")
#
#
# if __name__ == '__main__':
#     import uvicorn
#
#     uvicorn.run(app, host=Config.HOST, port=Config.PORT)

###############################################################################################

# from fastapi import FastAPI, HTTPException, Request
# from pydantic import BaseModel
# from typing import List
# import os
# import torch
# import logging
# from logging.handlers import RotatingFileHandler
# import sys
# import traceback
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from fastapi.responses import JSONResponse
#
# # Append the directory of `main.py` to sys.path to locate config.py
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
# from config import Config
#
# app = FastAPI(
#     title='Natural History Study Article Prediction API',
#     description="""
#     This API utilizes a transformer-based machine learning model to predict the relevance of articles for Natural History Studies. It is specifically designed to handle batches of article abstracts and assess their relevance to the study of rare diseases.
#
#     Input:
#
#     The API requires a JSON object with a key named "texts" that contains a list of article abstracts as strings. Each string should represent the abstract of a research article. The API evaluates each abstract individually to generate predictions.
#
#     Output:
#
#     The response is a JSON object with a key "predictions" that contains an array of integers. Each integer represents a category or outcome for each abstract provided in the input. Predictions are returned in the same order as the input texts.
#
#     A prediction of '0' indicates that the article is either not primarily experimental or not focused on the natural history of a disease.
#     A prediction of '1' indicates that the article's primary contribution is relevant to the study of the natural history of a rare disease.
#
#     Error Handling:
#
#     If the "texts" list is empty, improperly formatted, or not provided, the API will return a 400 Bad Request error, detailing the nature of the input error with an appropriate message.
#     The API accepts empty strings within the list and provides a prediction result for each, including empty strings.
#
#     For more information, please visit our Github repository at: https://github.com/ncats/RDAS/tree/minghui_development/NaturalHistory_Transformer_API_v1.0.
#     """,
#     version="1.0"
# )
#
# # Configure logging
# if not os.path.exists('logs'):
#     os.mkdir('logs')
# file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
# file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
# logging.getLogger().addHandler(file_handler)
# logging.getLogger().setLevel(logging.INFO)
# logging.info('Application startup')
#
# # Load model and tokenizer
# model_path = Config.MODEL_PATH
# print(f"Loading model from: {model_path}")
# tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
# model = AutoModelForSequenceClassification.from_pretrained(model_path)
# model.eval()
#
#
# class TextInput(BaseModel):
#     texts: List[str]
#
#     class Config:
#         schema_extra = {
#             "example": {
#                 "texts": [
#                     "This is a sample abstract for testing.",
#                     "The natural history, prognostication and optimal treatment of Richter transformation developed from chronic \nlymphocytic leukemia (CLL) are not well defined. We report the clinical characteristics and outcomes of a large series of \nbiopsy-confirmed Richter transformation (diffuse large B-cell lymphoma or high grade B-cell lymphoma, n=204) cases diagnosed \nfrom 1993 to 2018. After a median follow up of 67.0 months, the median overall survival (OS) was 12.0 months. Patients who \nreceived no prior treatment for CLL had significantly better OS (median 46.3 vs. 7.8 months; P<0.001). Patients with elevated \nlactate dehydrogenase (median 6.2 vs. 39.9 months; P<0.0001) or TP53 disruption (median 8.3 vs. 12.8 months; P=0.046) had worse\n OS than those without. Immunoglobulin heavy chain variable region gene mutation, cell of origin, Myc/Bcl-2 double expression and\n  MYC/BCL2/BCL6 double-/triple-hit status were not associated with OS. In multivariable Cox regression, elevated lactate dehydrogenase\n   [Hazard ratio (HR) 2.3, 95% Confidence Interval (CI): 1.3-4.1; P=0.01], prior CLL treatment (HR 2.0, 95%CI: 1.2-3.5; P=0.01), \n   and older age (HR 1.03, 95%CI: 1.01-1.05; P=0.01) were associated with worse OS. Twenty-four (12%) patients underwent stem cell \n   transplant (20 autologous and 4 allogeneic), and had a median post-transplant survival of 55.4 months. In conclusion, the overall \n   outcome of Richter transformation is poor. Richter transformation developed in patients with untreated CLL has significantly better \n   survival. Stem cell transplant may benefit select patients.\n"
#                 ]
#             }
#         }
#
#
# @app.post("/article_prediction_api/v1/predict")
# async def predict(input: TextInput):
#     if not input.texts:
#         raise HTTPException(status_code=400, detail="Texts field cannot be empty.")
#
#     try:
#         inputs = tokenizer(input.texts, return_tensors="pt", padding=True, truncation=True, max_length=256)
#         with torch.no_grad():
#             logits = model(**inputs).logits
#         predictions = logits.argmax(-1).tolist()
#         logging.info("Prediction successful")
#         return JSONResponse(content={'predictions': predictions}, status_code=200)
#     except Exception as e:
#         logging.error("Error during prediction: {}".format(str(e)))
#         logging.error(traceback.format_exc())  # Log the full traceback
#         raise HTTPException(status_code=500, detail="Unable to process due to error")
#
# if __name__ == '__main__':
#     import uvicorn
#
#     uvicorn.run(app, host=Config.HOST, port=Config.PORT)

##################################################################################################################

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List
import os
import torch
import logging
from logging.handlers import RotatingFileHandler
import sys
import traceback
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from fastapi.responses import JSONResponse

# Append the directory of `main.py` to sys.path to locate config.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))
from config import Config

app = FastAPI(
    title='Natural History Study Article Prediction API',
    description="""
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
    """,
    version="1.0"
)

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/application.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logging.getLogger().addHandler(file_handler)
logging.getLogger().setLevel(logging.INFO)
logging.info('Application startup')

# Load model and tokenizer
model_path = Config.MODEL_PATH
print(f"Loading model from: {model_path}")
tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(model_path)
model.eval()


class TextInput(BaseModel):
    texts: List[str]

    class Config:
        schema_extra = {
            "example": {
                "texts": [
                    "This is a sample abstract for testing.",
                    "The natural history, prognostication and optimal treatment of Richter transformation developed from chronic \nlymphocytic leukemia (CLL) are not well defined. We report the clinical characteristics and outcomes of a large series of \nbiopsy-confirmed Richter transformation (diffuse large B-cell lymphoma or high grade B-cell lymphoma, n=204) cases diagnosed \nfrom 1993 to 2018. After a median follow up of 67.0 months, the median overall survival (OS) was 12.0 months. Patients who \nreceived no prior treatment for CLL had significantly better OS (median 46.3 vs. 7.8 months; P<0.001). Patients with elevated \nlactate dehydrogenase (median 6.2 vs. 39.9 months; P<0.0001) or TP53 disruption (median 8.3 vs. 12.8 months; P=0.046) had worse\n OS than those without. Immunoglobulin heavy chain variable region gene mutation, cell of origin, Myc/Bcl-2 double expression and\n  MYC/BCL2/BCL6 double-/triple-hit status were not associated with OS. In multivariable Cox regression, elevated lactate dehydrogenase\n   [Hazard ratio (HR) 2.3, 95% Confidence Interval (CI): 1.3-4.1; P=0.01], prior CLL treatment (HR 2.0, 95%CI: 1.2-3.5; P=0.01), \n   and older age (HR 1.03, 95%CI: 1.01-1.05; P=0.01) were associated with worse OS. Twenty-four (12%) patients underwent stem cell \n   transplant (20 autologous and 4 allogeneic), and had a median post-transplant survival of 55.4 months. In conclusion, the overall \n   outcome of Richter transformation is poor. Richter transformation developed in patients with untreated CLL has significantly better \n   survival. Stem cell transplant may benefit select patients.\n"
                ]
            }
        }


@app.post("/article_prediction_api/v1/predict")
async def predict(input: TextInput):
    data = input.dict()

    if 'texts' not in data or not isinstance(data['texts'], list):
        raise HTTPException(status_code=400, detail='Invalid input. "texts" must be a list.')

    if not data['texts']:
        raise HTTPException(status_code=400, detail="Texts field cannot be empty.")

    try:
        inputs = tokenizer(data['texts'], return_tensors="pt", padding=True, truncation=True, max_length=256)
        with torch.no_grad():
            logits = model(**inputs).logits
        predictions = logits.argmax(-1).tolist()
        logging.info("Prediction successful")
        return JSONResponse(content={'predictions': predictions}, status_code=200)
    except Exception as e:
        logging.error("Error during prediction: {}".format(str(e)))
        logging.error(traceback.format_exc())  # Log the full traceback
        raise HTTPException(status_code=500, detail="Unable to process due to error")


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host=Config.HOST, port=Config.PORT)
