# Article Prediction API

## Overview
This Flask application provides a simple prediction API that utilizes a machine learning model to classify articles. It is designed to showcase how to set up a basic API for text classification tasks using Flask and the Transformers library.

## Features
- **Predict Endpoint**: Accepts both GET and POST requests for flexibility in testing and integration.
- **Logging**: Includes basic logging setup to track operations and errors.
- **Docker Integration**: Comes Docker-ready with a Dockerfile and docker-compose.yml for easy deployment and scaling.

## Requirements
- Python 3.8+
- Flask
- Transformers
- PyTorch

## Setup Instructions

### Local Setup
1. **Clone the repository:**
   ```bash
   git clone https://github.com/..../NaturalHistory_Transformer_API_v1.0.git
   cd NaturalHistory_Transformer_API_v1.0

2. **Install dependencies:**
   pip install -r requirements.txt

3. **Run the application:**
   python app/main.py

This will start the Flask server on localhost:5000.

## Docker Setup

1. **Build the Docker image:**
docker-compose build

2.**Run the container:**
docker-compose up

## Usage

### Making Predictions

**POST Request:**
Use a tool like Postman or curl to send a POST request:
curl -X POST http://localhost:5000/predict -H "Content-Type: application/json" -d "{\"texts\": [\"sample text\"]}"

For multiple texts:
curl -X POST http://localhost:5000/predict -H "Content-Type: application/json" -d "{\"texts\": [\"sample text1\", \"sample text2\"]}"

Python Script:
```bash
new_abstracts = ["text1", "text2",....]
data = {
    "texts": new_abstracts
}
response = requests.post(url, json=data)
```


**GET Request:**
Open a browser and go to:
http://localhost:5000/predict?texts=sample_text

For multiple texts:
http://localhost:5000/predict?texts=sample_text1&texts=sample_text2

**Accessing the Home Page**
Navigate to http://localhost:5000/index in your web browser to see a welcome message.

## Contact
For questions or support, please contact minghui.ao@nih.gov, qian.zhu@nih.gov
