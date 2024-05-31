# Natural History Study Article Prediction API

This API utilizes a transformer-based machine learning model to predict the relevance of articles for Natural History Studies. It is specifically designed to handle batches of article abstracts and assess their relevance to the study of rare diseases.

## Table of Contents
- [Features](#features)
- [Getting Started](#getting-started)
- [Running the Application](#running-the-application)
- [API Documentation](#api-documentation)
- [Feedback](#feedback)

## Features

- Predict the relevance of article abstracts to the study of rare diseases.
- Batch processing of multiple article abstracts.
- Robust error handling and logging.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine.
- Python 3.8 if you prefer running locally without Docker.

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/yourrepository.git
   cd yourrepository
   ```
2. **Create a .env file:**
   ```bash
   cp .env.example .env
   ```
   Update the .env file with the appropriate configuration values.
### Running the Application

#### Using Docker
1. **Build the Docker images:**
   ```bash
   docker-compose build
   ```
2. **Start the FastAPI application:**
   ```bash
   docker-compose up web
   ```
   The application will be accessible at http://localhost:5000. Go to http://localhost:5000/docs for usage information.
#### Running Locally
1. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
   
2. **Install the dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Run the application:**
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 5000
   ```
   The application will be accessible at http://localhost:5000. Go to http://localhost:5000/docs for usage information.

### API Documentation

The API documentation is available via Swagger UI and can be accessed at:
   ```bash
      http://localhost:5000/docs
   ```   
 You can use the interactive documentation to test the endpoints and understand the API.


### Feedback:
   If you have any feedback or questions, please contact minghui.ao@nih.gov, qian.zhu@nih.gov.