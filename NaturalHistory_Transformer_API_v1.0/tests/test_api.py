# import unittest
# import json
# from fastapi.testclient import TestClient
# from app.main import app
#
# client = TestClient(app)
#
# class APITestCase(unittest.TestCase):
#
#     def test_predict_endpoint(self):
#         sample_text = ["Example text for testing."]
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
#         self.assertEqual(response.status_code, 200)
#         data = response.json()
#         self.assertIn('predictions', data)
#         self.assertIsInstance(data['predictions'], list)
#         self.assertEqual(len(data['predictions']), len(sample_text))
#
#     def test_predict_endpoint_with_empty_input(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": []})
#         self.assertEqual(response.status_code, 400)
#
#     def test_predict_endpoint_error_for_non_list_input(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": "This should be a list"})
#         self.assertEqual(response.status_code, 422)
#
#     def test_predict_endpoint_error_for_wrong_key(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"text": ["This is incorrect key"]})
#         self.assertEqual(response.status_code, 422)
#
# if __name__ == '__main__':
#     unittest.main()

# import unittest
# import json
# from fastapi.testclient import TestClient
# from app.main import app
#
# client = TestClient(app)
#
# class APITestCase(unittest.TestCase):
#
#     def test_predict_endpoint(self):
#         sample_text = ["Example text for testing."]
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
#         self.assertEqual(response.status_code, 200)
#         data = response.json()
#         self.assertIn('predictions', data)
#         self.assertIsInstance(data['predictions'], list)
#         self.assertEqual(len(data['predictions']), len(sample_text))
#
#     def test_predict_endpoint_with_empty_input(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": []})
#         self.assertEqual(response.status_code, 400)
#         data = response.json()
#         self.assertIn('detail', data)
#         self.assertEqual(data['detail'], "Texts field cannot be empty.")
#
#     def test_predict_endpoint_error_for_non_list_input(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": "This should be a list"})
#         self.assertEqual(response.status_code, 400)
#         data = response.json()
#         self.assertIn('detail', data)
#         self.assertEqual(data['detail'], 'Invalid input. "texts" must be a list.')
#
#     def test_predict_endpoint_error_for_wrong_key(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"text": ["This is incorrect key"]})
#         self.assertEqual(response.status_code, 400)
#         data = response.json()
#         self.assertIn('detail', data)
#         self.assertEqual(data['detail'], 'Invalid input. "texts" must be a list.')
#
# if __name__ == '__main__':
#     unittest.main()


# import unittest
# import json
# from fastapi.testclient import TestClient
# from app.main import app
#
# client = TestClient(app)
#
# class APITestCase(unittest.TestCase):
#
#     def test_predict_endpoint(self):
#         sample_text = ["Example text for testing."]
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
#         self.assertEqual(response.status_code, 200)
#         data = response.json()
#         self.assertIn('predictions', data)
#         self.assertIsInstance(data['predictions'], list)
#         self.assertEqual(len(data['predictions']), len(sample_text))
#
#     def test_predict_endpoint_with_empty_input(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": []})
#         self.assertEqual(response.status_code, 400)
#         data = response.json()
#         self.assertIn('detail', data)
#         self.assertEqual(data['detail'], "Texts field cannot be empty.")
#
#     def test_predict_endpoint_error_for_non_list_input(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"texts": "This should be a list"})
#         self.assertEqual(response.status_code, 422)  # Expecting 422 from Pydantic validation
#         data = response.json()
#         self.assertIn('detail', data)
#
#     def test_predict_endpoint_error_for_wrong_key(self):
#         response = client.post('/article_prediction_api/v1/predict', json={"text": ["This is incorrect key"]})
#         self.assertEqual(response.status_code, 422)  # Expecting 422 from Pydantic validation
#         data = response.json()
#         self.assertIn('detail', data)
#
# if __name__ == '__main__':
#     unittest.main()

import unittest
import json
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

class APITestCase(unittest.TestCase):

    def test_predict_endpoint(self):
        sample_text = ["Example text for testing."]
        response = client.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('predictions', data)
        self.assertIsInstance(data['predictions'], list)
        self.assertEqual(len(data['predictions']), len(sample_text))

    def test_predict_endpoint_with_empty_input(self):
        response = client.post('/article_prediction_api/v1/predict', json={"texts": []})
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertIn('detail', data)
        self.assertEqual(data['detail'], "Texts field cannot be empty.")

    def test_predict_endpoint_error_for_non_list_input(self):
        response = client.post('/article_prediction_api/v1/predict', json={"texts": "This should be a list"})
        self.assertEqual(response.status_code, 422)  # Expecting 422 from Pydantic validation
        data = response.json()
        self.assertIn('detail', data)

    def test_predict_endpoint_error_for_wrong_key(self):
        response = client.post('/article_prediction_api/v1/predict', json={"text": ["This is incorrect key"]})
        self.assertEqual(response.status_code, 422)  # Expecting 422 from Pydantic validation
        data = response.json()
        self.assertIn('detail', data)

if __name__ == '__main__':
    unittest.main()
