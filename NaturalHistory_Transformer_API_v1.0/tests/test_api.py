# import unittest
# import json
# from app.main import app
# import os
#
# os.chdir("../app")
# # print(os.getcwd())
#
# class APITestCase(unittest.TestCase):
#
#     def setUp(self):
#         self.app = app.test_client()
#         self.app.testing = True
#
#     def test_predict_endpoint(self):
#         # Define a test case for the predict endpoint
#         sample_text = ["""Historically, surgical correction has been the treatment of choice for benign biliary strictures (BBS). Self-expandable metallic stents (MSs) have been useful for inoperable malignant biliary strictures; however, their use for BBS is controversial and their natural history unknown. To test our hypothesis that MSs provide only short-term benefit, we examined the long-term outcome of MSs for the treatment of BBS. Our goal was to develop a rational approach for treating BBS. Between July 1990 and December 1995, 15 patients had MSs placed for BBS and have been followed up for a mean of 86.3 months (range, 55-120 months). The mean age of the patients was 66.6 years and 12 were women. Stents were placed for surgical injury in 5 patients and underlying disease in 10 patients (lithiasis, 7; pancreatitis, 2; and primary sclerosing cholangitis, 1). One or more MSs (Gianturco-Rosch "Z" for 4 patients and Wallstents for 11 patients) were placed by percutaneous, endoscopic, or combined approaches. We considered patients to have a good clinical outcome if the stent remained patent, they required 2 or fewer invasive interventions, and they had no biliary dilation on subsequent imaging. Metallic stents were successfully placed in all 15 patients, and the mean patency rate was 30.6 months (range, 7-120 months). Five patients (33%) had a good clinical result with stent patency from 55 to 120 months. Ten patients (67%) required more than 2 radiologic and/or endoscopic procedures for recurrent cholangitis and/or obstruction (range, 7-120 months). Five of the 10 patients developed complete stent obstruction at 8, 9, 10, 15, and 120 months and underwent surgical removal of the stent and bilioenteric anastomosis. Four of these 5 patients had strictures from surgical injuries. The patient who had surgical removal 10 years after MS placement developed cholangiocarcinoma. Surgical repair remains the treatment of choice for BBS. Metallic stents should only be considered for poor surgical candidates, intrahepatic biliary strictures, or failed attempts at surgical repair. Most patients with MSs will develop recurrent cholangitis or stent obstruction and require intervention. Chronic inflammation and obstruction may predispose the patient to cholangiocarcinoma.
# """]
#         response = self.app.post('/article_prediction_api/v1/predict', data=json.dumps({"texts": sample_text}),
#                                  content_type='application/json')
#         self.assertEqual(response.status_code, 200)
#         data = json.loads(response.data)
#         self.assertIn('predictions', data)
#         self.assertIsInstance(data['predictions'], list)
#         self.assertEqual(len(data['predictions']), len(sample_text))  # Assuming one prediction per input text
#
#     def test_predict_endpoint(self):
#         # Define a test case for the predict endpoint with valid input
#         sample_text = ["Example text for testing."]
#         response = self.app.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
#         self.assertEqual(response.status_code, 200)
#         data = json.loads(response.data)
#         self.assertIn('predictions', data)
#         self.assertIsInstance(data['predictions'], list)
#         self.assertEqual(len(data['predictions']), len(sample_text))
#
#     def test_predict_endpoint_with_empty_input(self):
#         # Test handling of empty input
#         response = self.app.post('/article_prediction_api/v1/predict', json={"texts": []})
#         self.assertEqual(response.status_code, 400)  # Assuming API should handle empty input with a 400 error
#
#     def test_predict_endpoint_with_large_input(self):
#         # Test handling of larger than typical input
#         sample_text = ["Text"] * 1000  # Large number of input texts
#         response = self.app.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
#         self.assertEqual(response.status_code, 200)  # Assuming API can handle large inputs
#
#     def test_predict_endpoint_error_for_non_list_input(self):
#         # Ensure non-list inputs are handled properly
#         response = self.app.post('/article_prediction_api/v1/predict', json={"texts": "This should be a list"})
#         self.assertEqual(response.status_code, 400)
#
#     def test_predict_endpoint_error_for_wrong_key(self):
#         # Test API's response to requests with wrong JSON key
#         response = self.app.post('/article_prediction_api/v1/predict', json={"text": ["This is incorrect key"]})
#         self.assertEqual(response.status_code, 400)  # API should return a 400 for incorrect JSON key
#
#
#
# # This allows the tests to be executed
# if __name__ == '__main__':
#     unittest.main()


import unittest
import json
from app.main import app

class APITestCase(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_predict_endpoint(self):
        sample_text = ["Example text for testing."]
        response = self.app.post('/article_prediction_api/v1/predict', json={"texts": sample_text})
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('predictions', data)
        self.assertIsInstance(data['predictions'], list)
        self.assertEqual(len(data['predictions']), len(sample_text))

    def test_predict_endpoint_with_empty_input(self):
        response = self.app.post('/article_prediction_api/v1/predict', json={"texts": []})
        self.assertEqual(response.status_code, 400)

    def test_predict_endpoint_error_for_non_list_input(self):
        response = self.app.post('/article_prediction_api/v1/predict', json={"texts": "This should be a list"})
        self.assertEqual(response.status_code, 400)

    def test_predict_endpoint_error_for_wrong_key(self):
        response = self.app.post('/article_prediction_api/v1/predict', json={"text": ["This is incorrect key"]})
        self.assertEqual(response.status_code, 400)

if __name__ == '__main__':
    unittest.main()
