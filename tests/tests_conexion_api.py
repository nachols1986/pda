import unittest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
import requests
from src.conexion_api import load_credentials, make_request, save_to_csv

class TestLoadCredentials(unittest.TestCase):

    @patch('src.conexion_api.dotenv_values')
    @patch('src.conexion_api.os.getenv')
    def test_load_credentials(self, mock_getenv, mock_dotenv_values):
        
        # Simular las credenciales desde .env
        mock_dotenv_values.return_value = {'vclient_id': 'test_id', 'vclient_secret': 'test_secret'}
        mock_getenv.return_value = None
        
        vclient_id, vclient_secret = load_credentials('path/to/.env')
        self.assertEqual(vclient_id, 'test_id')
        self.assertEqual(vclient_secret, 'test_secret')

    @patch('src.conexion_api.dotenv_values', side_effect=FileNotFoundError)
    def test_load_credentials_file_not_found(self, mock_dotenv_values):
        with self.assertRaises(FileNotFoundError):
            load_credentials('path/to/.env')

    @patch('src.conexion_api.dotenv_values', return_value={'vclient_id': None, 'vclient_secret': None})
    @patch('src.conexion_api.os.getenv', side_effect=lambda x: None)
    def test_load_credentials_missing_values(self, mock_getenv, mock_dotenv_values):
        with self.assertRaises(ValueError):
            load_credentials('path/to/.env')

class TestMakeRequest(unittest.TestCase):

    @patch('src.conexion_api.requests.Session.get')
    def test_make_request_success(self, mock_get):
        
        # Simular una respuesta exitosa
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': {'stations': ['station1', 'station2']}}
        mock_get.return_value = mock_response
        
        session = requests.Session()
        url = 'http://testurl.com'
        result = make_request(session, url)
        self.assertEqual(result, ['station1', 'station2'])

    @patch('src.conexion_api.requests.Session.get')
    def test_make_request_failure(self, mock_get):
        mock_get.side_effect = requests.exceptions.HTTPError("Test error")
        
        session = requests.Session()
        url = 'http://testurl.com'
        result = make_request(session, url)
        self.assertIsNone(result)

class TestSaveToCSV(unittest.TestCase):

    @patch('src.conexion_api.pd.json_normalize')
    @patch('src.conexion_api.pd.DataFrame.to_csv')
    def test_save_to_csv(self, mock_to_csv, mock_json_normalize):
        
        # Simular la conversión a DataFrame y la guardada en CSV
        mock_json_normalize.return_value = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        filename = 'test.csv'
        data = [{'col1': 1, 'col2': 3}, {'col1': 2, 'col2': 4}]
        
        save_to_csv(data, filename)
        mock_json_normalize.assert_called_once_with(data)
        mock_to_csv.assert_called_once_with(filename, index=False)

if __name__ == '__main__':
    unittest.main()
