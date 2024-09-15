import unittest
from unittest.mock import patch, MagicMock
import requests
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from conexion_api import load_credentials, make_request, save_to_csv

class TestLoadCredentials(unittest.TestCase):

    @patch('conexion_api.os.getenv')
    def test_load_credentials(self, mock_getenv):
        # Simular las credenciales desde las variables de entorno
        mock_getenv.side_effect = lambda var: {
            'vclient_id': 'test_id',
            'vclient_secret': 'test_secret'
        }.get(var)
        
        # Ejecutar la función de carga de credenciales
        vclient_id, vclient_secret = load_credentials()
        self.assertEqual(vclient_id, 'test_id')
        self.assertEqual(vclient_secret, 'test_secret')

    @patch('conexion_api.os.getenv', side_effect=lambda var: None)
    def test_load_credentials_missing_values(self, mock_getenv):
        # Simular que las variables de entorno tienen valores None
        with self.assertRaises(ValueError):
            load_credentials()

    @patch('conexion_api.os.path.exists', return_value=False)
    @patch('conexion_api.dotenv_values')
    def test_load_credentials_file_not_found(self, mock_dotenv_values, mock_exists):
        # Simular que el archivo .env no existe
        with self.assertRaises(ValueError):
            load_credentials(env_path="dummy_path")

class TestMakeRequest(unittest.TestCase):

    @patch('conexion_api.requests.Session.get')
    def test_make_request_success(self, mock_get):
        # Simular una respuesta exitosa
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': {'stations': ['station1', 'station2']}}
        mock_get.return_value = mock_response
        
        session = requests.Session()
        url = 'http://testurl.com'
        result = make_request(session, url)
        self.assertEqual(result, ['station1', 'station2'])

    @patch('conexion_api.requests.Session.get')
    def test_make_request_failure(self, mock_get):
        # Simular una falla en la solicitud
        mock_get.side_effect = requests.exceptions.HTTPError("Test error")
        
        session = requests.Session()
        url = 'http://testurl.com'
        result = make_request(session, url)
        self.assertIsNone(result)

class TestSaveToCSV(unittest.TestCase):

    @patch('conexion_api.pd.json_normalize')
    @patch('conexion_api.pd.DataFrame.to_csv')
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
