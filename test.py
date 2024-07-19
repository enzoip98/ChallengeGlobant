import pytest
import requests
from flask import Flask
from app import app  # Importa tu aplicación Flask aquí

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_upload_csv(client):
    # Test para el endpoint /upload_csv
    file_key = 'jobs.csv'  # Asegúrate de tener un archivo test.csv en tu bucket S3 para este test
    data = {
        'file_key': file_key
    }
    response = client.post('/upload_csv', data=data)
    assert response.status_code == 200
    assert b'CSV file successfully uploaded and saved to database' in response.data

def test_request1(client):
    # Test para el endpoint /request1
    response = client.get('/request1')
    assert response.status_code == 200
    assert b'<table' in response.data  # Verifica que la respuesta contenga una tabla HTML

def test_request2(client):
    # Test para el endpoint /request2
    response = client.get('/request2')
    assert response.status_code == 200
    assert b'<table' in response.data  # Verifica que la respuesta contenga una tabla HTML