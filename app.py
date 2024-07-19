from flask import Flask, request, jsonify, Response
import boto3
import pandas
import tempfile
import os
from sqlalchemy import create_engine, text
import passwords
dbhost = "globantchallenge.cr64k06oyhhu.us-east-2.rds.amazonaws.com"
dbport = "5432"
dbname = "gcdb"
DB_URL = f'postgresql://wasabi:{passwords.db_password}@{dbhost}:{dbport}/{dbname}'
engine = create_engine(DB_URL)
app = Flask(__name__)

S3_bucket = "gc-bucket-wasabi"
s3 = boto3.client(
    's3',
    aws_access_key_id=passwords.aws_access_key_id,  # Reemplaza con tu Access Key ID
    aws_secret_access_key=passwords.aws_secret_access_key  # Reemplaza con tu Secret Access Key
)


@app.route("/upload_csv",methods = ['POST'])
def upload_csv():
    try:
        #Obtener el archivo CSV de la solicitud
        file = request.form['file_key']
        table_name = file.split('.')[0]
        print(f'Recibido archivo: {file} para la tabla: {table_name}')

        # Crear un archivo temporal para guardar el CSV
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        print(f'Descargando archivo desde S3 bucket: {S3_bucket}, key: {file}')
        s3.download_file(S3_bucket,file,temp_file.name)
        temp_file.close()
        print(f'Archivo descargado y guardado temporalmente en: {temp_file.name}')

        df = pandas.read_csv(temp_file.name,header=None)
        print(f'CSV leído con pandas, mostrando primeras filas: {df.head()}')
        #Leer las columnas de la tabla existente
        existing_columns = pandas.read_sql(f'SELECT * FROM {table_name} LIMIT 0', engine).columns
        print(f'Columnas existentes en la tabla {table_name}: {existing_columns}')
        df.columns = existing_columns
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Datos guardados en la tabla {table_name}')
        os.remove(temp_file.name)
        print(f'Archivo temporal eliminado: {temp_file.name}')
        return jsonify({'message': 'CSV file successfully uploaded and saved to database'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/request1', methods=['GET'])
def request1():
    try:
        request = """
WITH joined_table AS (
    SELECT 
        d.department,
        j.job,
        he.id,
        CASE WHEN EXTRACT(MONTH FROM he.datetime::timestamp) BETWEEN 1 AND 3 THEN 'X' ELSE NULL END AS Q1,
        CASE WHEN EXTRACT(MONTH FROM he.datetime::timestamp) BETWEEN 4 AND 6 THEN 'X' ELSE NULL END AS Q2,
        CASE WHEN EXTRACT(MONTH FROM he.datetime::timestamp) BETWEEN 7 AND 9 THEN 'X' ELSE NULL END AS Q3,
        CASE WHEN EXTRACT(MONTH FROM he.datetime::timestamp) BETWEEN 10 AND 12 THEN 'X' ELSE NULL END AS Q4
    FROM hired_employees he 
    LEFT JOIN departments d ON he.department_id = d.id
    LEFT JOIN jobs j ON he.job_id = j.id
    WHERE EXTRACT(YEAR FROM he.datetime::timestamp) = 2021
)
SELECT 
    department,
    job,
    COUNT(Q1) AS Q1,
    COUNT(Q2) AS Q2,
    COUNT(Q3) AS Q3,
    COUNT(Q4) AS Q4
FROM 
    joined_table
GROUP BY
    department,
    job
ORDER BY
    department,
    job;
        """
        with engine.connect() as connection:
            # Leer todos los datos de la tabla
            df = pandas.read_sql(text(request), connection)
        result_html = df.to_html(index=False)
        
        return Response(result_html, mimetype='text/html')
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/request2', methods=['GET'])
def request2():
    try:
        request = """
WITH joined_table AS (
    SELECT 
        d.id AS id,
        d.department,
        he.id AS employee_id
    FROM hired_employees he
    LEFT JOIN departments d ON he.department_id = d.id
    LEFT JOIN jobs j ON he.job_id = j.id
    WHERE EXTRACT(YEAR FROM he.datetime::timestamp) = 2021
),
department_hires AS (
    SELECT
        id,
        department,
        COUNT(employee_id) AS hired
    FROM 
        joined_table    
    GROUP BY
        id, department
),
average_hires AS (
    SELECT AVG(hired) AS avg_hired
    FROM department_hires
)
SELECT 
    dh.id,
    dh.department,
    dh.hired
FROM 
    department_hires dh,
    average_hires ah
WHERE 
    dh.hired > ah.avg_hired
ORDER BY 
    dh.hired DESC;
        """
        with engine.connect() as connection:
            # Leer todos los datos de la tabla
            df = pandas.read_sql(text(request), connection)
        result_html = df.to_html(index=False)
        
        return Response(result_html, mimetype='text/html')
    except Exception as e:
        return jsonify({'error': str(e)}), 400
if __name__ == '__main__':
    app.run(host = "0.0.0.0",port = 80,debug=True)