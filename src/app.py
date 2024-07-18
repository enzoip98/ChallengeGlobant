from flask import Flask, request, jsonify, Response
from pyspark.sql import SparkSession
from database import engine
import pandas
import tempfile
import os

app = Flask(__name__)

#Inicializar Spark
spark = SparkSession.builder.appName("GlobantChallenge").config("spark.jars","src\sqlite-jdbc-3.46.0.0.jar").getOrCreate()

@app.route("/upload_csv",methods = ['POST'])
def upload_csv():
    try:
        #Obtener el archivo CSV de la solicitud
        file = request.files['file']

        #Extraer el nombre del archivo temporal
        filename = file.filename
        table_name = os.path.splitext(filename)[0]

        # Crear un archivo temporal para guardar el CSV
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        file_path = temp_file.name
        temp_file.close()
        
        #Guarda el archivo temporal
        file.save(file_path)

        df = spark.read.option("header",False).option("delimiter",",").csv(file_path)
        #Leer las columnas de la tabla existente
        existing_table = spark.read.format('jdbc').options(
            url='jdbc:sqlite:test.db',
            dbtable=table_name,
            driver='org.sqlite.JDBC'
        ).load()
        existing_columns = existing_table.columns
        #Renombrar columnas
        for i, col in enumerate(existing_columns):
            df = df.withColumnRenamed(f"_c{i}", col)
        
        df.write.format("jdbc").options(
            url='jdbc:sqlite:test.db',
            dbtable=table_name,
            driver='org.sqlite.JDBC'
        ).mode('overwrite').save()

        os.remove(file_path)

        return jsonify({'message': 'CSV file successfully uploaded and saved to database'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/request1', methods=['GET'])
def request1():
    try:
        request = """
WITH joined_table AS (
    SELECT 
        department,
        job,
        hired_employees.id,
        CASE WHEN strftime('%m', datetime) BETWEEN '01' AND '03' THEN 'X' ELSE NULL END as Q1,
        CASE WHEN strftime('%m', datetime) BETWEEN '04' AND '06' THEN 'X' ELSE NULL END as Q2,
        CASE WHEN strftime('%m', datetime) BETWEEN '07' AND '09' THEN 'X' ELSE NULL END as Q3,
        CASE WHEN strftime('%m', datetime) BETWEEN '10' AND '12' THEN 'X' ELSE NULL END as Q4
    FROM hired_employees 
    LEFT JOIN 
        departments ON hired_employees.department_id = departments.id
    LEFT JOIN 
        jobs ON hired_employees.job_id = jobs.id
    WHERE strftime('%Y', datetime) = '2021'
)
SELECT 
    department,
    job,
    COUNT(Q1) as Q1,
    COUNT(Q2) as Q2,
    COUNT(Q3) as Q3,
    COUNT(Q4) as Q4
FROM 
    joined_table
GROUP BY
    department,
    job
ORDER BY
    department,
    job
        """
        # Leer todos los datos de la tabla
        df = spark.read.format('jdbc').options(
            url='jdbc:sqlite:test.db',
            query=request,
            driver='org.sqlite.JDBC'
        ).load()

        pandas_df = df.toPandas()
        pandas_df = pandas_df.apply(pandas.to_numeric, errors='ignore')
        result_html = pandas_df.to_html(index=False)
        
        return Response(result_html, mimetype='text/html')
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/request2', methods=['GET'])
def request2():
    try:
        request = """
WITH joined_table AS (
    SELECT 
        departments.id as id,
        department,
        hired_employees.id as employee_id
    FROM hired_employees 
    LEFT JOIN 
        departments ON hired_employees.department_id = departments.id
    LEFT JOIN 
        jobs ON hired_employees.job_id = jobs.id
    WHERE strftime('%Y', datetime) = '2021'
),
department_hires AS (
    SELECT
        id,
        department,
        count(employee_id) as hired
    FROM 
        joined_table    
    GROUP BY
        department
),
average_hires AS (
    SELECT AVG(hired) as avg_hired
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
    dh.hired DESC
        """
        # Leer todos los datos de la tabla
        df = spark.read.format('jdbc').options(
            url='jdbc:sqlite:test.db',
            query=request,
            driver='org.sqlite.JDBC'
        ).load()

        pandas_df = df.toPandas()
        pandas_df = pandas_df.apply(pandas.to_numeric, errors='ignore')
        result_html = pandas_df.to_html(index=False)
        
        return Response(result_html, mimetype='text/html')
    except Exception as e:
        return jsonify({'error': str(e)}), 400
if __name__ == '__main__':
    app.run(debug=True)