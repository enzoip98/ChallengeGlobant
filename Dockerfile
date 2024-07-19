# Utiliza una imagen base de Python
FROM python:3.10-slim

# Crea un grupo y un usuario no root
RUN groupadd -r appgroup && useradd -m -r -g appgroup appuser

# Establece el directorio de trabajo
WORKDIR /app

# Copia el archivo requirements.txt en el contenedor
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación en el contenedor
COPY . .

# Cambia los permisos de los archivos
RUN chown -R appuser:appgroup /app

# Cambia al usuario no root
USER appuser

# Establece la variable de entorno para Flask
ENV FLASK_APP=app.py

# Expone el puerto en el que Flask está corriendo
EXPOSE 5000

# Comando para correr la aplicación
CMD ["flask", "run", "--host=0.0.0.0","--port=5000"]