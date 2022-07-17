FROM neerteam/geopandas
EXPOSE 8501
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . .
CMD streamlit run --server.port $PORT main.py