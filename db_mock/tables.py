import psycopg2
from psycopg2 import sql

# --- PostgreSQL connection config ---
DB_CONFIG = {
    "host": "localhost",      
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# --- SQL statements to create tables ---
CREATE_TABLES_SQL = [
    # 1. Train Features
    """
    CREATE TABLE IF NOT EXISTS features (
        feature_date DATE PRIMARY KEY,
        day_type SMALLINT NOT NULL,
        dow SMALLINT NOT NULL,
        rain_average FLOAT,
        arl FLOAT,
        bts FLOAT,
        mrt_blue FLOAT,
        mrt_purple FLOAT,
        srt_red FLOAT,
        mrt_yellow FLOAT,
        mrt_pink FLOAT
    );
    CREATE INDEX IF NOT EXISTS idx_features_date ON features(feature_date);
    """,
    
    # 2. Predictions
    """
    CREATE TABLE IF NOT EXISTS predictions (
        prediction_id SERIAL PRIMARY KEY,
        prediction_date DATE NOT NULL,
        model_name TEXT NOT NULL,
        forecast_horizon SMALLINT NOT NULL,
        arl FLOAT,
        bts FLOAT,
        mrt_blue FLOAT,
        mrt_purple FLOAT,
        srt_red FLOAT,
        mrt_yellow FLOAT,
        mrt_pink FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_predictions_date_model ON predictions(prediction_date, model_name);
    CREATE INDEX IF NOT EXISTS idx_predictions_model ON predictions(model_name);
    """
    
]

def create_tables():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        for sql_stmt in CREATE_TABLES_SQL:
            cursor.execute(sql_stmt)
            print("Executed successfully:\n", sql_stmt.split("\n")[1].strip())
        
        cursor.close()
        conn.close()
        print("All tables created successfully.")
    except Exception as e:
        print("Error creating tables:", e)

if __name__ == "__main__":
    create_tables()
