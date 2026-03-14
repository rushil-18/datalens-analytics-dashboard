from flask import Flask, request, jsonify
from flask_cors import CORS
from psycopg2 import sql
import psycopg2
import os
import pandas as pd

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "database": os.getenv("DB_NAME",     "Analytics_dashboard"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "12345"),
    "port":     os.getenv("DB_PORT",     "5432")
}

def get_conn():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        print(f"DB connection error: {e}")
        raise

def init_db():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id          SERIAL PRIMARY KEY,
            name        TEXT UNIQUE,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("ALTER TABLE datasets ADD COLUMN IF NOT EXISTS row_count INTEGER;")
    cur.execute("ALTER TABLE datasets ADD COLUMN IF NOT EXISTS col_count  INTEGER;")
    conn.commit()
    cur.close()
    conn.close()
    print("Database ready")

init_db()

@app.route("/")
def home():
    return "DataLens Backend running"

@app.route("/upload", methods=["POST"])
def upload_dataset():
    conn = None
    try:
        file         = request.files["file"]
        dataset_name = os.path.splitext(file.filename)[0]

        # Always store uploads in Backend/uploads — regardless of where Flask is launched from
        BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
        upload_folder = os.path.join(BASE_DIR, "uploads")
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, file.filename)
        file.save(file_path)

        df = pd.read_csv(file_path)
        df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]
        df = df.fillna("")

        rows, col_count = df.shape
        column_names    = df.columns.tolist()

        conn = get_conn()
        cur  = conn.cursor()

        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(dataset_name)))

        col_defs = sql.SQL(", ").join(
            sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in column_names
        )
        cur.execute(sql.SQL("CREATE TABLE {} (_row_id SERIAL PRIMARY KEY, {})").format(
            sql.Identifier(dataset_name), col_defs
        ))

        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(dataset_name),
            sql.SQL(", ").join(map(sql.Identifier, column_names)),
            sql.SQL(", ").join(sql.Placeholder() * len(column_names))
        )
        data_rows = [tuple(str(v) for v in row) for _, row in df.iterrows()]
        cur.executemany(insert_sql, data_rows)

        cur.execute("""
            INSERT INTO datasets (name, row_count, col_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (name)
            DO UPDATE SET
                upload_date = CURRENT_TIMESTAMP,
                row_count   = EXCLUDED.row_count,
                col_count   = EXCLUDED.col_count
        """, (dataset_name, rows, col_count))

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "success", "dataset": dataset_name, "rows": rows, "columns": col_count})

    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        return jsonify({"status": "error", "message": str(e)})

@app.route("/datasets", methods=["GET"])
def get_datasets():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM datasets ORDER BY id DESC")

    rows = cur.fetchall()

    datasets = []

    for row in rows:
        datasets.append({
            "id": row[0],
            "name": row[1]
        })

    cur.close()
    conn.close()

    return jsonify(datasets)
        
    

@app.route("/dataset/<n>", methods=["GET"])
def get_dataset(n):
    conn = None
    try:
        limit = int(request.args.get("limit", 500))
        conn  = get_conn()
        cur   = conn.cursor()
        cur.execute(
            sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(n)),
            (limit,)
        )
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        data = []
        for row in rows:
            record = {
                col: (str(row[i]) if row[i] is not None else "")
                for i, col in enumerate(columns)
                if col != "_row_id"
            }
            data.append(record)
        return jsonify(data)

    except Exception as e:
        if conn: conn.close()
        return jsonify({"status": "error", "message": str(e)})

@app.route("/stats/<n>", methods=["GET"])
def get_stats(n):
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(n)))
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        df = pd.DataFrame(rows, columns=columns).drop(columns=["_row_id"], errors="ignore")
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")

        numeric_cols     = df.select_dtypes(include="number").columns.tolist()
        categorical_cols = df.select_dtypes(exclude="number").columns.tolist()

        numeric_stats = {}
        for col in numeric_cols:
            s = df[col]
            numeric_stats[col] = {
                "mean":       round(float(s.mean()),   4) if not s.isna().all() else None,
                "median":     round(float(s.median()), 4) if not s.isna().all() else None,
                "std":        round(float(s.std()),    4) if not s.isna().all() else None,
                "min":        round(float(s.min()),    4) if not s.isna().all() else None,
                "max":        round(float(s.max()),    4) if not s.isna().all() else None,
                "null_count": int(s.isna().sum())
            }

        categorical_stats = {}
        for col in categorical_cols:
            s   = df[col].replace("", pd.NA)
            top = s.mode()[0] if not s.mode().empty else "---"
            categorical_stats[col] = {
                "unique":     int(s.nunique()),
                "top":        str(top),
                "freq":       int((s == top).sum()),
                "null_count": int(s.isna().sum())
            }

        return jsonify({
            "dataset":      n,
            "row_count":    len(df),
            "column_count": len(df.columns),
            "numeric":      numeric_stats,
            "categorical":  categorical_stats
        })

    except Exception as e:
        if conn: conn.close()
        return jsonify({"status": "error", "message": str(e)})


@app.route("/delete_dataset/<int:dataset_id>", methods=["DELETE"])
def delete_dataset(dataset_id):

    conn = None

    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            "DELETE FROM datasets WHERE id = %s",
            (dataset_id,)
        )

        conn.commit()

        cur.close()
        conn.close()

        return jsonify({
            "status": "success",
            "message": "Dataset deleted successfully"
        })

    except Exception as e:

        if conn:
            conn.rollback()
            conn.close()

        return jsonify({
            "status": "error",
            "message": str(e)
        })
if __name__ == "__main__":
    app.run(debug=True)
