from flask import Flask
import sqlite3

app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Brain Butler API! Because who doesn't need a butler?"

def get_db_connection():
    conn = sqlite3.connect('src/db/mind-emiel.db')
    conn.row_factory = sqlite3.Row  # This allows us to return rows as dictionaries
    return conn

@app.route('/records', methods=['GET'])
def get_records():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM record")
    records = cursor.fetchall()
    conn.close()
    
    # Loop over records, extract textual description if possible.
    # Convert rows to dictionaries
    record_list = [dict(record) for record in records]
    return jsonify(record_list)

@app.route('/record/<string:record_id>', methods=['GET'])
def get_record(record_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM record WHERE id = ?", (record_id,))
    record = cursor.fetchone()
    conn.close()
    
    # Check which URI(s) to get
    # Convert rows to dictionaries
    #record_list = [dict(record) for record in records]
    return jsonify(record)

@app.route('/records', methods=['POST'])
def add_record():
    new_record = request.get_json()
    text_uri = new_record.get('text_uri')
    reference = new_record.get('reference')
    print(text_uri)
    print(reference)
    if not text_uri or not reference:
        return jsonify({"error": "Missing text_uri or reference"}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO record (text_uri, reference) VALUES (?, ?)", (text_uri, reference))
    conn.commit()
    new_record_id = cursor.lastrowid
    conn.close()
    return jsonify({"message":"Added record succesfully"})

@app.route('/records-tags', methods=['GET'])
def get_record_tag_links():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM record_tag")
    records_tags_links = cursor.fetchall()
    conn.close()
    
    # Loop over records, extract textual description if possible.
    # Convert rows to dictionaries
    links_list = [dict(link) for link in records_tags_links]
    return jsonify(links_list)

if __name__ == '__main__':
    initialize_database()
    app.run(debug=True)
