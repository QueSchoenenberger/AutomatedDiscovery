import logging
import os

from process_image import get_text_from_image, mask_image
from extract_metadata import get_metadata_mysql, get_metadata_mssql, get_metadata_postgresql
from flask import Flask, request, render_template, send_file
import pdfplumber
import io
import openai_api
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

app = Flask(__name__)
UPLOAD_FOLDER = 'static/images/raw'
OUTPUT_FOLDER = 'static/images/processed'


@app.route('/fileUpload', methods=['POST'])
def file_upload():
    filename, download_filename, modified_pdf_path = '', '', ''
    if 'file' not in request.files:
        return "No PDF file provided."

    uploaded_file = request.files['file']

    if uploaded_file.filename == '':
        return "No selected file."

    if uploaded_file is None:
        return render_template('index.html')

    if uploaded_file.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif')):
        filename = os.path.join(UPLOAD_FOLDER, uploaded_file.filename)
        filename = os.path.normpath(filename)
        filename = filename.replace("\\", "/")
        print(filename)
        uploaded_file.save(filename)

        download_filename = os.path.join(OUTPUT_FOLDER,
                                         f'{os.path.splitext(os.path.basename(uploaded_file.filename))[0]}_out.jpg')

        image_text, image_metadata, img = get_text_from_image(filename)
        modified_text = openai_api.remove_personal_data(image_text)

        mask_image(img, download_filename, modified_text, image_metadata)

        return render_template('index.html', download_path=download_filename, filename=download_filename)

    file_content = extract_text_from_pdf(uploaded_file)
    modified_file_content = openai_api.remove_personal_data(file_content)

    pdf_io = io.BytesIO()
    doc = SimpleDocTemplate(pdf_io, pagesize=letter)

    styles = getSampleStyleSheet()
    modified_text = modified_file_content.split('\n')
    story = []

    for line in modified_text:
        story.append(Paragraph(line, styles['Normal']))
        story.append(Spacer(1, 5))

    doc.build(story)

    pdf_io.seek(0)
    modified_pdf_path = 'modified_' + uploaded_file.filename
    with open(modified_pdf_path, 'wb') as f:
        f.write(pdf_io.read())

    return send_file(modified_pdf_path, mimetype='application/pdf')


def extract_text_from_pdf(pdf_file):
    with pdfplumber.open(pdf_file) as pdf:
        text = ''
        for page in pdf.pages:
            text += page.extract_text()
    return text


@app.route('/searchDatabase', methods=['POST'])
def search_database():
    metadata, relations, results = [], [], []
    host = request.form.get('host')
    user = request.form.get('user')
    password = request.form.get('password')
    database = request.form.get('database')
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    action = request.form.get('action')
    db_type = request.form.get('db_type')

    if db_type == "mysql":
        metadata, relations = get_metadata_mysql(host, user, password, database)

    elif db_type == "mssql":
        metadata, relations = get_metadata_mssql(host, user, password, database)

    elif db_type == "postgresql":
        metadata, relations = get_metadata_postgresql(host, user, password, database)

    api_answer = openai_api.get_queries(metadata, relations, first_name, last_name, action)

    if action == "showPersonalData":

        if db_type == "mysql":
            results = openai_api.run_query_mysql(host, user, password, database, api_answer)

        elif db_type == "mssql":
            results = openai_api.run_query_mssql(host, user, password, database, api_answer)

        elif db_type == "postgresql":
            results = openai_api.run_query_postgresql(host, user, password, database, api_answer)

        query_results = list(zip(api_answer, results))

        return render_template('query_results.html', query_results=query_results)

    elif action == "showTables":
        return render_template('queries.html', sql_queries=api_answer)

    return render_template('index.html')


@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    app.run(host="0.0.0.0")
