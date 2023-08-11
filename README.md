# Automated Discovery

The Redaction Assistant Web Application is a Flask-based web application that facilitates the redaction of personal data from PDFs and images using the **GPT-FOR-ALL** language model. It also provides database querying capabilities based on metadata and user-specified actions. The application is Dockerized for easy deployment.


## Features
- **Text Redaction:** Upload PDFs and images to automatically redact personal data using **GPT-FOR-ALL**.
Metadata Extraction: Extract metadata from MySQL, MSSQL, and PostgreSQL databases for querying.
Database Querying: Generate database queries based on metadata and user actions.
- **Web Interface:** User-friendly web interface for interacting with the application.
Dockerized: Ready for deployment using Docker.


## Prerequisites
Docker: Make sure you have Docker installed on your system.

## Installation and Usage

1. Clone the repository to your local machine:
```
git clone https://github.com/QueSchoenenberger/AutomatedDiscovery.git
cd AutomatedDiscovery
```

2. Build the Docker container:
```
docker build -t automated-discovery .
```

3. Run the Docker container:
```
docker run -p 5000:5000 redaction-app
Open a web browser and access the application at http://localhost:5000.
```

## Configuration
- API Key: Set your OpenAI API key in the openai_api.py file.

## Usage
1. Upload a PDF or image for redaction.
2. Choose an action: "Redact Personal Data" or "Show Tables."
3. Provide database connection details if choosing the database action.
4. View redacted content or query results.


## Notes
- This application uses the "**GPT-FOR-ALL**" model, so make sure you have an OpenAI API key.
- For database querying, provide appropriate database details in the web interface.
- The application is Dockerized for easy deployment. Adjust the Dockerfile as needed.

## Credits
This application was developed by Joseph Adam, Manuel Regli and Quentin Schoenenberger.

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit/).