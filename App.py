"""Compatible Azure/Gunicorn entry point: gunicorn App:app."""
from edi_stock import create_app

app = create_app()

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)
