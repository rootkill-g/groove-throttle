from flask import Flask, request
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.post('/crawl')
def crawl():
    urls = request.get_json()
    logging.info(f"Received URLs to crawl: {urls}")
    return {"status": "received", "urls": urls}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081)
