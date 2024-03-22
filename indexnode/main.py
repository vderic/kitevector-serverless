import os

from flask import Flask

app = Flask(__name__)

idx = global_index_load()


def global_index_load():
    pass


@app.route('/')
def index_master_hamdler(request):
    pass


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('POST', 8080)))
