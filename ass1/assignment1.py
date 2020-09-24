from flask import Flask, request, send_file, Response
from markupsafe import escape
from sqlitedict import SqliteDict
from io import BytesIO
import hashlib
import qrcode

app = Flask(__name__)
data = SqliteDict('./data.sqlite', autocommit=True)
#app.debug = True

@app.route('/api/bookmarks', methods=['POST'])
def post_bookmarks():
    bookmark = request.get_json()
    id = hashlib.sha256(str.encode(bookmark['url'])).hexdigest()
    if id in data.keys():
        return {'reason' : "The given URL already existed in the system."}, 400
    else:
        data[id] = {'id':id, 'name':bookmark['name'], 'url':bookmark['url'],'description':bookmark['description'],'count':0}
        return {'id': id}, 201


@app.route('/api/bookmarks/<id>', methods=['GET'])
def get_bookmark(id):
    id = escape(id)
    if data.get(id):
        bookmark = data.get(id)
        new_count = bookmark['count'] + 1
        data[id] = {'id':id, 'name':bookmark['name'], 'url':bookmark['url'],'description':bookmark['description'],'count':new_count}
        return {'id':id, 'name':bookmark['name'], 'url':bookmark['url'],'description':bookmark['description']}
    else:
        return Response(status=404, response="404 Not Found")


@app.route('/api/bookmarks/<id>', methods=['DELETE'])
def delete_bookmark(id):
    id = escape(id)
    if data.get(id):
        del data[id]
        return Response(status=204)
    else:
        return Response(status=404, response="404 Not Found")


@app.route('/api/bookmarks/<id>/qrcode', methods=['GET'])
def get_qrcode(id):
    id = escape(id)
    if data.get(id):
        bookmark = data.get(id)
        img = qrcode.make(bookmark['url'])
        io = BytesIO()
        img.save(io, 'PNG')
        io.seek(0)
        return send_file(io, mimetype='image/png')
    else:
        return Response(status=404, response="404 Not Found")


@app.route('/api/bookmarks/<id>/stats', methods=['GET'])
def get_stats(id):
    id = escape(id)
    if (request.headers.get('Etag')):
        etag = request.headers.get('Etag')
    else:
        etag = str(0)
    if data.get(id):
        bookmark = data.get(id)
        count = str(bookmark['count'])
        if etag == count:
            return Response(headers={'ETag':count}, status=304)
        else:
            return Response(headers={'ETag':count}, status=200, response=count)
    else:
        return Response(status=404, response="404 Not Found")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)