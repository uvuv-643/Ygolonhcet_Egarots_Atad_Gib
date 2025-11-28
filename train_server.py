import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from train_parser_local import get_trains_with_seats


class TrainParserHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            parsed_path = urlparse(self.path)
            query_params = parse_qs(parsed_path.query)
            
            url = query_params.get('url', [None])[0]
            if url:
                from urllib.parse import unquote
                url = unquote(url)
            if not url:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {'error': 'Missing url parameter'}
                self.wfile.write(json.dumps(response).encode())
                return
            
            num_trains = int(query_params.get('num_trains', ['3'])[0])
            
            trains = get_trains_with_seats(url, headless=True, num_trains=num_trains)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(trains, ensure_ascii=False).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'error': str(e)}
            self.wfile.write(json.dumps(response).encode())
    
    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode())
            
            url = data.get('url')
            if not url:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {'error': 'Missing url in request body'}
                self.wfile.write(json.dumps(response).encode())
                return
            
            num_trains = int(data.get('num_trains', 3))
            
            trains = get_trains_with_seats(url, headless=False, num_trains=num_trains)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(trains, ensure_ascii=False).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'error': str(e)}
            self.wfile.write(json.dumps(response).encode())
    
    def log_message(self, format, *args):
        print(f"[{self.address_string()}] {format % args}")


def run_server(port=8888):
    server_address = ('', port)
    httpd = HTTPServer(server_address, TrainParserHandler)
    print(f"Train parser server running on port {port}")
    print(f"Access at http://localhost:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8888
    run_server(port)

