from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import random
import time


class WeatherHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/weather":
            data = {
                "city": random.choice(["Berlin", "Munich", "Hamburg"]),
                "temperature": round(random.uniform(-5, 35), 1),
                "humidity": random.randint(30, 95),
                "unit": "celsius",
                "timestamp": time.time(),
            }
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Request-Id", f"req-{random.randint(1000,9999)}")
            self.end_headers()
            self.write = self.wfile.write
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"[api] {args[0]}")


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8080), WeatherHandler)
    print("[api] Listening on :8080")
    server.serve_forever()
