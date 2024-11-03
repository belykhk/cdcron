import atexit
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

logger = logging.getLogger(__name__)


class StubRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Ok")

    def log_message(self, format, *args):
        return  # Suppress logging to console


class HealthCheckServer:
    def __init__(self):
        self.port = int(os.getenv("HEALTHCHECK_PORT", 8080))
        self.server = HTTPServer(("0.0.0.0", self.port), StubRequestHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        atexit.register(self.stop)
        self.start()

    def start(self):
        """Start the server in a separate thread."""
        self.server_thread.start()
        logger.info(f"healthcheck server started on http://0.0.0.0:{self.port}")

    def stop(self):
        """Gracefully stop the server."""
        if self.server:
            logger.info("stopping healthcheck server...")
            self.server.shutdown()
            self.server.server_close()
            self.server_thread.join()
            logger.info("healthcheck server stopped")
