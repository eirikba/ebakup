#!/usr/bin/env python3

import http_server
import threading
import time

class WebUITask(object):

    def __init__(self, config, args):
        self._args = args

    def execute(self):
        args = self._args
        ui = args.services['uistate']
        http = http_server.HttpServer(
            ui.make_http_handler, port_low=4950, port_high=5000)
        self.http = http
        thread = threading.Thread(target=self.start_web_server, daemon=True)
        thread.start()
        ui.http_server = http
        ui.http_server_thread = thread
        logger = args.services['logger']
        while http.port is None:
            if not thread.is_alive():
                logger.log_error(
                    'subsystem failed', 'web ui', 'failed to start')
                return
            time.sleep(0.0001)
        logger.log_info(
            'subsystem started',
            'web ui',
            'listening on port ' + str(http.port))
        logger.print('Web ui started on port ' + str(http.port))

    def start_web_server(self):
        self.http.serve_forever()
