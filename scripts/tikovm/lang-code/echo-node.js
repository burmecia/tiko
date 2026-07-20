'use strict';
// Minimal Node.js HTTP echo handler for the tikovm lang-rootfs.
//
// This file is NOT baked into the rootfs — it is deployed to the remote_slow
// volume and loaded at cold start by /usr/local/bin/lang-bootstrap. This
// compute-vs-storage separation is the Lambda model: the VM image is the
// ephemeral runtime layer; this file is the durable function-code layer.
//
//   GET /         -> 200 "hello world from node v<version>\n"
//   GET /health   -> 200 {"ok":true}
//
// No external deps; uses the built-in http module so the Node.js runtime
// in the image suffices. Mirrors echo-python.py 1:1 so the two runtimes
// are interchangeable via the .runtime marker on the volume.

const http = require('http');

const argv = process.argv;
let port = 8080;
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--port' && i + 1 < argv.length) port = parseInt(argv[i + 1], 10);
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('hello world from node ' + process.version + '\n');
});

server.listen(port, '0.0.0.0', () => {
  console.error('tikovm lang-echo (node ' + process.version + ') listening on :' + port);
});
