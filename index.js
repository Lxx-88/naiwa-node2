const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

const UUID = (process.env.UUID || '').replace(/-/g, '').toLowerCase();
const PORT = process.env.PORT || 8080;

if (!UUID || UUID.length !== 32) {
  console.error('UUID env var missing or invalid');
  process.exit(1);
}
const uuidBuf = Buffer.from(UUID, 'hex');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('naiwa node2 ok');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let socket = null;
  let early = [];
  let ready = false;

  ws.on('message', (data) => {
    if (ready) {
      if (socket && !socket.destroyed) socket.write(data);
      return;
    }
    // First message: VLESS header (+ optional early payload)
    try {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (buf.length < 24) return ws.close();
      if (buf[0] !== 0) return ws.close();
      if (!buf.subarray(1, 17).equals(uuidBuf)) return ws.close();
      let idx = 17;
      const optLen = buf[idx++];
      idx += optLen;
      const cmd = buf[idx++];
      if (cmd !== 1) return ws.close(); // TCP only
      const port = buf.readUInt16BE(idx); idx += 2;
      const atyp = buf[idx++];
      let host;
      if (atyp === 1) { host = [...buf.subarray(idx, idx + 4)].join('.'); idx += 4; }
      else if (atyp === 2) { const l = buf[idx++]; host = buf.subarray(idx, idx + l).toString(); idx += l; }
      else if (atyp === 3) { const parts = []; for (let i = 0; i < 8; i++) { parts.push(buf.readUInt16BE(idx).toString(16)); idx += 2; } host = parts.join(':'); }
      else return ws.close();
      const rest = buf.subarray(idx);
      socket = net.connect(port, host, () => {
        ready = true;
        if (rest.length) socket.write(rest);
        for (const c of early) socket.write(c);
        early = [];
      });
      let first = true;
      socket.on('data', (chunk) => {
        if (ws.readyState !== ws.OPEN) { socket.destroy(); return; }
        if (first) { first = false; ws.send(Buffer.concat([Buffer.from([0, 0]), chunk])); }
        else ws.send(chunk);
      });
      socket.on('error', () => { try { ws.close(); } catch {} });
      socket.on('close', () => { try { ws.close(); } catch {} });
    } catch (e) {
      try { ws.close(); } catch {}
    }
  });

  ws.on('close', () => { if (socket) socket.destroy(); });
  ws.on('error', () => { if (socket) socket.destroy(); });
});

server.listen(PORT, () => console.log('listening on', PORT));
