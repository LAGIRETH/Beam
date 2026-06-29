# 🚀 BEAM - Decentralized P2P Data Transport Protocol

**Beam is a weapon against corporate subscriptions.**
Zero servers. Zero subscriptions. Zero corporate scanning. Pure P2P.

---

##  FEATURES

### 📦 1. File Beam Transporter
Transfer files directly to a browser's hard drive at maximum speed.
* **No file size limits:** Transfer 100GB+ files without breaking a sweat.
* **Direct-to-Disk Streaming:** Uses the File System Access API to write massive files directly to the user's hard drive without crashing browser RAM.
* **Zero Installation:** The receiver doesn't need to download anything; they just open a GitHub Pages URL.
* **Advanced Flow Control:** Uses `SetBufferedAmountLowThreshold` and direct buffer polling to prevent buffer overflows during massive transfers.
* **Live Telemetry:** Calculates and displays real-time transfer speed (MB/s) and ETA.
* **Serverless Signaling:** Uses `ntfy.sh` to pass the WebRTC handshake, costing exactly $0.00.

### 🌐 2. Secure Proxy Tunnel Interface
Expose localhost services to the global internet via WebRTC.
* **Localhost Exposure:** Exposes a local port (like 3000 or 8080) to the internet without port forwarding.
* **Custom Binary Protocol:** Invented a 5-byte header protocol (Connection ID + Status Flag) to multiplex multiple TCP connections over a single WebRTC DataChannel.
* **Browser-as-Gateway:** The remote user's browser acts as the HTTP client, fetching data from the internet and piping it back through the tunnel.
* **Connection Tracking:** Tracks `BytesSent`, `BytesRecv`, and `LastActive` timestamps for every proxy connection.
* **Idle Connection Cleanup:** A background goroutine automatically kills TCP connections that haven't sent data in 30 seconds to prevent resource exhaustion.
* **Holding Page:** Automatically serves a "503 Proxy Offline" HTML page if someone tries to access the tunnel before the WebRTC connection is fully established.

### 🔄 3. Beam Sync P2P Folder Sync
Real-time folder synchronization between PCs (The Dropbox Killer).
* **Real-Time File Watching:** Uses `fsnotify` at the OS level to instantly detect file creations, modifications, and deletions.
* **Bidirectional Sync:** Changes flow both ways simultaneously.
* **Chunked Binary Transfer:** Breaks large files into 32KB chunks with custom headers (Path Length, Offset, IsLast) to ensure perfect reassembly.
* **Recursive Directory Watching:** Automatically watches subfolders and creates directories on the remote PC if they don't exist.
* **Delete Propagation:** If you delete a file on PC-A, it instantly deletes on PC-B.

### 🛡️ Architecture & Security
* **End-to-End Encrypted:** WebRTC uses DTLS/SRTP encryption. The data is encrypted before it leaves your network interface.
* **Zero Middleman:** Data travels directly Peer-to-Peer. Not even the free STUN servers can read your payload.
* **Robust ICE Gathering:** Uses `webrtc.GatheringCompletePromise` to ensure the SDP offer contains all valid network routes before sending.
* **Graceful Shutdown:** Intercepts `SIGINT` and `SIGTERM` (Ctrl+C) to cleanly close all TCP sockets, DataChannels, and PeerConnections without memory leaks.
* **Infinite Scalability:** Costs $0 to run whether 1 person uses it or 1 million people use it.

---

##  LICENSE

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

This means you are free to use, modify, and distribute this software, even for commercial purposes. However, if you modify the code and run it on a server that interacts with users over a network, you **must** make your modified source code available to those users. 

Keep it open. Keep it free.
