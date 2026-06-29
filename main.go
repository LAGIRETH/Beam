package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"
)

type FileMetadata struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type ConnectionState struct {
	Conn       net.Conn
	LastActive time.Time
	BytesSent  int64
	BytesRecv  int64
}

var (
	connections    = make(map[uint32]*ConnectionState)
	connMutex      sync.RWMutex
	connCounter    uint32
	proxyChan      *webrtc.DataChannel
	proxyReady     = false
	peerConnection *webrtc.PeerConnection

	maxBufferSize     = 512 * 1024 // Reduced to 512KB for better flow control
	connectionTimeout = 30 * time.Second
	httpTimeout       = 10 * time.Second
)

func init() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n[SYSTEM] Shutdown signal received. Cleaning up...")
		cleanup()
		os.Exit(0)
	}()
}

func cleanup() {
	fmt.Println("[SYSTEM] Closing all active connections...")

	connMutex.Lock()
	defer connMutex.Unlock()

	for id, state := range connections {
		if state.Conn != nil {
			state.Conn.Close()
			fmt.Printf("[SYSTEM] Closed connection #%d\n", id)
		}
	}

	if peerConnection != nil {
		peerConnection.Close()
		fmt.Println("[SYSTEM] WebRTC peer connection closed")
	}
}

func generateRoomID() string {
	b := make([]byte, 4)
	_, err := rand.Read(b)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate random room ID: %v", err))
	}
	return hex.EncodeToString(b)
}

func validatePort(port string) bool {
	if port == "" {
		return false
	}
	var portNum int
	_, err := fmt.Sscanf(port, "%d", &portNum)
	if err != nil {
		return false
	}
	return portNum > 0 && portNum < 65536
}

func validateFilePath(path string) error {
	if path == "" {
		return fmt.Errorf("file path cannot be empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", path)
		}
		return fmt.Errorf("cannot access file: %v", err)
	}
	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file: %s", path)
	}
	if info.Size() == 0 {
		return fmt.Errorf("file is empty: %s", path)
	}
	return nil
}

func calculateETA(sent, total int64, elapsed float64) string {
	if sent == 0 || elapsed == 0 {
		return "calculating..."
	}
	speed := float64(sent) / elapsed
	remaining := float64(total-sent) / speed

	if remaining < 60 {
		return fmt.Sprintf("%.0fs", remaining)
	} else if remaining < 3600 {
		minutes := int(remaining / 60)
		seconds := int(math.Mod(remaining, 60))
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	} else {
		hours := int(remaining / 3600)
		minutes := int(math.Mod(remaining, 3600) / 60)
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
}

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║         █ BEAM SECURE MATRIX CORE V1.0 █                  ║")
	fmt.Println("║      Decentralized P2P Data Transport Protocol            ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("[1] 📦 File Beam Transporter")
	fmt.Println("    → Transfer files directly to browser storage")
	fmt.Println()
	fmt.Println("[2] 🌐 Secure Proxy Tunnel Interface")
	fmt.Println("    → Expose localhost services via WebRTC")
	fmt.Println()
	fmt.Println("[3] 🔄 Beam Sync P2P Folder Sync")
	fmt.Println("    → Sync folders between PCs (Dropbox killer)")
	fmt.Println()
	fmt.Print("Select target operational mode [1/2/3]:\n> ")

	var choice string
	fmt.Scanln(&choice)

	switch choice {
	case "1":
		runFileBeam()
	case "2":
		runProxyTunnel()
	case "3":
		runBeamSync()
	default:
		fmt.Printf("[ERROR] Invalid selection: %s. Please choose 1, 2, or 3.\n", choice)
		os.Exit(1)
	}
}

func runFileBeam() {
	fmt.Println("\n┌─ FILE BEAM TRANSPORTER ─────────────────────────────────────┐")
	fmt.Print("│ Enter the absolute or relative path of the file to beam:\n│ > ")
	var filePath string
	fmt.Scanln(&filePath)
	fmt.Println("└─────────────────────────────────────────────────────────────┘")

	if err := validateFilePath(filePath); err != nil {
		fmt.Printf("[ERROR] %v\n", err)
		return
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("[ERROR] Cannot stat file: %v\n", err)
		return
	}

	fmt.Printf("[INFO] Target file: %s\n", filepath.Base(filePath))
	fmt.Printf("[INFO] File size: %.2f MB\n", float64(fileInfo.Size())/1024/1024)

	roomID := generateRoomID()
	fmt.Printf("[INFO] Generated room ID: %s\n", roomID)

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	var errPc error
	peerConnection, errPc = webrtc.NewPeerConnection(config)
	if errPc != nil {
		fmt.Printf("[ERROR] Failed to create peer connection: %v\n", errPc)
		return
	}
	defer peerConnection.Close()

	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		fmt.Printf("[WEBRTC] Connection state changed: %s\n", state.String())
		if state == webrtc.PeerConnectionStateFailed {
			fmt.Println("[ERROR] WebRTC connection failed!")
			cleanup()
			os.Exit(1)
		}
	})

	dataChannel, err := peerConnection.CreateDataChannel("beam-data", nil)
	if err != nil {
		fmt.Printf("[ERROR] Failed to create data channel: %v\n", err)
		return
	}

	dataChannel.OnOpen(func() {
		fmt.Println("\n[SUCCESS] ✓ Direct P2P tunnel established with browser client!")
		fmt.Println("[INFO] Waiting for client to allocate disk space...")
	})

	dataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		if string(msg.Data) == "START" {
			fmt.Println("\n[SYSTEM] ✓ Disk allocated by remote peer")
			fmt.Println("[SYSTEM] Initiating payload stream...")

			meta := FileMetadata{Name: filepath.Base(filePath), Size: fileInfo.Size()}
			metaBytes, err := json.Marshal(meta)
			if err != nil {
				fmt.Printf("[ERROR] Failed to marshal metadata: %v\n", err)
				return
			}

			if err := dataChannel.SendText(string(metaBytes)); err != nil {
				fmt.Printf("[ERROR] Failed to send metadata: %v\n", err)
				return
			}

			file, err := os.Open(filePath)
			if err != nil {
				fmt.Printf("[ERROR] Failed to open file: %v\n", err)
				return
			}
			defer file.Close()

			// FIXED: Use smaller chunks and direct polling instead of channel-based flow control
			buffer := make([]byte, 32*1024) // Reduced to 32KB chunks for smoother flow
			startTime := time.Now()
			totalSent := int64(0)
			lastProgressUpdate := time.Now()

			fmt.Println("\n[PROGRESS] Starting transfer...")

			for {
				n, err := file.Read(buffer)
				if n > 0 {
					// FIXED: Direct polling of BufferedAmount instead of channel-based flow control
					// Wait if buffer is getting full (above 512KB)
					for dataChannel.BufferedAmount() > uint64(maxBufferSize) {
						time.Sleep(10 * time.Millisecond)
					}

					if errSend := dataChannel.Send(buffer[:n]); errSend != nil {
						fmt.Printf("\n[ERROR] Failed to send data: %v\n", errSend)
						return
					}

					totalSent += int64(n)

					if time.Since(lastProgressUpdate) > 500*time.Millisecond {
						percent := float64(totalSent) / float64(fileInfo.Size()) * 100
						elapsed := time.Since(startTime).Seconds()
						speed := float64(totalSent) / elapsed / 1024 / 1024

						fmt.Printf("\r[PROGRESS] %.1f%% | %.2f MB / %.2f MB | %.2f MB/s | ETA: %s    ",
							percent,
							float64(totalSent)/1024/1024,
							float64(fileInfo.Size())/1024/1024,
							speed,
							calculateETA(totalSent, fileInfo.Size(), elapsed))

						lastProgressUpdate = time.Now()
					}
				}

				if err == io.EOF {
					break
				}
				if err != nil {
					fmt.Printf("\n[ERROR] File read error: %v\n", err)
					return
				}
			}

			duration := time.Since(startTime)
			avgSpeed := float64(totalSent) / duration.Seconds() / 1024 / 1024

			fmt.Printf("\n\n╔════════════════════════════════════════════════════════════╗\n")
			fmt.Printf("║ [COMPLETE] ✓ Beaming sequence finalized cleanly!            ║\n")
			fmt.Printf("║ Duration: %v                                            \n", duration.Round(time.Millisecond))
			fmt.Printf("║ Average Speed: %.2f MB/s                                    \n", avgSpeed)
			fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
		}
	})

	initializeHandshake(peerConnection, roomID, "file")

	// CRITICAL FIX: Keep the program running for file transfer!
	fmt.Println("\n[SYSTEM] File beam tunnel is now active and waiting for receiver...")
	fmt.Println("[SYSTEM] Press Ctrl+C to cancel")

	// This blocks forever, keeping the program alive so the WebRTC connection stays open
	select {}
}

func runProxyTunnel() {
	fmt.Println("\n┌─ SECURE PROXY TUNNEL ─────────────────────────────────────────┐")
	fmt.Print("│ Enter local port to bind (e.g., 3000, 8080):\n│ > ")
	var localPort string
	fmt.Scanln(&localPort)
	fmt.Println("└─────────────────────────────────────────────────────────────┘")

	if !validatePort(localPort) {
		fmt.Printf("[ERROR] Invalid port number: %s\n", localPort)
		return
	}

	roomID := generateRoomID()
	fmt.Printf("[INFO] Generated room ID: %s\n", roomID)

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	var errPc error
	peerConnection, errPc = webrtc.NewPeerConnection(config)
	if errPc != nil {
		fmt.Printf("[ERROR] Failed to create peer connection: %v\n", errPc)
		return
	}
	defer peerConnection.Close()

	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		fmt.Printf("[WEBRTC] Connection state changed: %s\n", state.String())
		if state == webrtc.PeerConnectionStateFailed {
			fmt.Println("[ERROR] WebRTC connection failed!")
			cleanup()
			os.Exit(1)
		}
	})

	var errDc error
	proxyChan, errDc = peerConnection.CreateDataChannel("beam-proxy", nil)
	if errDc != nil {
		fmt.Printf("[ERROR] Failed to create proxy data channel: %v\n", errDc)
		return
	}

	proxyChan.OnOpen(func() {
		fmt.Println("\n[SUCCESS] ✓ Multiplexed WebRTC Proxy transport active!")
		proxyReady = true
	})

	proxyChan.OnMessage(func(msg webrtc.DataChannelMessage) {
		if len(msg.Data) < 5 {
			return
		}

		connID := binary.BigEndian.Uint32(msg.Data[0:4])
		statusFlag := msg.Data[4]

		connMutex.RLock()
		state, exists := connections[connID]
		connMutex.RUnlock()

		if exists {
			state.LastActive = time.Now()
			if statusFlag == 1 {
				go func(socket net.Conn, id uint32) {
					time.Sleep(50 * time.Millisecond)
					socket.Close()
					fmt.Printf("[PROXY] Connection #%d closed by remote\n", id)
				}(state.Conn, connID)
			} else {
				n, err := state.Conn.Write(msg.Data[5:])
				if err != nil {
					fmt.Printf("[ERROR] Failed to write to connection #%d: %v\n", connID, err)
				} else {
					state.BytesRecv += int64(n)
				}
			}
		}
	})

	listener, err := net.Listen("tcp", "127.0.0.1:"+localPort)
	if err != nil {
		fmt.Printf("[ERROR] Failed to bind local port %s: %v\n", localPort, err)
		return
	}
	defer listener.Close()

	fmt.Printf("[SYSTEM] ✓ Proxy TCP listener active on http://127.0.0.1:%s\n", localPort)
	fmt.Println("[INFO] Waiting for incoming connections...")

	go connectionCleanupRoutine()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				continue
			}

			if !proxyReady {
				holdingResponse := "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n<html><body style='background:#090d16;color:#f9fafb;font-family:sans-serif;padding:40px;text-align:center;'><h1>⚠️ Proxy Pipeline Offline</h1><p style='color:#9ca3af;'>Please access your dashboard token link first to activate WebRTC routing.</p></body></html>"
				conn.Write([]byte(holdingResponse))
				conn.Close()
				continue
			}

			connMutex.Lock()
			connCounter++
			id := connCounter
			connections[id] = &ConnectionState{Conn: conn, LastActive: time.Now()}
			connMutex.Unlock()

			fmt.Printf("[PROXY] ✓ New connection #%d accepted\n", id)
			go handleProxyConnection(conn, id)
		}
	}()

	initializeHandshake(peerConnection, roomID, "proxy")
	
	fmt.Println("\n[SYSTEM] Proxy tunnel is now active and waiting for connections...")
	fmt.Println("[SYSTEM] Press Ctrl+C to shutdown")
	
	select {}
}

func handleProxyConnection(conn net.Conn, connID uint32) {
	defer func() {
		conn.Close()
		connMutex.Lock()
		delete(connections, connID)
		connMutex.Unlock()

		closeFrame := make([]byte, 5)
		binary.BigEndian.PutUint32(closeFrame[0:4], connID)
		closeFrame[4] = 1

		if proxyReady && proxyChan != nil {
			proxyChan.Send(closeFrame)
		}
		fmt.Printf("[PROXY] Connection #%d terminated\n", connID)
	}()

	buf := make([]byte, 32768)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			dataFrame := make([]byte, 5+n)
			binary.BigEndian.PutUint32(dataFrame[0:4], connID)
			dataFrame[4] = 0
			copy(dataFrame[5:], buf[:n])

			if !proxyReady || proxyChan == nil {
				break
			}

			if errSend := proxyChan.Send(dataFrame); errSend != nil {
				break
			}

			connMutex.RLock()
			if state, exists := connections[connID]; exists {
				state.BytesSent += int64(n)
			}
			connMutex.RUnlock()
		}

		if err != nil {
			break
		}
	}
}

func connectionCleanupRoutine() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		connMutex.Lock()
		now := time.Now()
		for id, state := range connections {
			if now.Sub(state.LastActive) > connectionTimeout {
				fmt.Printf("[CLEANUP] Closing idle connection #%d\n", id)
				state.Conn.Close()
				delete(connections, id)
			}
		}
		connMutex.Unlock()
	}
}

func initializeHandshake(pc *webrtc.PeerConnection, roomID string, mode string) {
	fmt.Println("\n[HANDSHAKE] Initiating WebRTC Signaling Sequence...")
	fmt.Println("[HANDSHAKE] Step 1: Creating SDP Offer...")

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		fmt.Printf("[ERROR] Critical failure creating offer: %v\n", err)
		return
	}

	fmt.Println("[HANDSHAKE] Step 2: Setting Local Description...")
	err = pc.SetLocalDescription(offer)
	if err != nil {
		fmt.Printf("[ERROR] Critical failure setting local description: %v\n", err)
		return
	}

	fmt.Println("[HANDSHAKE] Step 3: Waiting for ICE Candidate Gathering to complete...")
	fmt.Println("[HANDSHAKE] (This ensures the browser knows exactly how to reach your machine)")
	
	<-webrtc.GatheringCompletePromise(pc)
	
	fmt.Println("[HANDSHAKE] ✓ ICE Gathering Complete. All network routes identified.")

	localDesc := pc.LocalDescription()
	if localDesc == nil {
		fmt.Println("[ERROR] Local description is nil after ICE gathering.")
		return
	}

	candidateCount := strings.Count(localDesc.SDP, "a=candidate:")
	fmt.Printf("[HANDSHAKE] Step 4: Encoding SDP with %d ICE candidates...\n", candidateCount)

	b64Offer := base64.StdEncoding.EncodeToString([]byte(localDesc.SDP))
	fmt.Printf("[HANDSHAKE] ✓ Offer encoded successfully (%d bytes)\n", len(b64Offer))

	ntfyOfferURL := fmt.Sprintf("https://ntfy.sh/beam_%s_offer", roomID)
	fmt.Printf("[HANDSHAKE] Step 5: Transmitting Offer to Signaling Node...\n")
	
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", ntfyOfferURL, strings.NewReader(b64Offer))
	if err != nil {
		fmt.Printf("[ERROR] Failed to construct HTTP request: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[ERROR] Network failure posting to ntfy.sh: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("[ERROR] Signaling node rejected offer with status %d\n", resp.StatusCode)
		return
	}

	fmt.Println("[HANDSHAKE] ✓ Offer successfully parked on signaling node.")

	githubPagesURL := fmt.Sprintf("https://lagireth.github.io/beam-receiver/?id=%s&mode=%s", roomID, mode)

	fmt.Println("\n╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║              █ ACCESS TOKEN GENERATED █                   ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")

	if mode == "file" {
		fmt.Println("\n📦 MODE: FILE BEAM TRANSPORTER")
		fmt.Println("Send this URL to the RECEIVER:")
	} else {
		fmt.Println("\n🌐 MODE: SECURE PROXY TUNNEL")
		fmt.Println("Send this URL to the CLIENT:")
	}

	fmt.Println("\n" + githubPagesURL)
	fmt.Println("\n─────────────────────────────────────────────────────────────")
	fmt.Println("[HANDSHAKE] Step 6: Listening for remote peer's Answer...")
	fmt.Println("(Press Ctrl+C to abort)")
	fmt.Println()

	answerURL := fmt.Sprintf("https://ntfy.sh/beam_%s_answer/raw?poll=1", roomID)
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer pollCancel()

	for {
		select {
		case <-pollCtx.Done():
			fmt.Println("\n[ERROR] Timeout waiting for answer (5 minutes).")
			return
		default:
		}

		req, err := http.NewRequestWithContext(pollCtx, "GET", answerURL, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			b64Answer, err := io.ReadAll(resp.Body)
			resp.Body.Close()

			if err == nil && len(b64Answer) > 0 {
				fmt.Println("[HANDSHAKE] ✓ Answer received from remote peer!")
				fmt.Println("[HANDSHAKE] Step 7: Decoding and applying remote Answer...")
				
				answerSDP, err := base64.StdEncoding.DecodeString(string(b64Answer))
				if err == nil {
					answer := webrtc.SessionDescription{
						Type: webrtc.SDPTypeAnswer,
						SDP:  string(answerSDP),
					}

					err = pc.SetRemoteDescription(answer)
					if err != nil {
						fmt.Printf("[ERROR] Failed to apply remote description: %v\n", err)
						return
					}

					fmt.Println("\n[SUCCESS] ✓ WebRTC Handshake Protocol Complete!")
					fmt.Println("[INFO] The P2P tunnel is now negotiating the final data link...")
					return
				} else {
					fmt.Printf("[ERROR] Failed to decode base64 answer: %v\n", err)
				}
			}
		} else if resp != nil {
			resp.Body.Close()
		}

		time.Sleep(1 * time.Second)
	}
}
