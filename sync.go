package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pion/webrtc/v4"
)

// SyncMessage represents a sync command
type SyncMessage struct {
	Type     string `json:"type"` // CREATE, UPDATE, DELETE, ACK
	Path     string `json:"path"`
	Size     int64  `json:"size,omitempty"`
	Modified int64  `json:"modified,omitempty"`
}

var (
	syncPeerConnection *webrtc.PeerConnection
	syncDataChannel    *webrtc.DataChannel
	syncWatcher        *fsnotify.Watcher
	syncLocalFolder    string
	syncMutex          sync.Mutex
	sendingFiles       = make(map[string]bool)
)

func runBeamSync() {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║         █ BEAM SYNC P2P FOLDER SYNC █                     ║")
	fmt.Println("║      Decentralized Cloud-Syncing Killer                   ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("[1] 🚀 Initiate Sync (Create new sync room)")
	fmt.Println("[2] 🔗 Join Sync (Connect to existing room)")
	fmt.Println()
	fmt.Print("Select mode [1/2]: ")

	var choice string
	fmt.Scanln(&choice)

	if choice == "1" {
		initiateSync()
	} else {
		joinSync()
	}
}

func initiateSync() {
	fmt.Print("\nEnter local folder path to sync: ")
	fmt.Scanln(&syncLocalFolder)

	if _, err := os.Stat(syncLocalFolder); os.IsNotExist(err) {
		fmt.Printf("[ERROR] Folder does not exist: %s\n", syncLocalFolder)
		return
	}

	roomID := generateRoomID()
	fmt.Printf("[INFO] Generated room ID: %s\n", roomID)

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}

	var err error
	syncPeerConnection, err = webrtc.NewPeerConnection(config)
	if err != nil {
		panic(err)
	}
	defer syncPeerConnection.Close()

	syncDataChannel, err = syncPeerConnection.CreateDataChannel("beam-sync", nil)
	if err != nil {
		panic(err)
	}

	setupSyncDataChannelHandlers()

	// Create offer
	offer, err := syncPeerConnection.CreateOffer(nil)
	if err != nil {
		panic(err)
	}

	err = syncPeerConnection.SetLocalDescription(offer)
	if err != nil {
		panic(err)
	}

	// Wait for ICE gathering
	<-webrtc.GatheringCompletePromise(syncPeerConnection)

	b64Offer := base64.StdEncoding.EncodeToString([]byte(syncPeerConnection.LocalDescription().SDP))

	// Post offer to ntfy.sh
	ntfyURL := fmt.Sprintf("https://ntfy.sh/beam_sync_%s_offer", roomID)
	resp, err := http.Post(ntfyURL, "text/plain", strings.NewReader(b64Offer))
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	fmt.Println("\n╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║              █ SYNC ROOM CREATED █                        ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Printf("\nRoom ID: %s\n", roomID)
	fmt.Println("\nShare this Room ID with the other PC.")
	fmt.Println("Waiting for peer to join...")

	// Poll for answer
	answerURL := fmt.Sprintf("https://ntfy.sh/beam_sync_%s_answer/raw?poll=1", roomID)
	client := &http.Client{Timeout: 30 * time.Second}

	for {
		resp, err := client.Get(answerURL)
		if err == nil && resp.StatusCode == 200 {
			b64Answer, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if len(b64Answer) > 0 {
				answerSDP, _ := base64.StdEncoding.DecodeString(string(b64Answer))
				answer := webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  string(answerSDP),
				}
				syncPeerConnection.SetRemoteDescription(answer)
				fmt.Println("\n[SUCCESS] ✓ Peer connected! Sync tunnel active.")
				break
			}
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}

	// Start folder watcher
	startFolderWatcher()

	fmt.Println("\n[SYSTEM] Watching for file changes...")
	fmt.Println("[SYSTEM] Press Ctrl+C to stop sync")

	// Keep alive
	select {}
}

func joinSync() {
	fmt.Print("\nEnter local folder path to sync: ")
	fmt.Scanln(&syncLocalFolder)

	if _, err := os.Stat(syncLocalFolder); os.IsNotExist(err) {
		fmt.Printf("[ERROR] Folder does not exist: %s\n", syncLocalFolder)
		return
	}

	fmt.Print("Enter room ID to join: ")
	var roomID string
	fmt.Scanln(&roomID)

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}

	var err error
	syncPeerConnection, err = webrtc.NewPeerConnection(config)
	if err != nil {
		panic(err)
	}
	defer syncPeerConnection.Close()

	syncPeerConnection.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() == "beam-sync" {
			syncDataChannel = dc
			setupSyncDataChannelHandlers()
		}
	})

	// Poll for offer
	offerURL := fmt.Sprintf("https://ntfy.sh/beam_sync_%s_offer/raw?poll=1", roomID)
	client := &http.Client{Timeout: 30 * time.Second}

	fmt.Println("[INFO] Waiting for sync room offer...")

	var offerSDP string
	for {
		resp, err := client.Get(offerURL)
		if err == nil && resp.StatusCode == 200 {
			b64Offer, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if len(b64Offer) > 0 {
				decoded, _ := base64.StdEncoding.DecodeString(string(b64Offer))
				offerSDP = string(decoded)
				break
			}
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}

	// Set remote description (offer)
	err = syncPeerConnection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	})
	if err != nil {
		panic(err)
	}

	// Create answer
	answer, err := syncPeerConnection.CreateAnswer(nil)
	if err != nil {
		panic(err)
	}

	err = syncPeerConnection.SetLocalDescription(answer)
	if err != nil {
		panic(err)
	}

	// Wait for ICE gathering
	<-webrtc.GatheringCompletePromise(syncPeerConnection)

	b64Answer := base64.StdEncoding.EncodeToString([]byte(syncPeerConnection.LocalDescription().SDP))

	// Post answer to ntfy.sh
	ntfyURL := fmt.Sprintf("https://ntfy.sh/beam_sync_%s_answer", roomID)
	resp, err := http.Post(ntfyURL, "text/plain", strings.NewReader(b64Answer))
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	fmt.Println("\n[SUCCESS] ✓ Joined sync room! Tunnel active.")

	// Wait for data channel to open
	time.Sleep(2 * time.Second)

	// Start folder watcher
	startFolderWatcher()

	fmt.Println("\n[SYSTEM] Watching for file changes...")
	fmt.Println("[SYSTEM] Press Ctrl+C to stop sync")

	// Keep alive
	select {}
}

func setupSyncDataChannelHandlers() {
	syncDataChannel.OnOpen(func() {
		fmt.Println("[SUCCESS] ✓ Sync data channel opened!")
	})

	syncDataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			// JSON command
			var syncMsg SyncMessage
			if err := json.Unmarshal(msg.Data, &syncMsg); err != nil {
				fmt.Printf("[ERROR] Failed to parse message: %v\n", err)
				return
			}

			handleSyncMessage(syncMsg)
		} else {
			// Binary file chunk
			handleFileChunk(msg.Data)
		}
	})

	syncDataChannel.OnClose(func() {
		fmt.Println("[INFO] Sync data channel closed")
	})
}

func handleSyncMessage(msg SyncMessage) {
	fmt.Printf("[SYNC] Received %s command for: %s\n", msg.Type, msg.Path)

	fullPath := filepath.Join(syncLocalFolder, msg.Path)

	switch msg.Type {
	case "CREATE", "UPDATE":
		// File will be sent as binary chunks
		fmt.Printf("[SYNC] Waiting for file data: %s (%d bytes)\n", msg.Path, msg.Size)

	case "DELETE":
		if err := os.Remove(fullPath); err != nil {
			fmt.Printf("[ERROR] Failed to delete file: %v\n", err)
		} else {
			fmt.Printf("[SYNC] ✓ Deleted: %s\n", msg.Path)
		}

	case "ACK":
		fmt.Printf("[SYNC] ✓ Remote peer acknowledged: %s\n", msg.Path)
	}
}

func handleFileChunk(data []byte) {
	if len(data) < 9 {
		return
	}

	// Parse chunk header
	pathLen := binary.BigEndian.Uint32(data[0:4])
	offset := binary.BigEndian.Uint32(data[4:8])
	isLast := data[8] == 1
	pathEnd := 9 + pathLen
	path := string(data[9:pathEnd])
	chunkData := data[pathEnd:]

	fullPath := filepath.Join(syncLocalFolder, path)

	// Create directory if needed
	dir := filepath.Dir(fullPath)
	os.MkdirAll(dir, 0755)

	// Open file for writing
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("[ERROR] Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	// Seek to offset
	file.Seek(int64(offset), 0)

	// Write chunk
	file.Write(chunkData)

	fmt.Printf("[SYNC] Received chunk: %s (offset: %d, size: %d, last: %v)\n",
		path, offset, len(chunkData), isLast)

	if isLast {
		fmt.Printf("[SYNC] ✓ File complete: %s\n", path)
	}
}

func startFolderWatcher() {
	var err error
	syncWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}

	// Watch the folder recursively
	err = filepath.Walk(syncLocalFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return syncWatcher.Add(path)
		}
		return nil
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("[SYSTEM] ✓ Watching folder: %s\n", syncLocalFolder)

	go func() {
		for {
			select {
			case event, ok := <-syncWatcher.Events:
				if !ok {
					return
				}

				// Get relative path
				relPath, _ := filepath.Rel(syncLocalFolder, event.Name)

				// Skip if we're currently sending this file
				syncMutex.Lock()
				if sendingFiles[relPath] {
					syncMutex.Unlock()
					continue
				}
				syncMutex.Unlock()

				if event.Op&fsnotify.Create == fsnotify.Create ||
					event.Op&fsnotify.Write == fsnotify.Write {

					// Check if it's a directory
					info, err := os.Stat(event.Name)
					if err != nil || info.IsDir() {
						continue
					}

					fmt.Printf("[WATCH] File changed: %s (%s)\n", relPath, event.Op)
					go sendFile(relPath, event.Name)

				} else if event.Op&fsnotify.Remove == fsnotify.Remove {
					fmt.Printf("[WATCH] File deleted: %s\n", relPath)
					go sendDeleteCommand(relPath)
				}

			case err, ok := <-syncWatcher.Errors:
				if !ok {
					return
				}
				fmt.Printf("[ERROR] Watcher error: %v\n", err)
			}
		}
	}()
}

func sendFile(relPath, fullPath string) {
	// Mark as sending
	syncMutex.Lock()
	sendingFiles[relPath] = true
	syncMutex.Unlock()

	defer func() {
		syncMutex.Lock()
		delete(sendingFiles, relPath)
		syncMutex.Unlock()
	}()

	// Get file info
	info, err := os.Stat(fullPath)
	if err != nil {
		fmt.Printf("[ERROR] Failed to stat file: %v\n", err)
		return
	}

	// Send metadata
	msg := SyncMessage{
		Type:     "UPDATE",
		Path:     relPath,
		Size:     info.Size(),
		Modified: info.ModTime().Unix(),
	}

	msgBytes, _ := json.Marshal(msg)
	if err := syncDataChannel.SendText(string(msgBytes)); err != nil {
		fmt.Printf("[ERROR] Failed to send metadata: %v\n", err)
		return
	}

	// Open file
	file, err := os.Open(fullPath)
	if err != nil {
		fmt.Printf("[ERROR] Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	// Send file in chunks
	buffer := make([]byte, 64*1024) // 64KB chunks
	offset := int64(0)
	chunkNum := 0

	for {
		n, err := file.Read(buffer)
		if n > 0 {
			// Build chunk packet
			pathBytes := []byte(relPath)
			packetSize := 9 + len(pathBytes) + n
			packet := make([]byte, packetSize)

			// Header
			binary.BigEndian.PutUint32(packet[0:4], uint32(len(pathBytes)))
			binary.BigEndian.PutUint32(packet[4:8], uint32(offset))

			isLast := err == io.EOF
			if isLast {
				packet[8] = 1
			} else {
				packet[8] = 0
			}

			// Path
			copy(packet[9:], pathBytes)

			// Data
			copy(packet[9+len(pathBytes):], buffer[:n])

			// Send
			if err := syncDataChannel.Send(packet); err != nil {
				fmt.Printf("[ERROR] Failed to send chunk: %v\n", err)
				return
			}

			offset += int64(n)
			chunkNum++

			fmt.Printf("[SEND] Chunk %d: %s (offset: %d, size: %d)\n",
				chunkNum, relPath, offset-int64(n), n)

			// Flow control
			time.Sleep(10 * time.Millisecond)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("[ERROR] File read error: %v\n", err)
			return
		}
	}

	fmt.Printf("[SEND] ✓ File sent: %s (%d chunks)\n", relPath, chunkNum)
}

func sendDeleteCommand(relPath string) {
	msg := SyncMessage{
		Type: "DELETE",
		Path: relPath,
	}

	msgBytes, _ := json.Marshal(msg)
	if err := syncDataChannel.SendText(string(msgBytes)); err != nil {
		fmt.Printf("[ERROR] Failed to send delete command: %v\n", err)
		return
	}

	fmt.Printf("[SEND] ✓ Delete command sent: %s\n", relPath)
}