const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 100e6,
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling']
});
const cors = require('cors');
const os = require('os');
const path = require('path');

app.use(cors());
app.use(express.static(path.join(__dirname, '..')));

const rooms = {};
const audioFiles = {};

function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

const localIP = getLocalIP();

io.on('connection', (socket) => {
  console.log('✅ Device connected:', socket.id);
  socket.emit('serverInfo', { localIP });

  socket.on('createRoom', (data) => {
    const { roomCode, hostName } = data;
    
    if (rooms[roomCode] && rooms[roomCode].hostId !== socket.id) {
      socket.emit('error', { message: 'Room code already exists' });
      return;
    }
    
    rooms[roomCode] = {
      hostId: socket.id,
      hostName: hostName || 'Host',
      listeners: [],
      currentSong: null,
      position: 0,
      isPlaying: false,
      queue: [],
      playlist: [],
      duration: 0,
      isActive: false,
      volume: 100,
      isShuffled: false,
      repeatMode: 'off',
      createdAt: Date.now(),
      lastSync: Date.now()
    };
    
    socket.join(roomCode);
    socket.roomCode = roomCode;
    socket.isHost = true;
    console.log(`🎉 Room created: ${roomCode} by ${hostName || socket.id}`);
    socket.emit('roomCreated', { roomCode, isActive: false });
  });

  socket.on('activateRoom', (data) => {
    const { roomCode } = data;
    if (rooms[roomCode] && rooms[roomCode].hostId === socket.id) {
      rooms[roomCode].isActive = true;
      console.log(`✅ Room activated: ${roomCode}`);
      
      // Notify only listeners (not host)
      socket.to(roomCode).emit('roomActivated', { 
        roomCode,
        isActive: true 
      });
      
      // Send initial state to listeners only
      const room = rooms[roomCode];
      socket.to(roomCode).emit('syncPlayback', {
        currentSong: room.currentSong,
        position: room.position,
        isPlaying: room.isPlaying,
        queue: room.queue,
        playlist: room.playlist,
        duration: room.duration,
        repeatMode: room.repeatMode,
        isShuffled: room.isShuffled,
        timestamp: Date.now(),
        forceSync: true
      });
    }
  });

  socket.on('uploadAudio', (data) => {
    const { roomCode, songId, audioData, songName, duration, artist } = data;
    
    if (!rooms[roomCode] || rooms[roomCode].hostId !== socket.id) {
      return;
    }

    const fileKey = `${roomCode}_${songId}`;
    audioFiles[fileKey] = {
      data: audioData,
      name: songName,
      duration: duration || 0,
      artist: artist || 'Unknown Artist'
    };

    console.log(`📤 Audio uploaded: ${songName} for room ${roomCode}`);
    socket.emit('audioUploaded', { songId, songName });
  });

  socket.on('joinRoom', (data) => {
    const { roomCode, listenerName } = data;
    
    if (!rooms[roomCode]) {
      socket.emit('error', { message: 'Room not found' });
      return;
    }

    const room = rooms[roomCode];
    const alreadyJoined = room.listeners.some(l => l.id === socket.id);
    
    if (!alreadyJoined) {
      room.listeners.push({
        id: socket.id,
        name: listenerName || `Listener ${room.listeners.length + 1}`,
        joinedAt: Date.now()
      });
      console.log(`👋 ${listenerName || socket.id} joined room: ${roomCode}`);
    }
    
    socket.join(roomCode);
    socket.roomCode = roomCode;
    socket.isHost = false;

    // Send complete room state
    socket.emit('roomJoined', {
      roomCode,
      hostName: room.hostName,
      currentSong: room.currentSong,
      position: room.position,
      isPlaying: room.isPlaying,
      queue: room.queue,
      playlist: room.playlist,
      duration: room.duration,
      repeatMode: room.repeatMode,
      isShuffled: room.isShuffled,
      isActive: room.isActive
    });

    // Send audio files if party is active
    if (room.isActive) {
      // Send current song immediately
      if (room.currentSong) {
        const fileKey = `${roomCode}_${room.currentSong.id}`;
        if (audioFiles[fileKey]) {
          setTimeout(() => {
            socket.emit('receiveAudio', {
              songId: room.currentSong.id,
              audioData: audioFiles[fileKey].data,
              songName: audioFiles[fileKey].name
            });
          }, 50);
        }
      }

      // Send next 2-3 songs for preloading
      if (room.queue && room.queue.length > 0) {
        const songsToPreload = room.queue.slice(0, 3);
        songsToPreload.forEach((song, index) => {
          const fileKey = `${roomCode}_${song.id}`;
          if (audioFiles[fileKey]) {
            setTimeout(() => {
              socket.emit('receiveAudio', {
                songId: song.id,
                audioData: audioFiles[fileKey].data,
                songName: audioFiles[fileKey].name,
                isPreload: true
              });
            }, 150 + (index * 50));
          }
        });
      }
    }

    // Notify host only
    io.to(room.hostId).emit('listenerJoined', {
      listeners: room.listeners
    });
  });

  socket.on('requestAudio', (data) => {
    const { roomCode, songId } = data;
    const fileKey = `${roomCode}_${songId}`;
    if (audioFiles[fileKey]) {
      socket.emit('receiveAudio', {
        songId: songId,
        audioData: audioFiles[fileKey].data,
        songName: audioFiles[fileKey].name
      });
      console.log(`📤 Sent audio ${songId} to ${socket.id}`);
    }
  });

  socket.on('updatePlayback', (data) => {
    const { roomCode, currentSong, position, isPlaying, queue, playlist, duration, repeatMode, isShuffled } = data;
    
    if (!rooms[roomCode] || rooms[roomCode].hostId !== socket.id) {
      return;
    }
    
    const room = rooms[roomCode];
    room.currentSong = currentSong;
    room.position = position;
    room.isPlaying = isPlaying;
    room.queue = queue || room.queue;
    room.playlist = playlist || room.playlist;
    room.duration = duration || room.duration;
    room.lastSync = Date.now();
    if (repeatMode !== undefined) room.repeatMode = repeatMode;
    if (isShuffled !== undefined) room.isShuffled = isShuffled;

    // ✅ CRITICAL FIX: Broadcast to LISTENERS ONLY (exclude host)
    socket.to(roomCode).emit('syncPlayback', {
      currentSong,
      position,
      isPlaying,
      queue: room.queue,
      playlist: room.playlist,
      duration: room.duration,
      repeatMode: room.repeatMode,
      isShuffled: room.isShuffled,
      timestamp: Date.now()
    });
  });

  socket.on('songChanged', (data) => {
    const { roomCode, currentSong, queue, playlist, duration } = data;
    
    if (!rooms[roomCode] || rooms[roomCode].hostId !== socket.id) {
      return;
    }

    const room = rooms[roomCode];
    room.currentSong = currentSong;
    room.queue = queue;
    room.playlist = playlist;
    room.position = 0;
    room.duration = duration;
    room.isPlaying = true;

    console.log(`🎵 Song changed in room ${roomCode}: ${currentSong ? currentSong.name : 'None'}`);

    // Broadcast to listeners only
    socket.to(roomCode).emit('loadNewSong', {
      currentSong,
      queue,
      playlist,
      duration,
      timestamp: Date.now()
    });

    // Send audio to listeners
    if (currentSong) {
      const fileKey = `${roomCode}_${currentSong.id}`;
      if (audioFiles[fileKey]) {
        socket.to(roomCode).emit('receiveAudio', {
          songId: currentSong.id,
          audioData: audioFiles[fileKey].data,
          songName: audioFiles[fileKey].name
        });
      }
    }
  });

  socket.on('playlistUpdated', (data) => {
    const { roomCode, playlist } = data;
    
    if (!rooms[roomCode] || rooms[roomCode].hostId !== socket.id) {
      return;
    }

    rooms[roomCode].playlist = playlist;
    console.log(`📝 Playlist updated in room ${roomCode}`);

    // Broadcast to listeners only
    socket.to(roomCode).emit('playlistUpdated', {
      playlist
    });
  });

  socket.on('queueUpdated', (data) => {
    const { roomCode, newSongs } = data;
    
    if (!rooms[roomCode] || rooms[roomCode].hostId !== socket.id) {
      return;
    }

    console.log(`📝 Queue updated in room ${roomCode}, new songs: ${newSongs.length}`);

    newSongs.forEach(song => {
      const fileKey = `${roomCode}_${song.id}`;
      if (audioFiles[fileKey]) {
        socket.to(roomCode).emit('receiveAudio', {
          songId: song.id,
          audioData: audioFiles[fileKey].data,
          songName: audioFiles[fileKey].name,
          isQueue: true
        });
      }
    });
  });

  socket.on('sendReaction', (data) => {
    const { roomCode, reaction, senderName } = data;
    if (rooms[roomCode]) {
      // Send to everyone in room
      io.to(roomCode).emit('receiveReaction', {
        reaction,
        senderName,
        timestamp: Date.now()
      });
    }
  });

  socket.on('sendMessage', (data) => {
    const { roomCode, message, senderName, isHost } = data;
    if (rooms[roomCode]) {
      // Send to everyone in room
      io.to(roomCode).emit('receiveMessage', {
        message,
        senderName,
        isHost: isHost || false,
        timestamp: Date.now(),
        senderId: socket.id
      });
    }
  });

  socket.on('leaveRoom', () => {
    handleDisconnect(socket);
  });

  socket.on('disconnect', () => {
    console.log('❌ Device disconnected:', socket.id);
    handleDisconnect(socket);
  });

  function handleDisconnect(socket) {
    const userRoomCode = socket.roomCode;
    
    if (userRoomCode && rooms[userRoomCode]) {
      const room = rooms[userRoomCode];

      if (socket.isHost && room.hostId === socket.id) {
        console.log(`🚪 Host left room ${userRoomCode}`);
        
        setTimeout(() => {
          if (rooms[userRoomCode] && rooms[userRoomCode].hostId === socket.id) {
            for (const key in audioFiles) {
              if (key.startsWith(`${userRoomCode}_`)) {
                delete audioFiles[key];
              }
            }
            
            io.to(userRoomCode).emit('hostDisconnected');
            delete rooms[userRoomCode];
            console.log(`🗑️ Room ${userRoomCode} deleted`);
          }
        }, 30000);
        
      } else {
        room.listeners = room.listeners.filter(l => l.id !== socket.id);
        console.log(`👋 Listener left room ${userRoomCode}`);
        
        if (room.hostId) {
          io.to(room.hostId).emit('listenerLeft', {
            listeners: room.listeners
          });
        }
      }
    }
  }
});

const PORT = process.env.PORT || 3000;

http.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 Music Party Server Running!`);
  console.log(`┌────────────────────────────────────────┐`);
  console.log(`📱 Local: http://localhost:${PORT}`);
  console.log(`🌐 Network: http://${localIP}:${PORT}`);
  console.log(`└────────────────────────────────────────┘`);
  console.log(`\n💡 To connect from other devices:`);
  console.log(`   1. Connect devices to the same WiFi network`);
  console.log(`   2. Open http://${localIP}:${PORT} on any device`);
  console.log(`   3. Share the room code or QR code`);
  console.log(`\n💾 Max file size: 100MB`);
  console.log(`⚡ Real-time sync enabled`);
  console.log(`┌────────────────────────────────────────┐\n`);
});