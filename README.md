# RTMP PHP Server

A PHP implementation of an RTMP (Real-Time Messaging Protocol) server using ReactPHP for asynchronous event-driven networking. This minimal viable product (MVP) demonstrates RTMP protocol handling in PHP for live video streaming applications.

## Overview

This is a from-scratch RTMP server implementation in PHP that handles the core RTMP protocol including handshake, chunking, AMF0 encoding/decoding, and basic stream management. Built on ReactPHP's event loop and socket server for non-blocking I/O operations.

## Features

- **RTMP Handshake Protocol**: Complete C0/S0, C1/S1, C2/S2 handshake implementation
- **Chunking Protocol**: Support for RTMP chunk streaming with configurable chunk sizes
- **AMF0 Codec**: Minimal AMF0 encoder/decoder for command messages
- **Stream Management**: Publisher and subscriber connection handling
- **Asynchronous I/O**: Built on ReactPHP for efficient non-blocking operations
- **Pure PHP**: No external binary dependencies (FFmpeg not required)

## Prerequisites

- PHP 7.4 or later
- Composer
- ReactPHP socket and event-loop packages

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/stukenov/rtmp-php-server.git
   cd rtmp-php-server
   ```

2. Install dependencies:
   ```bash
   composer install
   ```

## Usage

### Starting the Server

Run the RTMP server:

```bash
php index.php
```

The server will start listening on `0.0.0.0:1935` (default RTMP port).

### Publishing a Stream

Use FFmpeg or OBS to publish a stream to the server:

```bash
# Using FFmpeg
ffmpeg -re -i input.mp4 -c copy -f flv rtmp://localhost/live/mystream

# Using OBS
# Server: rtmp://localhost/live
# Stream Key: mystream
```

### Playing a Stream

Use a media player to consume the stream:

```bash
# Using FFplay
ffplay rtmp://localhost/live/mystream

# Using VLC
# Media -> Open Network Stream -> rtmp://localhost/live/mystream
```

## Architecture

### Core Components

1. **AMF0 Codec** (`Amf0` class)
   - Encodes and decodes AMF0 data types
   - Supports numbers, booleans, strings, objects, arrays

2. **Stream Manager** (`StreamManager` class)
   - Manages active streams and connections
   - Routes video/audio data from publishers to subscribers
   - Handles stream lifecycle

3. **Connection Handler**
   - Manages RTMP handshake
   - Processes incoming chunks
   - Dispatches commands (connect, publish, play)

### RTMP Message Flow

1. Client connects and performs handshake (C0, C1, C2)
2. Server responds with handshake (S0, S1, S2)
3. Client sends `connect` command with application name
4. Client publishes or plays a stream
5. Data is chunked and routed between connections

### Supported RTMP Commands

- `connect`: Application connection
- `releaseStream`: Stream preparation
- `FCPublish`: Flash client publish start
- `createStream`: Stream creation
- `publish`: Start publishing
- `play`: Start playback
- `deleteStream`: Stream cleanup
- `FCUnpublish`: Flash client publish end

## Configuration

Edit `index.php` to configure:

```php
// Server address and port
$server = new SocketServer('0.0.0.0:1935', [], $loop);

// Chunk size
$chunkSize = 4096;
```

## AMF0 Data Types

The server supports the following AMF0 types:
- **Type 0**: Number (double, 8 bytes)
- **Type 1**: Boolean (1 byte)
- **Type 2**: String (2-byte length + data)
- **Type 3**: Object (key-value pairs)
- **Type 5**: Null
- **Type 8**: ECMA Array
- **Type 10**: Strict Array

## Development

### Testing

Test with a simple publish-subscribe flow:

```bash
# Terminal 1: Start server
php index.php

# Terminal 2: Publish stream
ffmpeg -re -i test.mp4 -c copy -f flv rtmp://localhost/live/test

# Terminal 3: Play stream
ffplay rtmp://localhost/live/test
```

### Debugging

Enable verbose output by adding logging:

```php
echo "Received command: {$commandName}\n";
print_r($commandObject);
```

## Limitations

- **MVP Implementation**: Basic functionality without advanced features
- **No Authentication**: No built-in security or authorization
- **No Recording**: Streams are not persisted to disk
- **No Transcoding**: Video/audio passed through without modification
- **Single Process**: No horizontal scaling support
- **Memory-Based**: All stream data held in memory

## Performance Considerations

- ReactPHP provides non-blocking I/O for multiple concurrent connections
- Memory usage grows with number of active streams and subscribers
- Consider process managers (Supervisor, systemd) for production deployment
- Use reverse proxy (nginx-rtmp) for better performance in production

## Use Cases

- Educational RTMP server implementation
- Proof of concept for PHP-based streaming
- Understanding RTMP protocol internals
- Lightweight streaming for development/testing
- Custom RTMP protocol extensions

## Advanced Usage

### Custom Stream Keys

Modify the stream manager to add authentication:

```php
public function publish($streamKey, $connection) {
    if (!$this->validateStreamKey($streamKey)) {
        return false;
    }
    // ... existing code
}
```

### Metadata Handling

Add custom metadata processing:

```php
// In onAudioMessage or onVideoMessage
if ($this->isMetadata($data)) {
    $this->processMetadata($data);
}
```

## Production Recommendations

For production use, consider:
- Using nginx-rtmp module for better performance
- Adding authentication and authorization
- Implementing stream recording
- Adding monitoring and health checks
- Using load balancers for scaling
- Implementing DRM and encryption

## Dependencies

- `react/socket`: ^1.15
- `react/event-loop`: ^1.5

## License

MIT License - see LICENSE file for details.

Copyright (c) 2025 Saken Tukenov

## References

- [RTMP Specification](https://rtmp.veriskope.com/docs/spec/)
- [ReactPHP Documentation](https://reactphp.org/)
- [AMF0 Specification](https://rtmp.veriskope.com/pdf/amf0-file-format-specification.pdf)
