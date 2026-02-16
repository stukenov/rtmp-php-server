<?php
/**
 * Реальный RTMP-сервер на PHP с использованием ReactPHP.
 *
 * Этот пример объединяет все необходимые компоненты:
 *   - AMF0‑кодирование/декодирование для обработки команд.
 *   - Менеджер потоков для связывания публикующего клиента и подписчиков.
 *   - Обработку рукопожатия, чанков и команд RTMP.
 *
 * Для установки зависимостей выполните:
 *   composer require react/socket react/event-loop
 *
 * Запустите сервер командой:
 *   php index.php
 */

require __DIR__ . '/vendor/autoload.php';

use React\EventLoop\Factory;
use React\Socket\Server as SocketServer;
use React\Socket\ConnectionInterface;

/**
 * Минимальный AMF0‑кодер/декодер.
 */
class Amf0 {
    public static function decode($data, &$offset = 0) {
        if ($offset >= strlen($data)) {
            throw new Exception("Нет данных для декодирования");
        }
        $type = ord($data[$offset]);
        $offset++;
        switch ($type) {
            case 0: // Number (8 байт)
                $numData = substr($data, $offset, 8);
                $offset += 8;
                // RTMP использует big-endian; для корректного порядка байт переворачиваем, если нужно
                $arr = unpack("d", strrev($numData));
                return current($arr);
            case 1: // Boolean (1 байт)
                $bool = (ord($data[$offset]) !== 0);
                $offset++;
                return $bool;
            case 2: // String (2 байта длины + данные)
                $len = unpack("n", substr($data, $offset, 2))[1];
                $offset += 2;
                $str = substr($data, $offset, $len);
                $offset += $len;
                return $str;
            case 3: // Object (ключ-значение, заканчивается маркером 0x09)
                $obj = [];
                while (true) {
                    if ($offset + 3 > strlen($data)) break;
                    $keyLen = unpack("n", substr($data, $offset, 2))[1];
                    $offset += 2;
                    if ($keyLen === 0) {
                        if (ord($data[$offset]) === 9) {
                            $offset++;
                            break;
                        }
                    }
                    $key = substr($data, $offset, $keyLen);
                    $offset += $keyLen;
                    $obj[$key] = self::decode($data, $offset);
                }
                return $obj;
            case 5: // Null
                return null;
            case 8: // ECMA Array (4 байта длины + пары ключ-значение, затем терминатор)
                $len = unpack("N", substr($data, $offset, 4))[1];
                $offset += 4;
                $obj = [];
                for ($i = 0; $i < $len; $i++) {
                    $keyLen = unpack("n", substr($data, $offset, 2))[1];
                    $offset += 2;
                    $key = substr($data, $offset, $keyLen);
                    $offset += $keyLen;
                    $obj[$key] = self::decode($data, $offset);
                }
                $offset += 3; // Пропускаем терминатор (0x000009)
                return $obj;
            case 10: // Strict Array (4 байта длины + значения)
                $len = unpack("N", substr($data, $offset, 4))[1];
                $offset += 4;
                $arr = [];
                for ($i = 0; $i < $len; $i++) {
                    $arr[] = self::decode($data, $offset);
                }
                return $arr;
            default:
                throw new Exception("Тип AMF0 не поддерживается: {$type}");
        }
    }

    public static function encode($data) {
        // Приводим объекты к массиву, если это необходимо
        if (is_object($data)) {
            $data = (array)$data;
        }
        if (is_numeric($data)) {
            return chr(0) . self::encodeNumber($data);
        } elseif (is_bool($data)) {
            return chr(1) . ($data ? chr(1) : chr(0));
        } elseif (is_string($data)) {
            $len = strlen($data);
            if ($len > 65535) {
                throw new Exception("Строка слишком длинная");
            }
            return chr(2) . pack("n", $len) . $data;
        } elseif (is_array($data)) {
            // Если массив последовательный, используем Strict Array; иначе – объект
            if (array_keys($data) === range(0, count($data) - 1)) {
                $encoded = chr(10) . pack("N", count($data));
                foreach ($data as $item) {
                    $encoded .= self::encode($item);
                }
                return $encoded;
            } else {
                $encoded = chr(3);
                foreach ($data as $key => $value) {
                    $encoded .= pack("n", strlen($key)) . $key . self::encode($value);
                }
                $encoded .= pack("n", 0) . chr(9);
                return $encoded;
            }
        } elseif (is_null($data)) {
            return chr(5);
        }
        throw new Exception("Невозможно закодировать тип " . gettype($data));
    }

    private static function encodeNumber($num) {
        $packed = pack("d", $num);
        if (pack("d", 1.0) === "\0\0\0\0\0\0\xf0?") {
            return $packed;
        } else {
            return strrev($packed);
        }
    }
}

/**
 * Менеджер потоков.
 * Сопоставляет имя потока с публикующим клиентом и списком подписчиков.
 */
class StreamManager {
    protected $streams = [];

    public function addPublisher($streamName, ConnectionInterface $conn) {
        $this->streams[$streamName]['publisher'] = $conn;
        echo "Publisher для потока \"$streamName\" добавлен\n";
    }

    public function addSubscriber($streamName, ConnectionInterface $conn) {
        if (!isset($this->streams[$streamName]['subscribers'])) {
            $this->streams[$streamName]['subscribers'] = [];
        }
        $this->streams[$streamName]['subscribers'][] = $conn;
        echo "Subscriber для потока \"$streamName\" добавлен\n";
    }

    public function removeConnection(ConnectionInterface $conn) {
        foreach ($this->streams as $streamName => &$stream) {
            if (isset($stream['publisher']) && $stream['publisher'] === $conn) {
                unset($stream['publisher']);
                echo "Publisher для потока \"$streamName\" удалён\n";
            }
            if (isset($stream['subscribers'])) {
                foreach ($stream['subscribers'] as $i => $sub) {
                    if ($sub === $conn) {
                        unset($stream['subscribers'][$i]);
                        echo "Subscriber для потока \"$streamName\" удалён\n";
                    }
                }
            }
        }
    }

    public function forwardMedia($streamName, $data) {
        if (isset($this->streams[$streamName]['subscribers'])) {
            foreach ($this->streams[$streamName]['subscribers'] as $sub) {
                $sub->write($data);
            }
        }
    }
}

/**
 * Класс, реализующий одно RTMP‑соединение.
 *
 * Поддерживаемые функции:
 * – Полное рукопожатие (C0, C1, S0, S1, S2, C2);
 * – Разбор чанков (fmt=0/1/2/3) с динамическим размером чанка (setChunkSize);
 * – Обработка команд (AMF0) и пользовательских сообщений (User Control);
 * – Минимальная маршрутизация потоков (publish/play).
 *
 * Обрабатываемые команды:
 *   connect, createStream, publish, play, deleteStream,
 *   releaseStream, closeStream, FCSubscribe, FCPublish.
 */
class RtmpConnection {
    /** @var ConnectionInterface */
    protected $connection;
    /** @var StreamManager */
    protected $streamManager;

    // Рукопожатие:
    protected $state = 'handshake';
    protected $handshakeBuffer = '';
    protected $expectedHandshakeBytes = 1537; // C0 (1 байт) + C1 (1536 байт)

    // Буфер для чанков:
    protected $buffer = '';
    protected $currentChunkSize = 128;
    protected $chunkStreams = []; // Состояние для каждого csid

    // Для команд:
    protected $streamName;
    protected $isPublisher = false;
    protected $transactionId = 0;

    public function __construct(ConnectionInterface $conn, StreamManager $manager) {
        $this->connection = $conn;
        $this->streamManager = $manager;

        $this->connection->on('data', [$this, 'onData']);
        $this->connection->on('close', [$this, 'onClose']);
    }

    public function onData($data) {
        if ($this->state !== 'handshake_done') {
            $this->handleHandshake($data);
        } else {
            $this->buffer .= $data;
            $this->processChunks();
        }
    }

    /**
     * Реализует полное рукопожатие RTMP.
     * 1. Клиент отправляет C0 (1 байт) и C1 (1536 байт).
     * 2. Сервер отправляет S0 (версия 3), S1 (случайные 1536 байт) и S2 (эхо C1).
     * 3. Клиент отправляет C2 (1536 байт) – рукопожатие завершается.
     */
    protected function handleHandshake($data) {
        $this->handshakeBuffer .= $data;
        if ($this->state === 'handshake' && strlen($this->handshakeBuffer) >= 1537) {
            $c0 = ord($this->handshakeBuffer[0]);
            $c1 = substr($this->handshakeBuffer, 1, 1536);
            echo "Получен C0: версия $c0\n";
            $s0 = chr(3);
            $s1 = random_bytes(1536);
            $s2 = $c1; // Эхо C1
            $this->connection->write($s0 . $s1 . $s2);
            echo "Отправлены S0, S1, S2\n";
            $this->handshakeBuffer = substr($this->handshakeBuffer, 1537);
            $this->state = 'handshake_c2';
        }
        if ($this->state === 'handshake_c2' && strlen($this->handshakeBuffer) >= 1536) {
            // Получен C2 – рукопожатие завершено
            $c2 = substr($this->handshakeBuffer, 0, 1536);
            $this->handshakeBuffer = substr($this->handshakeBuffer, 1536);
            echo "Получен C2, рукопожатие завершено\n";
            $this->state = 'handshake_done';
            if (strlen($this->handshakeBuffer) > 0) {
                $this->onData('');
            }
        }
    }

    /**
     * Разбирает накопленный буфер и собирает RTMP-чанки.
     */
    protected function processChunks() {
        while (true) {
            $startLen = strlen($this->buffer);
            if ($startLen < 1) break;
            // Чтение базового заголовка
            $basicHeader = ord($this->buffer[0]);
            $fmt = ($basicHeader >> 6) & 0x03;
            $csid = $basicHeader & 0x3F;
            $headerLen = 1;
            if ($csid === 0) {
                if (strlen($this->buffer) < 2) break;
                $csid = 64 + ord($this->buffer[1]);
                $headerLen = 2;
            } elseif ($csid === 1) {
                if (strlen($this->buffer) < 3) break;
                $csid = 64 + ord($this->buffer[1]) + (ord($this->buffer[2]) << 8);
                $headerLen = 3;
            }
            // Определяем размер message header в зависимости от fmt
            $msgHeaderLen = 0;
            if ($fmt === 0) {
                $msgHeaderLen = 11;
            } elseif ($fmt === 1) {
                $msgHeaderLen = 7;
            } elseif ($fmt === 2) {
                $msgHeaderLen = 3;
            }
            if (strlen($this->buffer) < $headerLen + $msgHeaderLen) break;
            $headerData = substr($this->buffer, $headerLen, $msgHeaderLen);
            $this->buffer = substr($this->buffer, $headerLen + $msgHeaderLen);
            $chunkHeader = [];
            if ($fmt === 0) {
                $timestamp = unpack("N", "\0" . substr($headerData, 0, 3))[1];
                $msgLength = unpack("N", "\0" . substr($headerData, 3, 3))[1];
                $msgType = ord($headerData[6]);
                $msgStreamId = unpack("V", substr($headerData, 7, 4))[1];
                $chunkHeader = [
                    'timestamp' => $timestamp,
                    'message_length' => $msgLength,
                    'message_type_id' => $msgType,
                    'message_stream_id' => $msgStreamId,
                ];
                $this->chunkStreams[$csid] = $chunkHeader;
                $this->chunkStreams[$csid]['data'] = '';
                $this->chunkStreams[$csid]['received'] = 0;
            } else {
                if (!isset($this->chunkStreams[$csid])) {
                    echo "Ошибка: отсутствует предыдущий заголовок для csid $csid\n";
                    return;
                }
                $prev = $this->chunkStreams[$csid];
                if ($fmt === 1) {
                    $timestampDelta = unpack("N", "\0" . substr($headerData, 0, 3))[1];
                    $msgLength = unpack("N", "\0" . substr($headerData, 3, 3))[1];
                    $msgType = ord($headerData[6]);
                    $chunkHeader = $prev;
                    $chunkHeader['timestamp'] += $timestampDelta;
                    $chunkHeader['message_length'] = $msgLength;
                    $chunkHeader['message_type_id'] = $msgType;
                    $this->chunkStreams[$csid] = $chunkHeader;
                    if (!isset($this->chunkStreams[$csid]['data'])) {
                        $this->chunkStreams[$csid]['data'] = '';
                        $this->chunkStreams[$csid]['received'] = 0;
                    }
                } elseif ($fmt === 2) {
                    $timestampDelta = unpack("N", "\0" . substr($headerData, 0, 3))[1];
                    $chunkHeader = $prev;
                    $chunkHeader['timestamp'] += $timestampDelta;
                    $this->chunkStreams[$csid] = $chunkHeader;
                } elseif ($fmt === 3) {
                    $chunkHeader = $prev;
                }
            }
            $remaining = $chunkHeader['message_length'] - $this->chunkStreams[$csid]['received'];
            $toRead = min($this->currentChunkSize, $remaining);
            if (strlen($this->buffer) < $toRead) break;
            $chunkData = substr($this->buffer, 0, $toRead);
            $this->buffer = substr($this->buffer, $toRead);
            $this->chunkStreams[$csid]['data'] .= $chunkData;
            $this->chunkStreams[$csid]['received'] += $toRead;
            if ($this->chunkStreams[$csid]['received'] == $chunkHeader['message_length']) {
                $fullMessage = $this->chunkStreams[$csid]['data'];
                $msgType = $chunkHeader['message_type_id'];
                $msgStreamId = $chunkHeader['message_stream_id'];
                $timestamp = $chunkHeader['timestamp'];
                unset($this->chunkStreams[$csid]);
                $this->handleMessage($msgType, $fullMessage, $msgStreamId, $timestamp);
            }
            if (strlen($this->buffer) === $startLen) break;
        }
    }

    /**
     * Обрабатывает полученное RTMP-сообщение в зависимости от его типа.
     */
    protected function handleMessage($msgType, $messageBody, $msgStreamId, $timestamp) {
        switch ($msgType) {
            case 1: // setChunkSize
                if (strlen($messageBody) >= 4) {
                    $this->currentChunkSize = unpack("N", substr($messageBody, 0, 4))[1];
                    echo "Новый размер чанка: {$this->currentChunkSize}\n";
                }
                break;
            case 2: // Abort
                if (strlen($messageBody) >= 4) {
                    $abortCsid = unpack("N", substr($messageBody, 0, 4))[1];
                    unset($this->chunkStreams[$abortCsid]);
                    echo "Abort: chunk stream {$abortCsid} прерван\n";
                }
                break;
            case 3: // Acknowledgement
                if (strlen($messageBody) >= 4) {
                    $ackBytes = unpack("N", substr($messageBody, 0, 4))[1];
                    echo "Получено подтверждение (Acknowledgement): {$ackBytes} байт\n";
                }
                break;
            case 4: // User Control Message
                $this->handleUserControl($messageBody);
                break;
            case 5: // Window Acknowledgement Size
                if (strlen($messageBody) >= 4) {
                    $windowSize = unpack("N", substr($messageBody, 0, 4))[1];
                    echo "Размер окна подтверждения: {$windowSize}\n";
                }
                break;
            case 6: // Set Peer Bandwidth
                if (strlen($messageBody) >= 5) {
                    $bandwidth = unpack("N", substr($messageBody, 0, 4))[1];
                    $limitType = ord($messageBody[4]);
                    echo "Установлена полоса пропускания: {$bandwidth}, тип ограничения: {$limitType}\n";
                }
                break;
            case 8: // Audio
            case 9: // Video
                if ($this->isPublisher && $this->streamName) {
                    $dataToSend = pack("C", $msgType) . $messageBody;
                    $this->streamManager->forwardMedia($this->streamName, $dataToSend);
                }
                break;
            case 20: // AMF0 Command Message
                $this->handleCommand($messageBody);
                break;
            default:
                echo "Необработанный тип сообщения: {$msgType}\n";
                break;
        }
    }

    /**
     * Обрабатывает пользовательское сообщение (User Control Message).
     */
    protected function handleUserControl($data) {
        if (strlen($data) < 2) return;
        $eventType = unpack("n", substr($data, 0, 2))[1];
        switch ($eventType) {
            case 0:
                echo "User Control: Stream Begin\n";
                break;
            case 1:
                echo "User Control: Stream EOF\n";
                break;
            case 3:
                echo "User Control: Ping Request\n";
                // Отправляем Ping Response (event type 7) с теми же данными
                $this->sendUserControl(7, substr($data, 2));
                break;
            default:
                echo "User Control: Event type {$eventType}\n";
                break;
        }
    }

    protected function sendUserControl($eventType, $eventData = "") {
        $payload = pack("n", $eventType) . $eventData;
        $this->sendMessage(4, 0, 0, $payload);
    }

    /**
     * Обрабатывает командное сообщение (AMF0).
     * Формат команды: [commandName (string), transactionId (number), commandObject (object), ...]
     */
    protected function handleCommand($data) {
        $offset = 0;
        try {
            $command = Amf0::decode($data, $offset);
        } catch (Exception $e) {
            echo "Ошибка AMF0 decode: " . $e->getMessage() . "\n";
            return;
        }
        if (!is_string($command)) {
            echo "Неверный формат команды\n";
            return;
        }
        try {
            $transactionId = Amf0::decode($data, $offset);
        } catch (Exception $e) {
            echo "Ошибка decode transactionId: " . $e->getMessage() . "\n";
            return;
        }
        $commandObject = Amf0::decode($data, $offset);
        $additional = [];
        while ($offset < strlen($data)) {
            $additional[] = Amf0::decode($data, $offset);
        }
        echo "Получена команда: {$command}\n";
        $this->transactionId = $transactionId;
        switch ($command) {
            case "connect":
                $this->handleConnect($transactionId, $commandObject, $additional);
                break;
            case "createStream":
                $this->handleCreateStream($transactionId, $commandObject, $additional);
                break;
            case "publish":
                $this->handlePublish($transactionId, $commandObject, $additional);
                break;
            case "play":
                $this->handlePlay($transactionId, $commandObject, $additional);
                break;
            case "deleteStream":
                $this->handleDeleteStream($transactionId, $commandObject, $additional);
                break;
            case "releaseStream":
                $this->handleReleaseStream($transactionId, $commandObject, $additional);
                break;
            case "closeStream":
                $this->handleCloseStream($transactionId, $commandObject, $additional);
                break;
            case "FCSubscribe":
                $this->handleFCSubscribe($transactionId, $commandObject, $additional);
                break;
            case "FCPublish":
                $this->handleFCPublish($transactionId, $commandObject, $additional);
                break;
            default:
                echo "Команда не распознана: {$command}\n";
                break;
        }
    }

    protected function handleConnect($transactionId, $commandObject, $additional) {
        // Используем ассоциативные массивы вместо объектов для ответа
        $response = [
            "_result",
            $transactionId,
            [
                "fmsVer" => "FMS/3,5,7,7009",
                "capabilities" => 31
            ],
            [
                "level" => "status",
                "code" => "NetConnection.Connect.Success",
                "description" => "Соединение установлено",
                "objectEncoding" => 0
            ]
        ];
        $payload = "";
        foreach ($response as $item) {
            $payload .= Amf0::encode($item);
        }
        $this->sendMessage(20, 0, 0, $payload);
    }

    protected function handleCreateStream($transactionId, $commandObject, $additional) {
        $response = [
            "_result",
            $transactionId,
            null,
            1 // Идентификатор потока
        ];
        $payload = "";
        foreach ($response as $item) {
            $payload .= Amf0::encode($item);
        }
        $this->sendMessage(20, 0, 0, $payload);
    }

    protected function handlePublish($transactionId, $commandObject, $additional) {
        // Если в additional передано название потока, используем его; иначе – "default"
        $this->streamName = isset($additional[0]) ? $additional[0] : "default";
        $this->isPublisher = true;
        $this->streamManager->addPublisher($this->streamName, $this->connection);
        echo "Начало публикации потока: {$this->streamName}\n";
    }

    protected function handlePlay($transactionId, $commandObject, $additional) {
        // Если в additional передано название потока, используем его; иначе – "default"
        $this->streamName = isset($additional[0]) ? $additional[0] : "default";
        $this->isPublisher = false;
        $this->streamManager->addSubscriber($this->streamName, $this->connection);
        echo "Начало воспроизведения потока: {$this->streamName}\n";
    }

    protected function handleDeleteStream($transactionId, $commandObject, $additional) {
        echo "Получена команда deleteStream\n";
        $response = [
            "_result",
            $transactionId,
            null,
            true
        ];
        $payload = "";
        foreach ($response as $item) {
            $payload .= Amf0::encode($item);
        }
        $this->sendMessage(20, 0, 0, $payload);
    }

    protected function handleReleaseStream($transactionId, $commandObject, $additional) {
        echo "Получена команда releaseStream\n";
        // Здесь можно освободить ресурсы, связанные с потоком
    }

    protected function handleCloseStream($transactionId, $commandObject, $additional) {
        echo "Получена команда closeStream\n";
        // Здесь можно закрыть поток и освободить ресурсы
    }

    protected function handleFCSubscribe($transactionId, $commandObject, $additional) {
        echo "Получена команда FCSubscribe\n";
        // Обычно не требует ответа
    }

    protected function handleFCPublish($transactionId, $commandObject, $additional) {
        echo "Получена команда FCPublish\n";
        // Обычно не требует ответа
    }

    /**
     * Формирует и отправляет RTMP‑сообщение.
     * Если полезная нагрузка превышает размер чанка, сообщение разбивается на части.
     */
    protected function sendMessage($msgType, $streamId, $timestamp, $payload) {
        $csid = 3; // Фиксированный csid для серверных сообщений
        $basicHeader = chr((0 << 6) | $csid); // fmt=0
        $timestampBytes = substr(pack("N", $timestamp), 1); // 3 байта
        $length = strlen($payload);
        $lengthBytes = substr(pack("N", $length), 1); // 3 байта
        $typeByte = chr($msgType);
        $streamIdBytes = pack("V", $streamId); // 4 байта (little-endian)
        $header = $basicHeader . $timestampBytes . $lengthBytes . $typeByte . $streamIdBytes;
        if ($length <= $this->currentChunkSize) {
            $this->connection->write($header . $payload);
        } else {
            $chunks = str_split($payload, $this->currentChunkSize);
            $this->connection->write($header . array_shift($chunks));
            foreach ($chunks as $chunk) {
                // Последующие чанки отправляем с fmt=3 (только базовый заголовок)
                $basicHeaderSub = chr((3 << 6) | $csid);
                $this->connection->write($basicHeaderSub . $chunk);
            }
        }
    }

    public function onClose() {
        echo "Соединение закрыто: " . $this->connection->getRemoteAddress() . "\n";
        $this->streamManager->removeConnection($this->connection);
    }
}

// Создаем цикл событий и запускаем RTMP-сервер на порту 1935
$loop = Factory::create();
$socketServer = new SocketServer('0.0.0.0:1935', $loop);
$streamManager = new StreamManager();

echo "RTMP-сервер запущен на порту 1935\n";

$socketServer->on('connection', function (ConnectionInterface $conn) use ($streamManager) {
    new RtmpConnection($conn, $streamManager);
});

$loop->run();
