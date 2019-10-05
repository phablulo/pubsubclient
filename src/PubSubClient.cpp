/*
  PubSubClient.cpp - A simple client for MQTT.
  Nick O'Leary
  http://knolleary.net
*/

#include "PubSubClient.h"
#include "Arduino.h"

PubSubClient::PubSubClient() :
_state(MQTT_DISCONNECTED),
_client(nullptr),
_stream(nullptr),
_callback(nullptr)
{
}

PubSubClient::PubSubClient(Client & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(nullptr)
{
}

PubSubClient::PubSubClient(IPAddress addr, 
                           uint16_t  port, 
                           Client  & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(nullptr)
{
  setServer(addr, port);
}

PubSubClient::PubSubClient(IPAddress addr, 
                           uint16_t  port, 
                           Client  & client, 
                           Stream  & stream) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(&stream),
_callback(nullptr)
{
  setServer(addr, port);
}

PubSubClient::PubSubClient(IPAddress addr, 
                           uint16_t  port, 
                           MQTT_CALLBACK_SIGNATURE(callback), 
                           Client  & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(callback)
{
  setServer(addr, port);
}

PubSubClient::PubSubClient(IPAddress addr, 
                           uint16_t  port, 
                           MQTT_CALLBACK_SIGNATURE(callback), 
                           Client & client, 
                           Stream & stream) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(&stream),
_callback(callback)
{
  setServer(addr, port);
}

PubSubClient::PubSubClient(uint8_t * ip, 
                           uint16_t  port, 
                           Client  & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(nullptr)
{
  setServer(ip, port);
}

PubSubClient::PubSubClient(uint8_t * ip, 
                           uint16_t  port, 
                           Client  & client, 
                           Stream  & stream) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(&stream),
_callback(nullptr)
{
  setServer(ip, port);
}

PubSubClient::PubSubClient(uint8_t * ip, 
                           uint16_t  port, 
                           MQTT_CALLBACK_SIGNATURE(callback), 
                           Client  & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(callback)
{
  setServer(ip, port);
}

PubSubClient::PubSubClient(uint8_t * ip, 
                           uint16_t  port, 
                           MQTT_CALLBACK_SIGNATURE(callback), 
                           Client  & client, 
                           Stream  & stream) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(&stream),
_callback(callback)
{
  setServer(ip, port);
}

PubSubClient::PubSubClient(const char * domain, 
                           uint16_t     port, 
                           Client     & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(nullptr)
{
  setServer(domain, port);
}

PubSubClient::PubSubClient(const char * domain, 
                           uint16_t     port, 
                           Client     & client, 
                           Stream     & stream) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(&stream),
_callback(nullptr)
{
  setServer(domain, port);
}

PubSubClient::PubSubClient(const char * domain, 
                           uint16_t     port, 
                           MQTT_CALLBACK_SIGNATURE(callback), 
                           Client     & client) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(nullptr),
_callback(callback)
{
  setServer(domain, port);
}

PubSubClient::PubSubClient(const char * domain, 
                           uint16_t     port, 
                           MQTT_CALLBACK_SIGNATURE(callback), 
                           Client     & client, 
                           Stream     & stream) :
_state(MQTT_DISCONNECTED),
_client(&client),
_stream(&stream),
_callback(callback)
{
  setServer(domain, port);
}

boolean PubSubClient::connect(const char * id, 
                              const char * user, 
                              const char * pass, 
                              const char * willTopic, 
                              uint8_t      willQos, 
                              boolean      willRetain, 
                              const char * willMessage, 
                              boolean      cleanSession) 
{
  if (connected()) return true;

  boolean result;

  if (_domain == nullptr) {
    result = _client->connect(_ip, _port);
  }
  else {
    result = _client->connect(_domain, _port);
  }

  if (result) {
    nextMsgId = 1;
    // Leave room in the buffer for header and variable length field
    uint16_t length = MQTT_MAX_HEADER_SIZE;

    #if MQTT_VERSION == MQTT_VERSION_3_1
      uint8_t d[9] = { 0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', MQTT_VERSION };
      #define MQTT_HEADER_VERSION_LENGTH 9
    #elif MQTT_VERSION == MQTT_VERSION_3_1_1
      uint8_t d[7] = { 0x00, 0x04, 'M', 'Q', 'T', 'T', MQTT_VERSION };
      #define MQTT_HEADER_VERSION_LENGTH 7
    #endif
            
    for (unsigned int i = 0; i < MQTT_HEADER_VERSION_LENGTH; i++) {
      buffer[length++] = d[i];
    }

    uint8_t v;

    if (willTopic != nullptr) {
      v = 0x04 | (willQos << 3) | (willRetain << 5);
    } 
    else {
      v = 0x00;
    }

    if (cleanSession) v = v | 0x02;

    if (user != nullptr) {
      v = v | 0x80;

      if(pass != nullptr) {
        v = v | (0x80 >> 1);
      }
    }

    buffer[length++] = v;

    buffer[length++] = (MQTT_KEEPALIVE) >> 8;
    buffer[length++] = (MQTT_KEEPALIVE) & 0xFF;

    if (!check_and_write(&length, id)) return false;
 
    if (willTopic != nullptr) {
      if (!check_and_write(&length, willTopic  )) return false;
      if (!check_and_write(&length, willMessage)) return false;
    }

    if (user != nullptr) {
      if (!check_and_write(&length, user)) return false;
      if (pass != nullptr) {
        if (!check_and_write(&length, pass)) return false;
      }
    }

    write(MQTTCONNECT, buffer, length - MQTT_MAX_HEADER_SIZE);

    lastInActivity = lastOutActivity = millis();

    while (!_client->available()) {
      if ((millis() - lastInActivity) > (MQTT_SOCKET_TIMEOUT * 1000UL)) {
        _state = MQTT_CONNECTION_TIMEOUT;
        _client->stop();
        return false;
      }
    }

    uint8_t  llen;
    uint32_t len = readPacket(&llen);

    if (len == 4) {
      if (buffer[3] == 0) {
        lastInActivity = millis();
        pingOutstanding = false;
        _state = MQTT_CONNECTED;
        return true;
      } 
      else {
        _state = buffer[3];
      }
    }
    _client->stop();
  } 
  else {
    _state = MQTT_CONNECT_FAILED;
  }

  return false;
}

// reads a byte into result
boolean PubSubClient::readByte(uint8_t * result) 
{
  uint32_t previousMillis = millis();
  while (!_client->available()) {
    yield();
    if ((millis() - previousMillis) > (MQTT_SOCKET_TIMEOUT * 1000UL)) {
      return false;
    }
  }
  *result = _client->read();
  return true;
}

// reads a byte into result[*index] and increments index
boolean PubSubClient::readByte(uint8_t * result, uint16_t * index)
{
  if (readByte(&result[*index])) {
    *index += 1;
    return true;
  }
  return false;
}

uint32_t PubSubClient::readPacket(uint8_t * lengthLength) 
{
  uint16_t len = 0;

  if (!readByte(buffer, &len)) return 0;
    
  bool isPublish = (buffer[0] & 0xF0) == MQTTPUBLISH;
    
  uint32_t shift      = 0;
  uint32_t length     = 0;
  uint16_t skip       = 0;
  uint8_t  digit      = 0;
  uint8_t  start      = 0;

  do {
    if (len >= 5) { // Cannot be larger than 4 bytes, including buffer type
      // Invalid remaining length encoding - kill the connection
      _state = MQTT_DISCONNECTED;
      _client->stop();
      return 0;
    }
    
    if (!readByte(&digit)) return 0;

    buffer[len++] = digit;
    length += (digit & 0x7F) << shift;
    shift  += 7;
  } while ((digit & 0x80) != 0);

  *lengthLength = len - 1;

  if (isPublish) {
    // Read in topic length to calculate bytes to skip over for Stream writing
    if (!readByte(buffer, &len)) return 0;
    if (!readByte(buffer, &len)) return 0;
    skip = (buffer[*lengthLength + 1] << 8) + buffer[*lengthLength + 2];
    start = 2;
    if (buffer[0] & MQTTQOS1) {
      // skip message id
      skip += 2;
    }
  }

  uint32_t idx = len;

  for (uint32_t i = start; i < length; i++) {
    if (!readByte(&digit)) return 0;
    if (_stream) {
      if (isPublish && ((idx - *lengthLength - 2) > skip)) {
        _stream->write(digit);
      }
    }
    if (len < MQTT_MAX_PACKET_SIZE) {
      buffer[len] = digit;
      len++;
    }
    idx++;
  }

  if (!_stream && (idx > MQTT_MAX_PACKET_SIZE)) {
    len = 0; // This will cause the packet to be ignored.
  }

  return len;
}

boolean PubSubClient::loop() 
{
  if (!connected()) return false;

  unsigned long t = millis();

  if (((t - lastInActivity ) > (MQTT_KEEPALIVE * 1000UL)) || 
      ((t - lastOutActivity) > (MQTT_KEEPALIVE * 1000UL))) {

    if (pingOutstanding) {
      _state = MQTT_CONNECTION_TIMEOUT;
      _client->stop();
      return false;
    } 
    else {
      buffer[0] = MQTTPINGREQ;
      buffer[1] = 0;
      _client->write(buffer, 2);
      lastOutActivity = t;
      lastInActivity  = t;
      pingOutstanding = true;
    }
  }

  if (_client->available()) {

    uint8_t   llen;
    uint32_t  len   = readPacket(&llen);
    uint16_t  msgId = 0;
    uint8_t * payload;

    if (len > 0) {
      lastInActivity = t;
      uint8_t type = buffer[0] & 0xF0;
      if (type == MQTTPUBLISH) {

        if (_callback) {
          uint16_t tl = (buffer[llen + 1] << 8) + buffer[llen + 2]; // topic length in bytes
          memmove(&buffer[llen + 2], &buffer[llen + 3], tl);        // move topic inside buffer 1 byte to front
          buffer[llen + 2 + tl] = 0;                                // end the topic as a 'C' string with \x00
          char * topic = (char*) &buffer[llen + 2];

          // msgId only present for QOS > 0
          if ((buffer[0] & 0x06) == MQTTQOS1) {
            msgId   = (buffer[llen + 3 + tl] << 8) + buffer[llen + 3 + tl + 1];
            payload = &buffer[llen + 3 + tl + 2];
            _callback(topic, payload, len - llen - 3 - tl - 2);

            buffer[0] = MQTTPUBACK;
            buffer[1] = 2;
            buffer[2] = (msgId >> 8);
            buffer[3] = (msgId & 0xFF);
            _client->write(buffer, 4);
            lastOutActivity = t;

          } 
          else {
            payload = &buffer[llen + 3 + tl];
            _callback(topic, payload, len - llen - 3 - tl);
          }
        }
      } 
      else if (type == MQTTPINGREQ) {
        buffer[0] = MQTTPINGRESP;
        buffer[1] = 0;
        _client->write(buffer, 2);
      } 
      else if (type == MQTTPINGRESP) {
        pingOutstanding = false;
      }
    } 
    else if (!connected()) {
      // readPacket has closed the connection
      return false;
    }
  }
  return true;
}

boolean PubSubClient::publish(const char * topic, 
                              const uint8_t * payload, 
                              unsigned int plength, 
                              boolean retained)
{
  if (!connected()) return false;
  if (MQTT_MAX_PACKET_SIZE < (MQTT_MAX_HEADER_SIZE + 2 + strlen(topic) + plength)) return false;

  // Leave room in the buffer for header and variable length field
  uint16_t length = MQTT_MAX_HEADER_SIZE;
  length = writeString(topic, buffer, length);
  uint16_t i;

  for (i = 0; i < plength; i++) {
    buffer[length++] = payload[i];
  }

  uint8_t header = MQTTPUBLISH;
  if (retained) {
    header |= 1;
  }

  return write(header, buffer, length - MQTT_MAX_HEADER_SIZE);
}

boolean PubSubClient::publish_P(const char    * topic, 
                                const uint8_t * payload, 
                                unsigned int    plength, 
                                boolean         retained) 
{
  if (!connected()) return false;

  unsigned int rc     = 0;
  unsigned int pos    = 0;
  uint16_t     tlen   = strlen(topic);
  uint8_t      llen   = 0;
  uint8_t      header = MQTTPUBLISH;

  if (retained) header |= 1;

  buffer[pos++] = header;

  unsigned int len = plength + 2 + tlen;

  do {
    uint8_t digit = len & 0x7F;
    len >>= 7;
  
    if (len > 0) digit |= 0x80;
  
    buffer[pos++] = digit;
    llen++;
  } while (len > 0);

  pos = writeString(topic, buffer, pos);

  rc += _client->write(buffer, pos);

  for (unsigned int i = 0; i < plength; i++) {
    rc += _client->write((char) pgm_read_byte_near(payload + i));
  }

  lastOutActivity = millis();

  return rc == (tlen + 4 + plength);
}

boolean PubSubClient::beginPublish(const char* topic, unsigned int plength, boolean retained) 
{
  if (!connected()) return false;

  // Send the header and variable length field
  uint16_t length = writeString(topic, buffer, MQTT_MAX_HEADER_SIZE);
  uint8_t  header = MQTTPUBLISH;

  if (retained) header |= 1;

  size_t hlen = buildHeader(header, buffer, plength + length - MQTT_MAX_HEADER_SIZE);
  uint16_t rc = _client->write(buffer + (MQTT_MAX_HEADER_SIZE - hlen), length - (MQTT_MAX_HEADER_SIZE - hlen));
  lastOutActivity = millis();

  return rc == (length - (MQTT_MAX_HEADER_SIZE - hlen));
}

boolean PubSubClient::endPublish() 
{
  return true;
}

size_t PubSubClient::write(uint8_t data) 
{
  lastOutActivity = millis();
  return _client->write(data);
}

size_t PubSubClient::write(const uint8_t * buffer, size_t size) 
{
  lastOutActivity = millis();
  return _client->write(buffer, size);
}

size_t PubSubClient::buildHeader(uint8_t header, uint8_t * buf, uint16_t length) 
{
  uint16_t len  = length;
  uint8_t  llen = 0;
  uint8_t  pos  = 0;
  uint8_t  lenBuf[4];

  do {
    uint8_t digit = len & 0x7F;
    len >>= 7;

    if (len > 0) digit |= 0x80;
    
    lenBuf[pos++] = digit;
    llen++;
  } while(len > 0);

  buf[4 - llen] = header;
  for (int i = 0; i < llen; i++) {
    buf[MQTT_MAX_HEADER_SIZE - llen + i] = lenBuf[i];
  }

  return llen + 1; // Full header size is variable length bit plus the 1-byte fixed header
}

boolean PubSubClient::write(uint8_t header, uint8_t * buf, uint16_t length) 
{
  uint16_t rc;
  uint8_t  hlen = buildHeader(header, buf, length);

  #ifdef MQTT_MAX_TRANSFER_SIZE

    uint8_t * writeBuf = buf + (MQTT_MAX_HEADER_SIZE - hlen);
    uint16_t  bytesRemaining = length + hlen;  //Match the length type
    uint8_t   bytesToWrite;
    boolean   result = true;

    while((bytesRemaining > 0) && result) {
      bytesToWrite = (bytesRemaining > MQTT_MAX_TRANSFER_SIZE) ? MQTT_MAX_TRANSFER_SIZE : bytesRemaining;
      rc = _client->write(writeBuf, bytesToWrite);
      result = (rc == bytesToWrite);
      bytesRemaining -= rc;
      writeBuf += rc;
    }

    return result;

  #else

    rc = _client->write(buf + (MQTT_MAX_HEADER_SIZE - hlen), length + hlen);
    lastOutActivity = millis();
    return rc == (hlen + length);

  #endif
}

boolean PubSubClient::subscribe(const char* topic) 
{
  return subscribe(topic, 0);
}

boolean PubSubClient::subscribe(const char* topic, uint8_t qos) 
{
  if (!connected()) return false;
  if (qos > 1) return false;
  if (MQTT_MAX_PACKET_SIZE < (9 + strlen(topic))) return false;

  // Leave room in the buffer for header and variable length field
  uint16_t length = MQTT_MAX_HEADER_SIZE;
  nextMsgId++;

  if (nextMsgId == 0) nextMsgId = 1;

  buffer[length++] = (nextMsgId >> 8);
  buffer[length++] = (nextMsgId & 0xFF);
  length = writeString((char*)topic, buffer, length);
  buffer[length++] = qos;

  return write(MQTTSUBSCRIBE | MQTTQOS1, buffer, length - MQTT_MAX_HEADER_SIZE);
}

boolean PubSubClient::unsubscribe(const char * topic) 
{
  if (!connected()) return false;
  if (MQTT_MAX_PACKET_SIZE < (9 + strlen(topic))) return false;

  uint16_t length = MQTT_MAX_HEADER_SIZE;
  nextMsgId++;

  if (nextMsgId == 0) nextMsgId = 1;

  buffer[length++] = (nextMsgId >> 8);
  buffer[length++] = (nextMsgId & 0xFF);

  length = writeString(topic, buffer, length);

  return write(MQTTUNSUBSCRIBE | MQTTQOS1, buffer, length - MQTT_MAX_HEADER_SIZE);
}

void PubSubClient::disconnect() 
{
  buffer[0] = MQTTDISCONNECT;
  buffer[1] = 0;
  _client->write(buffer, 2);
  _state = MQTT_DISCONNECTED;
  _client->flush();
  _client->stop();
  lastInActivity = lastOutActivity = millis();
}

uint16_t PubSubClient::writeString(const char * string, uint8_t * buf, uint16_t pos) 
{
  const char * idp = string;
  uint16_t i = 0;
  pos += 2;

  while (*idp) {
    buf[pos++] = *idp++;
    i++;
  }

  buf[pos - i - 2] = (i >> 8);
  buf[pos - i - 1] = (i & 0xFF);

  return pos;
}

boolean PubSubClient::check_and_write(uint16_t * length, const char * string) 
{
  if ((*length + 2 + strlen(string)) > MQTT_MAX_PACKET_SIZE) {
    _client->stop();
    return false;
  }
  *length = writeString(string, buffer, *length);
  return true;
}

boolean PubSubClient::connected() 
{
  boolean rc;
  if (_client == nullptr ) {
    rc = false;
  } 
  else {
    rc = _client->connected();
    if (!rc) {
      if (_state == MQTT_CONNECTED) {
        _state = MQTT_CONNECTION_LOST;
        _client->flush();
        _client->stop();
      }
    }
  }

  return rc;
}

PubSubClient & PubSubClient::setServer(uint8_t * ip, uint16_t port) 
{
  IPAddress addr(ip[0], ip[1], ip[2], ip[3]);
  return setServer(addr, port);
}

PubSubClient & PubSubClient::setServer(IPAddress ip, uint16_t port) 
{
  _ip     = ip;
  _port   = port;
  _domain = nullptr;

  return *this;
}

PubSubClient & PubSubClient::setServer(const char * domain, uint16_t port) 
{
  _domain = domain;
  _port   = port;

  return *this;
}

PubSubClient & PubSubClient::setCallback(MQTT_CALLBACK_SIGNATURE(callback)) 
{
  _callback = callback;

  return *this;
}

PubSubClient & PubSubClient::setClient(Client & client)
{
  _client = &client;

  return *this;
}

PubSubClient & PubSubClient::setStream(Stream & stream)
{
  _stream = &stream;

  return *this;
}
void PubSubClient::removeStream() {
  _stream = nullptr;
}

int PubSubClient::state() 
{
  return _state;
}
