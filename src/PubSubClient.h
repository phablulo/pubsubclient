/*
 PubSubClient.h - A simple client for MQTT.
  Nick O'Leary
  http://knolleary.net
*/

#ifndef PubSubClient_h
#define PubSubClient_h

#include <Arduino.h>
#include "IPAddress.h"
#include "Client.h"
#include "Stream.h"

#define MQTT_VERSION_3_1      3
#define MQTT_VERSION_3_1_1    4

// MQTT_VERSION : Pick the version
//#define MQTT_VERSION MQTT_VERSION_3_1
#ifndef MQTT_VERSION
  #define MQTT_VERSION MQTT_VERSION_3_1_1
#endif

// MQTT_MAX_PACKET_SIZE : Maximum packet size
#ifndef MQTT_MAX_PACKET_SIZE
  #define MQTT_MAX_PACKET_SIZE 128
#endif

// MQTT_KEEPALIVE : keepAlive interval in Seconds
#ifndef MQTT_KEEPALIVE
  #define MQTT_KEEPALIVE 15
#endif

// MQTT_SOCKET_TIMEOUT: socket timeout interval in Seconds
#ifndef MQTT_SOCKET_TIMEOUT
  #define MQTT_SOCKET_TIMEOUT 15
#endif

// MQTT_MAX_TRANSFER_SIZE : limit how much data is passed to the network client
//  in each write call. Needed for the Arduino Wifi Shield. Leave undefined to
//  pass the entire MQTT packet in each write call.
//#define MQTT_MAX_TRANSFER_SIZE 80

// Possible values for client.state()
#define MQTT_CONNECTION_TIMEOUT     -4
#define MQTT_CONNECTION_LOST        -3
#define MQTT_CONNECT_FAILED         -2
#define MQTT_DISCONNECTED           -1
#define MQTT_CONNECTED               0
#define MQTT_CONNECT_BAD_PROTOCOL    1
#define MQTT_CONNECT_BAD_CLIENT_ID   2
#define MQTT_CONNECT_UNAVAILABLE     3
#define MQTT_CONNECT_BAD_CREDENTIALS 4
#define MQTT_CONNECT_UNAUTHORIZED    5

#define MQTTCONNECT      1 << 4 // Client request to connect to Server
#define MQTTCONNACK      2 << 4 // Connect Acknowledgment
#define MQTTPUBLISH      3 << 4 // Publish message
#define MQTTPUBACK       4 << 4 // Publish Acknowledgment
#define MQTTPUBREC       5 << 4 // Publish Received (assured delivery part 1)
#define MQTTPUBREL       6 << 4 // Publish Release (assured delivery part 2)
#define MQTTPUBCOMP      7 << 4 // Publish Complete (assured delivery part 3)
#define MQTTSUBSCRIBE    8 << 4 // Client Subscribe request
#define MQTTSUBACK       9 << 4 // Subscribe Acknowledgment
#define MQTTUNSUBSCRIBE 10 << 4 // Client Unsubscribe request
#define MQTTUNSUBACK    11 << 4 // Unsubscribe Acknowledgment
#define MQTTPINGREQ     12 << 4 // PING Request
#define MQTTPINGRESP    13 << 4 // PING Response
#define MQTTDISCONNECT  14 << 4 // Client is Disconnecting
#define MQTTReserved    15 << 4 // Reserved

#define MQTTQOS0        (0 << 1)
#define MQTTQOS1        (1 << 1)
#define MQTTQOS2        (2 << 1)

// Maximum size of fixed header and variable length size header
#define MQTT_MAX_HEADER_SIZE 5

#if defined(ESP8266) || defined(ESP32)
  #include <functional>
  #define MQTT_CALLBACK_SIGNATURE(c) std::function<void(char *, uint8_t *, unsigned int)> c
#else
  #define MQTT_CALLBACK_SIGNATURE(c) void (*c)(char *, uint8_t *, unsigned int)
#endif

class PubSubClient : public Print {
private:
  int           _state;
  Client      * _client;
  Stream      * _stream;
  MQTT_CALLBACK_SIGNATURE(_callback);
  
  IPAddress     _ip;
  const char  * _domain;
  uint16_t      _port;

  uint8_t       buffer[MQTT_MAX_PACKET_SIZE];
  uint16_t      nextMsgId;
  unsigned long lastOutActivity;
  unsigned long lastInActivity;
  bool          pingOutstanding;

  uint32_t     readPacket(uint8_t    * lengthLength);
  boolean        readByte(uint8_t    * result);
  boolean        readByte(uint8_t    * result, uint16_t   * index);
  boolean           write(uint8_t      header, uint8_t    * buf, uint16_t length);
  uint16_t    writeString(const char * string, uint8_t    * buf, uint16_t pos);
  boolean check_and_write(uint16_t   * length, const char * string);

  // Build up the header ready to send
  // Returns the size of the header
  // Note: the header is built at the end of the first MQTT_MAX_HEADER_SIZE bytes, so will start
  //       (MQTT_MAX_HEADER_SIZE - <returned size>) bytes into the buffer
  size_t buildHeader(uint8_t header, uint8_t * buf, uint16_t length);

public:
  PubSubClient();
  PubSubClient(Client & client);
  PubSubClient(IPAddress, uint16_t, Client & client);
  PubSubClient(IPAddress, uint16_t, Client & client, Stream & stream);
  PubSubClient(IPAddress, uint16_t, MQTT_CALLBACK_SIGNATURE(callback), Client & client);
  PubSubClient(IPAddress, uint16_t, MQTT_CALLBACK_SIGNATURE(callback), Client & client, Stream & stream);
  PubSubClient(uint8_t *, uint16_t, Client & client);
  PubSubClient(uint8_t *, uint16_t, Client & client, Stream & stream);
  PubSubClient(uint8_t *, uint16_t, MQTT_CALLBACK_SIGNATURE(callback), Client & client);
  PubSubClient(uint8_t *, uint16_t, MQTT_CALLBACK_SIGNATURE(callback), Client & client, Stream & stream);
  PubSubClient(const char *, uint16_t, Client & client);
  PubSubClient(const char *, uint16_t, Client & client, Stream & stream);
  PubSubClient(const char *, uint16_t, MQTT_CALLBACK_SIGNATURE(callback), Client & client);
  PubSubClient(const char *, uint16_t, MQTT_CALLBACK_SIGNATURE(callback), Client & client, Stream & stream);

  PubSubClient & setServer(IPAddress    ip,     uint16_t port);
  PubSubClient & setServer(uint8_t    * ip,     uint16_t port);
  PubSubClient & setServer(const char * domain, uint16_t port);

  PubSubClient & setCallback(MQTT_CALLBACK_SIGNATURE(callback));
    
  PubSubClient & setClient(Client & client);
   
  PubSubClient & setStream(Stream & stream);
  void removeStream();

  boolean connect(const char * id, 
                  const char * user         = nullptr, 
                  const char * pass         = nullptr, 
                  const char * willTopic    = nullptr, 
                  uint8_t      willQos      = 0, 
                  boolean      willRetain   = false, 
                  const char * willMessage  = nullptr, 
                  boolean      cleanSession = true);
  
  inline boolean connect(const char * id, 
                         const char * willTopic, 
                         uint8_t      willQos, 
                         boolean      willRetain, 
                         const char * willMessage) 
  {
    return connect(id, nullptr, nullptr, willTopic, willQos, willRetain, willMessage, true);
  }

  void disconnect();

  boolean publish(const char * topic, const uint8_t * payload, unsigned int plength, boolean retained = false);
  inline boolean publish(const char * topic, const char * payload, boolean retained = false)
  {
    return publish(topic, (const uint8_t *) payload, strlen(payload), retained);
  }

  boolean publish_P(const char * topic, const uint8_t * payload, unsigned int plength, boolean retained);
  inline boolean publish_P(const char * topic, const char * payload, boolean retained)
  {
    return publish_P(topic, (const uint8_t *) payload, strlen(payload), retained);
  }

  // Start to publish a message.
  // This API:
  //   beginPublish(...)
  //   one or more calls to write(...)
  //   endPublish()
  // Allows for arbitrarily large payloads to be sent without them having to be copied into
  // a new buffer and held in memory at one time
  // Returns 1 if the message was started successfully, 0 if there was an error
  boolean beginPublish(const char* topic, unsigned int plength, boolean retained);

  // Finish off this publish message (started with beginPublish)
  // Returns true if the packet was sent successfully, false if there was an error
  boolean endPublish();

  // Write a single byte of payload (only to be used with beginPublish/endPublish)
  virtual size_t write(uint8_t);

  // Write size bytes from buffer into the payload (only to be used with beginPublish/endPublish)
  // Returns the number of bytes written
  virtual size_t write(const uint8_t * buffer, size_t size);

  boolean subscribe(const char * topic);
  boolean subscribe(const char * topic, uint8_t qos);
  boolean unsubscribe(const char * topic);
  boolean loop();
  boolean connected();
  int state();
};


#endif
