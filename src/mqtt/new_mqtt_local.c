
#include "new_mqtt_local.h"
#include "../new_common.h"
#include "../new_pins.h"
#include "../new_cfg.h"
#include "../jsmn/jsmn_h.h"
#include "../logging/logging.h"
// Commands register, execution API and cmd tokenizer
#include "../cmnds/cmd_public.h"
#include "../hal/hal_wifi.h"
#include "../driver/drv_public.h"
#include "../driver/drv_ntp.h"
#include "../driver/drv_tuyaMCU.h"
#include "../hal/hal_ota.h"
#include "../cJSON/cJSON.h"
#include "../httpclient/http_client.h"
#include <inttypes.h>
#define MAX_JSON_VALUE_LENGTH   128
#ifndef LWIP_MQTT_EXAMPLE_IPADDR_INIT
#if LWIP_IPV4
#define LWIP_MQTT_EXAMPLE_IPADDR_INIT = IPADDR4_INIT(PP_HTONL(IPADDR_LOOPBACK))
#else
#define LWIP_MQTT_EXAMPLE_IPADDR_INIT
#endif
#endif

#ifdef PLATFORM_BEKEN
#include <tcpip.h>
// from hal_main_bk7231.c
// triggers a one-shot timer to cause read.
extern void MQTT_TriggerRead();
#endif

// these won't exist except on Beken?
#ifndef LOCK_TCPIP_CORE
#define LOCK_TCPIP_CORE()
#endif

#ifndef UNLOCK_TCPIP_CORE
#define UNLOCK_TCPIP_CORE()
#endif

//
// Variables for periodical self state broadcast
//
// current time left (counting down)
static int g_secondsBeforeNextFullBroadcast = 30;
// constant value, how much interval between self state broadcast (enabled by flag)
// You can change it with command: mqtt_broadcastInterval 60
static int g_intervalBetweenMQTTBroadcasts = 60;
// While doing self state broadcast, it limits the number of publishes 
// per second in order not to overload LWIP
static int g_maxBroadcastItemsPublishedPerSecond = 1;

/////////////////////////////////////////////////////////////
// mqtt receive buffer, so we can action in our threads, not
// in tcp_thread
//
#define CONTROL		1
#define SET_MQTT	2
#define SET_LINKS	3
#define GET_LINKS	4
#define SET_WIFI	5
#define UPDATE_SYS	6
#define GET_STATE 	1
#define GET_SYS		2

#define	SWITCH_LOCK			1
#define	DOOR_SENSOR			2
#define	GARAGE_CONTROLER	3
#define	TAMPER_SENSOR		4

#define MQTT_RX_BUFFER_MAX 4096
unsigned char mqtt_rx_buffer[MQTT_RX_BUFFER_MAX];
int mqtt_rx_buffer_head_local;
int mqtt_rx_buffer_tail_local;
int mqtt_rx_buffer_count_local;
unsigned char temp_topic[128];
unsigned char temp_data[2048];

int addLenData_local(int len, const unsigned char *data){
	mqtt_rx_buffer[mqtt_rx_buffer_head_local] = (len >> 8) & 0xff;
	mqtt_rx_buffer_head_local = (mqtt_rx_buffer_head_local + 1) % MQTT_RX_BUFFER_MAX;
	mqtt_rx_buffer_count_local++;
	mqtt_rx_buffer[mqtt_rx_buffer_head_local] = (len) & 0xff;
	mqtt_rx_buffer_head_local = (mqtt_rx_buffer_head_local + 1) % MQTT_RX_BUFFER_MAX;
	mqtt_rx_buffer_count_local++;
	for (int i = 0; i < len; i++){
		mqtt_rx_buffer[mqtt_rx_buffer_head_local] = data[i];
		mqtt_rx_buffer_head_local = (mqtt_rx_buffer_head_local + 1) % MQTT_RX_BUFFER_MAX;
		mqtt_rx_buffer_count_local++;
	}
	return len + 2;
}

int getLenData_local(int *len, unsigned char *data, int maxlen){
	int l;
	l = mqtt_rx_buffer[mqtt_rx_buffer_tail_local];
	mqtt_rx_buffer_tail_local = (mqtt_rx_buffer_tail_local + 1) % MQTT_RX_BUFFER_MAX;
	mqtt_rx_buffer_count_local--;
	l = l<<8;
	l |= mqtt_rx_buffer[mqtt_rx_buffer_tail_local];
	mqtt_rx_buffer_tail_local = (mqtt_rx_buffer_tail_local + 1) % MQTT_RX_BUFFER_MAX;
	mqtt_rx_buffer_count_local--;

	for (int i = 0; i < l; i++){
		if (i < maxlen){
			data[i] = mqtt_rx_buffer[mqtt_rx_buffer_tail_local];
		}
		mqtt_rx_buffer_tail_local = (mqtt_rx_buffer_tail_local + 1) % MQTT_RX_BUFFER_MAX;
		mqtt_rx_buffer_count_local--;
	}
	if (mqtt_rx_buffer_count_local < 0){
		// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "MQTT_rx buffer underflow!!!");
		mqtt_rx_buffer_count_local = 0;
		mqtt_rx_buffer_tail_local = mqtt_rx_buffer_head_local = 0;
	}

	if (l > maxlen){
		*len = maxlen;
	} else {
		*len = l;
	}
	return l + 2;
}

static SemaphoreHandle_t g_mutex_local = 0;

static bool MQTT_Mutex_Take(int del) {
	int taken;

	if (g_mutex_local == 0)
	{
		g_mutex_local = xSemaphoreCreateMutex();
	}
	taken = xSemaphoreTake(g_mutex_local, del);
	if (taken == pdTRUE) {
		return true;
	}
	return false;
}

static void MQTT_Mutex_Free()
{
	xSemaphoreGive(g_mutex_local);
}

// this is called from tcp_thread context to queue received mqtt,
// and then we'll retrieve them from our own thread for processing.
//
// NOTE: this function is now public, but only because my unit tests
// system can use it to spoof MQTT packets to check if MQTT commands
// are working...
int MQTT_Post_Received_local(const char *topic, int topiclen, const unsigned char *data, int datalen){
	MQTT_Mutex_Take(100);
	if ((MQTT_RX_BUFFER_MAX - 1 - mqtt_rx_buffer_count_local) < topiclen + datalen + 2 + 2){
		// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "MQTT_rx buffer overflow for topic %s", topic);
	} else {
		addLenData_local(topiclen, (unsigned char *)topic);
		addLenData_local(datalen, data);
	}
	MQTT_Mutex_Free();


#ifdef PLATFORM_BEKEN
	MQTT_TriggerRead();
#endif
	return 1;
}
int MQTT_Post_Received_Str_local(const char *topic, const char *data) {
	return MQTT_Post_Received_local(topic, strlen(topic), (const unsigned char*)data, strlen(data));
}
int get_received_local(char **topic, int *topiclen, unsigned char **data, int *datalen){
	int res = 0;
	MQTT_Mutex_Take(100);
	if (mqtt_rx_buffer_tail_local != mqtt_rx_buffer_head_local){
		getLenData_local(topiclen, temp_topic, sizeof(temp_topic)-1);
		temp_topic[*topiclen] = 0;
		getLenData_local(datalen, temp_data, sizeof(temp_data)-1);
		temp_data[*datalen] = 0;
		*topic = (char *)temp_topic;
		*data = temp_data;
		res = 1;
	}
	MQTT_Mutex_Free();
	return res;
}
//
//////////////////////////////////////////////////////////////////////

#define MQTT_QUEUE_ITEM_IS_REUSABLE(x)  (x->topic[0] == 0)
#define MQTT_QUEUE_ITEM_SET_REUSABLE(x) (x->topic[0] = 0)

MqttPublishItem_t* g_MqttPublishQueueHead_local = NULL;
int g_MqttPublishItemsQueued_local = 0;   //Items in the queue waiting to be published. This is not the queue length.

// from mqtt.c
extern void mqtt_disconnect(mqtt_client_t* client);

static int g_my_reconnect_mqtt_after_time = -1;
ip_addr_t mqtt_ip_local LWIP_MQTT_EXAMPLE_IPADDR_INIT;
mqtt_client_t* mqtt_client_local;
static int g_timeSinceLastMQTTPublish_local = 0;
static int mqtt_initialised_local = 0;
static int mqtt_connect_events = 0;
static int mqtt_connect_result = ERR_OK;
static char mqtt_status_message[256];
static int mqtt_published_events = 0;
static int mqtt_publish_errors = 0;
static int mqtt_received_events = 0;

static int g_just_connected = 0;


typedef struct mqtt_callback_tag {
	char* topic;
	char* subscriptionTopic;
	int ID;
	mqtt_callback_fn callback;
} mqtt_callback_t;

#define MAX_MQTT_CALLBACKS 3
static mqtt_callback_t* callbacks[MAX_MQTT_CALLBACKS];
static int numCallbacks = 0;
// note: only one incomming can be processed at a time.
static obk_mqtt_request_t g_mqtt_request;
static obk_mqtt_request_t g_mqtt_request_cb;

#define LOOPS_WITH_DISCONNECTED 15
int mqtt_loopsWithDisconnected_local = 0;
int mqtt_reconnect_local = 0;
// set for the device to broadcast self state on start
int g_bPublishAllStatesNow_local = 0;
int g_publishItemIndex_local = PUBLISHITEM_ALL_INDEX_FIRST;
static bool g_firstFullBroadcast = true;  //Flag indicating that we need to do a full broadcast

int g_memoryErrorsThisSession_local = 0;
int g_mqtt_bBaseTopicDirty_local = 0;

void MQTT_PublishWholeDeviceState_Internal_local(bool bAll)
{
	g_bPublishAllStatesNow_local = 1;
	if (bAll) {
		g_publishItemIndex_local = PUBLISHITEM_ALL_INDEX_FIRST;
	}
	else {
		g_publishItemIndex_local = PUBLISHITEM_DYNAMIC_INDEX_FIRST;
	}
}

void MQTT_PublishWholeDeviceState_local()
{
	//Publish all status items once. Publish only dynamic items after that.
	MQTT_PublishWholeDeviceState_Internal_local(g_firstFullBroadcast);
}

void MQTT_PublishOnlyDeviceChannelsIfPossible_local()
{
	if (g_bPublishAllStatesNow_local == 1)
		return;
	g_bPublishAllStatesNow_local = 1;
	//Start with light channels
	g_publishItemIndex_local = PUBLISHITEM_SELF_DYNAMIC_LIGHTSTATE;
}

static struct mqtt_connect_client_info_t mqtt_client_info =
{
  "test",
  // do not fil those settings, they are overriden when read from memory
  "user", /* user */
  "pass", /* pass */
  100,  /* keep alive */
  NULL, /* will_topic */
  NULL, /* will_msg */
  0,    /* will_qos */
  0     /* will_retain */
#if LWIP_ALTCP && LWIP_ALTCP_TLS
  , NULL
#endif
};

// channel set callback
int channelSet_local(obk_mqtt_request_t* request);
int channelGet_local(obk_mqtt_request_t* request);
static void MQTT_do_connect(mqtt_client_t* client);
static void mqtt_connection_cb(mqtt_client_t* client, void* arg, mqtt_connection_status_t status);

int MQTT_GetConnectEvents_local(void)
{
	return mqtt_connect_events;
}

int MQTT_GetPublishEventCounter_local(void)
{
	return mqtt_published_events;
}

int MQTT_GetPublishErrorCounter_local(void)
{
	return mqtt_publish_errors;
}

int MQTT_GetReceivedEventCounter_local(void)
{
	return mqtt_received_events;
}

int MQTT_GetConnectResult_local(void)
{
	return mqtt_connect_result;
}

//Based on mqtt_connection_status_t and https://www.nongnu.org/lwip/2_1_x/group__mqtt.html
const char* get_callback_error_local(int reason) {
	switch (reason)
	{
	case MQTT_CONNECT_REFUSED_PROTOCOL_VERSION: return "Refused protocol version";
	case MQTT_CONNECT_REFUSED_IDENTIFIER: return "Refused identifier";
	case MQTT_CONNECT_REFUSED_SERVER: return "Refused server";
	case MQTT_CONNECT_REFUSED_USERNAME_PASS: return "Refused user credentials";
	case MQTT_CONNECT_REFUSED_NOT_AUTHORIZED_: return "Refused not authorized";
	case MQTT_CONNECT_DISCONNECTED: return "Disconnected";
	case MQTT_CONNECT_TIMEOUT: return "Timeout";
	}
	return "";
}

const char* get_error_name_local(int err)
{
	switch (err)
	{
	case ERR_OK: return "ERR_OK";
	case ERR_MEM: return "ERR_MEM";
		/** Buffer error.            */
	case ERR_BUF: return "ERR_BUF";
		/** Timeout.                 */
	case ERR_TIMEOUT: return "ERR_TIMEOUT";
		/** Routing problem.         */
	case ERR_RTE: return "ERR_RTE";
		/** Operation in progress    */
	case ERR_INPROGRESS: return "ERR_INPROGRESS";
		/** Illegal value.           */
	case ERR_VAL: return "ERR_VAL";
		/** Operation would block.   */
	case ERR_WOULDBLOCK: return "ERR_WOULDBLOCK";
		/** Address in use.          */
	case ERR_USE: return "ERR_USE";
#if defined(ERR_ALREADY)
		/** Already connecting.      */
	case ERR_ALREADY: return "ERR_ALREADY";
#endif
		/** Conn already established.*/
	case ERR_ISCONN: return "ERR_ISCONN";
		/** Not connected.           */
	case ERR_CONN: return "ERR_CONN";
		/** Low-level netif error    */
	case ERR_IF: return "ERR_IF";
		/** Connection aborted.      */
	case ERR_ABRT: return "ERR_ABRT";
		/** Connection reset.        */
	case ERR_RST: return "ERR_RST";
		/** Connection closed.       */
	case ERR_CLSD: return "ERR_CLSD";
		/** Illegal argument.        */
	case ERR_ARG: return "ERR_ARG";
	}
	return "";
}

char* MQTT_GetStatusMessage_local(void)
{
	return mqtt_status_message;
}

void MQTT_ClearCallbacks_local() {
	int i;
	for (i = 0; i < MAX_MQTT_CALLBACKS; i++) {
		if (callbacks[i]) {
			free(callbacks[i]->topic);
			free(callbacks[i]->subscriptionTopic);
			free(callbacks[i]);
			callbacks[i] = 0;
		}
	}
}
// this can REPLACE callbacks, since we MAY wish to change the root topic....
// in which case we would re-resigster all callbacks?
int MQTT_RegisterCallback_local(const char* basetopic, const char* subscriptiontopic, int ID, mqtt_callback_fn callback) {
	int index;
	int i;
	int subscribechange = 0;
	if (!basetopic || !subscriptiontopic || !callback) {
		return -1;
	}
	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT_RegisterCallback_local called for bT %s subT %s", basetopic, subscriptiontopic);

	// find existing to replace
	for (index = 0; index < numCallbacks; index++) {
		if (callbacks[index]) {
			if (callbacks[index]->ID == ID) {
				break;
			}
		}
	}

	// find empty if any (empty by MQTT_RemoveCallback_local)
	if (index == numCallbacks) {
		for (index = 0; index < numCallbacks; index++) {
			if (!callbacks[index]) {
				break;
			}
		}
	}

	if (index >= MAX_MQTT_CALLBACKS) {
		return -4;
	}
	if (!callbacks[index]) {
		callbacks[index] = (mqtt_callback_t*)os_malloc(sizeof(mqtt_callback_t));
		if (callbacks[index] != 0) {
			memset(callbacks[index], 0, sizeof(mqtt_callback_t));
		}
	}
	if (!callbacks[index]) {
		return -2;
	}
	if (!callbacks[index]->topic || strcmp(callbacks[index]->topic, basetopic)) {
		if (callbacks[index]->topic) {
			os_free(callbacks[index]->topic);
		}
		callbacks[index]->topic = (char*)os_malloc(strlen(basetopic) + 1);
		if (!callbacks[index]->topic) {
			os_free(callbacks[index]);
			return -3;
		}
		strcpy(callbacks[index]->topic, basetopic);
	}

	if (!callbacks[index]->subscriptionTopic || strcmp(callbacks[index]->subscriptionTopic, subscriptiontopic)) {
		if (callbacks[index]->subscriptionTopic) {
			os_free(callbacks[index]->subscriptionTopic);
		}
		callbacks[index]->subscriptionTopic = (char*)os_malloc(strlen(subscriptiontopic) + 1);
		callbacks[index]->subscriptionTopic[0] = '\0';
		if (!callbacks[index]->subscriptionTopic) {
			os_free(callbacks[index]->topic);
			os_free(callbacks[index]);
			return -3;
		}

		// find out if this subscription is new.
		for (i = 0; i < numCallbacks; i++) {
			if (callbacks[i]) {
				if (callbacks[i]->subscriptionTopic &&
					!strcmp(callbacks[i]->subscriptionTopic, subscriptiontopic)) {
					break;
				}
			}
		}
		strcpy(callbacks[index]->subscriptionTopic, subscriptiontopic);
		// if this subscription is new, must reconnect
		if (i == numCallbacks) {
			subscribechange++;
		}
	}

	callbacks[index]->callback = callback;
	if (index == numCallbacks) {
		numCallbacks++;
	}

	if (subscribechange) {
		if (mqtt_client_local) {
			mqtt_reconnect_local = 8;
		}
	}
	// success
	return 0;
}

int MQTT_RemoveCallback_local(int ID) {
	int index;

	for (index = 0; index < numCallbacks; index++) {
		if (callbacks[index]) {
			if (callbacks[index]->ID == ID) {
				if (callbacks[index]->topic) {
					os_free(callbacks[index]->topic);
					callbacks[index]->topic = NULL;
				}
				if (callbacks[index]->subscriptionTopic) {
					os_free(callbacks[index]->subscriptionTopic);
					callbacks[index]->subscriptionTopic = NULL;
				}
				os_free(callbacks[index]);
				callbacks[index] = NULL;
				if (mqtt_client_local) {
					mqtt_reconnect_local = 8;
				}
				return 1;
			}
		}
	}
	return 0;
}

const char *skipExpected_local(const char *p, const char *tok) {
	while (1) {
		if (*p == 0)
			return 0;
		if (*p != *tok)
			return 0;
		p++;
		tok++;
		if (*tok == 0) {
			if (*p == '/') {
				p++;
				return p;
			}
			return 0;
		}
	}
	return p;
}
/** From a MQTT topic formatted <client>/<topic>, check if <client> is present
 *  and return <topic>.
 *
 *  @param topic	The topic to parse
 *  @return 		The topic without the client, or NULL if <client>/ wasn't present
 */
const char* MQTT_RemoveClientFromTopic_local(const char* topic, const char *prefix) {
	const char *p2;
	const char *p = topic;
	char pic[31];
	unsigned char mac[6];
	getMAC(mac);
    uint64_t num = 0;
    for (int index = 0; index < 6; index++) {
        num = (num << 8) | (byte)mac[index];
    }
	sprintf(pic, "%"PRIu64"/garage.1", num);

	if (prefix) {
		p = skipExpected_local(p, prefix);
		if (p == 0) {
			return 0;
		}
	}
	// it is either group topic or a device topic
	p2 = skipExpected_local(p,"garage.1");
	if (p2 == 0) {
		p2 = skipExpected_local(p, pic);
	}
	return p2;
}

// this accepts obkXXXXXX/<chan>/get to request channel publish
int channelGet_local(obk_mqtt_request_t* request) {
	return 1;
	//int len = request->receivedLen;
	int channel = 0;
	const char* p;

	// we only support here publishes with emtpy value, otherwise we would get into
	// a loop where we receive a get, and then send get reply with val, and receive our own get
	if (request->receivedLen) {
		return 1;
	}

	// addLogAdv(LOG_DEBUG, LOG_FEATURE_MQTT, "channelGet_local topic %i with arg %s", request->topic, request->received);

	p = MQTT_RemoveClientFromTopic_local(request->topic,0);

	if (p == NULL) {
		return 0;
	}

	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "channelGet_local part topic %s", p);

	if (!stricmp(p, "led_enableAll")) {
		LED_SendEnableAllState();
		return 1;
	}
	if (!stricmp(p, "led_dimmer")) {
		LED_SendDimmerChange();
		return 1;
	}
	if (!stricmp(p, "led_temperature")) {
		sendTemperatureChange();
		return 1;
	}
	if (!stricmp(p, "led_finalcolor_rgb")) {
		sendFinalColor();
		return 1;
	}
	if (!stricmp(p, "led_basecolor_rgb")) {
		sendColorChange();
		return 1;
	}

	// atoi won't parse any non-decimal chars, so it should skip over the rest of the topic.
	channel = atoi(p);

	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "channelGet_local channel %i", channel);

	// if channel out of range, stop here.
	if ((channel < 0) || (channel > 32)) {
		return 0;
	}

	MQTT_ChannelPublish_local(channel, 0);

	// return 1 to stop processing callbacks here.
	// return 0 to allow later callbacks to process this topic.
	return 1;
}
// this accepts obkXXXXXX/<chan>/set to receive data to set channels
int channelSet_local(obk_mqtt_request_t* request) {
	// MQTT_ReturnState_local();
	//int len = request->receivedLen;
	addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Channelset CAll");
	int channel = 0;
	int iValue = 0;
	int i;
	int r;
	char* pic;
	struct tm * ltm;
	unsigned int time_ntp;
	char* server[128];
	char* path[256];
	char tokenStrValue[MAX_JSON_VALUE_LENGTH + 1];
	int state_control = 0;
	time_ntp = g_ntpTime + 7 * 60 * 60;
	// addLogAdv(LOG_DEBUG, LOG_FEATURE_MQTT, "channelSet_local topic %i with arg %s", request->topic, request->received);

	pic = MQTT_RemoveClientFromTopic_local(request->topic,0);

	if (pic == NULL) {
		return 0;
	}

	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "channelSet_local part topic %s", pic);
	if (!(strcmp(pic, "set") == 0)) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "channelSet_local NOT 'set'");
		return 0;
	}
	jsmn_parser* p = os_malloc(sizeof(jsmn_parser));
#define TOKEN_COUNT 128
	jsmntok_t* t = os_malloc(sizeof(jsmntok_t) * TOKEN_COUNT);
	char* json_str = request->received;
	int json_len = strlen(json_str);

	memset(p, 0, sizeof(jsmn_parser));
	memset(t, 0, sizeof(jsmntok_t) * 128);

	jsmn_init(p);
	r = jsmn_parse(p, json_str, json_len, t, TOKEN_COUNT);
	if (r < 0) {
		ADDLOG_ERROR(LOG_FEATURE_API, "Failed to parse JSON: %d", r);
		// sprintf(tmp, "Failed to parse JSON: %d\n", r);
		os_free(p);
		os_free(t);
	}

	/* Assume the top-level element is an object */
	if (r < 1 || t[0].type != JSMN_OBJECT) {
		ADDLOG_ERROR(LOG_FEATURE_API, "Array expected", r);
		// sprintf(tmp, "Object expected");
		os_free(p);
		os_free(t);
	}

	/* Loop over all keys of the root object */
	int uid;
	for (i = 1; i < r; i++) {
		if (tryGetTokenString(json_str, &t[i], tokenStrValue) != true) {
			// ADDLOG_DEBUG(LOG_FEATURE_API, "Parsing failed");
			continue;
		}
		//ADDLOG_DEBUG(LOG_FEATURE_API, "parsed %s", tokenStrValue);
		addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT request: %s ", tokenStrValue);
		if (state_control == CONTROL) {
			if (strcmp(tokenStrValue,"data") == 0){
				if (tryGetTokenString(json_str, &t[i + 1], tokenStrValue) == true) {
					// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Control: %s ", tokenStrValue);
					if (strcmp(tokenStrValue,"open") == 0) {
						if(curtain_lock == false){
							CHANNEL_Set(OPEN,1,0);
						}
					}
					else if (strcmp(tokenStrValue,"close") == 0) {
						if(curtain_lock == false){
							CHANNEL_Set(CLOSE,1,0);
						}
					}
					else if (strcmp(tokenStrValue,"stop") == 0){
						if(curtain_lock == false){
							CHANNEL_Set(STOP,1,0);
						}
					}
					else if (strcmp(tokenStrValue,"lock") == 0){
						if(curtain_lock == false){
							CHANNEL_Set(LOCK,LOCK_STATE,0);
						}
						else
						{
							CHANNEL_Set(LOCK,UNLOCK_STATE,0);
						}
					}
				}
			}
			i += t[i + 1].size + 1;
		}
		else {
			// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Unexpected key: %.*s", t[i].end - t[i].start,
			// 	json_str + t[i].start);
		}
		if (strcmp(tokenStrValue, "type") == 0) {
			if (tryGetTokenString(json_str, &t[i + 1], tokenStrValue) == true) {
				if (!strcmp(tokenStrValue, "get_states")) {
					MQTT_ReturnState_local();
					os_free(p);
					os_free(t);
					return 1;
				}
				else if (!strcmp(tokenStrValue, "control")){
					state_control = CONTROL;
				}
			}
			i += t[i + 1].size + 1;
		}
	}
	os_free(p);
	os_free(t);
	return 1;
}
cJSON* build_device_node() {
	char text[32];
	cJSON* dev = cJSON_CreateObject();
	cJSON* ids = cJSON_CreateArray();
	sprintf(text, "mqtt_%s", CFG_GetMQTTNetIdLocal());
	cJSON_AddItemToArray(ids, cJSON_CreateString(text));
	
	cJSON_AddItemToObject(dev, "identifiers", ids);     //identifiers

	cJSON_AddStringToObject(dev, "manufacturer", "JAVIS");
	cJSON_AddStringToObject(dev, "model", "Garage controller (Module Wifi 3.0)");
	cJSON_AddStringToObject(dev, "name", CFG_GetMQTTNetIdLocal());
	return dev;
}

void sendDiscoveryHomeassistant(char* name) {
	
	cJSON* root;
	cJSON* dev;
	cJSON* arr;
	char *msg;
	char macstr[3 * 6 + 1];
	char text[64];
	root = cJSON_CreateObject();
	dev = cJSON_CreateObject();
	arr = cJSON_CreateArray();
// door sensor
	sprintf(text, "%s/connected", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(dev, "topic", text);
	cJSON_AddItemToArray(arr, dev);
	cJSON_AddItemToObject(root, "availability", arr);
	cJSON_AddItemToObject(root, "device", build_device_node());
	cJSON_AddStringToObject(root, "device_class", "door");
	sprintf(text, "Cảm biến %s mqtt", name);
	cJSON_AddStringToObject(root, "name", text);
	cJSON_AddStringToObject(root, "payload_off", "1");
	cJSON_AddStringToObject(root, "payload_on", "0");
	sprintf(text, "%s/garage.1/state", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(root, "state_topic", text);
	sprintf(text, "%s_contact_wifi_mqtt", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(root, "unique_id", text);
	sprintf(text, "jwgp_%s_garage_door_sensor", HAL_GetMACStrn(macstr));
	cJSON_AddStringToObject(root, "object_id", text);
	cJSON_AddStringToObject(root, "value_template", "{{ value_json.door_sensor }}");
	msg = cJSON_PrintUnformatted(root);
	MQTT_PublishTopicToHomeAssistant_local(mqtt_client_local, msg, DOOR_SENSOR);
// tamper
	cJSON_DeleteItemFromObject(root, "device_class");
	cJSON_DeleteItemFromObject(root, "name");
	cJSON_DeleteItemFromObject(root, "payload_off");
	cJSON_DeleteItemFromObject(root, "payload_on");
	cJSON_DeleteItemFromObject(root, "unique_id");
	cJSON_DeleteItemFromObject(root, "value_template");
	cJSON_DeleteItemFromObject(root, "object_id");

	sprintf(text, "jwgp_%s_garage_chongxo", HAL_GetMACStrn(macstr));
	cJSON_AddStringToObject(root, "object_id", text);

	cJSON_AddStringToObject(root, "device_class", "tamper");
	sprintf(text, "Chống xô %s mqtt", name);
	cJSON_AddStringToObject(root, "name", text);
	cJSON_AddStringToObject(root, "payload_off", "0");
	cJSON_AddStringToObject(root, "payload_on", "1");
	sprintf(text, "%s_tamper_wifi_mqtt", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(root, "unique_id", text);
	cJSON_AddStringToObject(root, "value_template", "{{ value_json.sensor }}");
	
	msg = cJSON_PrintUnformatted(root);
	MQTT_PublishTopicToHomeAssistant_local(mqtt_client_local, msg, TAMPER_SENSOR);
//lock
	cJSON_DeleteItemFromObject(root, "device_class");
	cJSON_DeleteItemFromObject(root, "name");
	cJSON_DeleteItemFromObject(root, "payload_off");
	cJSON_DeleteItemFromObject(root, "payload_on");
	cJSON_DeleteItemFromObject(root, "unique_id");
	cJSON_DeleteItemFromObject(root, "value_template");
	cJSON_DeleteItemFromObject(root, "object_id");

	sprintf(text, "jwgp_%s_garage_lock", HAL_GetMACStrn(macstr));
	cJSON_AddStringToObject(root, "object_id", text);

	sprintf(text, "%s/garage.1/set", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(root, "command_topic", text);
	sprintf(text, "Khóa %s mqtt", name);
	cJSON_AddStringToObject(root, "name", text);
	cJSON_AddStringToObject(root, "payload_off", "{\"type\":\"control\",\"data\":\"lock\"}");
	cJSON_AddStringToObject(root, "payload_on", "{\"type\":\"control\",\"data\":\"lock\"}");
	cJSON_AddStringToObject(root, "state_off", "0");
	cJSON_AddStringToObject(root, "state_on", "1");
	sprintf(text, "%s_lock_wifi_mqtt", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(root, "unique_id", text);
	cJSON_AddStringToObject(root, "value_template", "{{ value_json.lock }}");

	msg = cJSON_PrintUnformatted(root);
	MQTT_PublishTopicToHomeAssistant_local(mqtt_client_local, msg, SWITCH_LOCK);
//garage controler
	cJSON_DeleteItemFromObject(root, "device_class");
	cJSON_DeleteItemFromObject(root, "name");
	cJSON_DeleteItemFromObject(root, "payload_off");
	cJSON_DeleteItemFromObject(root, "payload_on");
	cJSON_DeleteItemFromObject(root, "state_off");
	cJSON_DeleteItemFromObject(root, "state_on");
	cJSON_DeleteItemFromObject(root, "unique_id");
	cJSON_DeleteItemFromObject(root, "value_template");
	cJSON_DeleteItemFromObject(root, "object_id");

	sprintf(text, "jwgp_%s_garage_controler", HAL_GetMACStrn(macstr));
	cJSON_AddStringToObject(root, "object_id", text);
	sprintf(text, "%s mqtt", name);
	cJSON_AddStringToObject(root, "name", text);
	cJSON_AddStringToObject(root, "device_class", "garage");
	// cJSON_AddStringToObject(root, "position_template", "{{ value_json.position }}");
	// sprintf(text, "%s/garage.1/state", CFG_GetMQTTNetIdLocal());
	// cJSON_AddStringToObject(root, "position_topic", text);
	// cJSON_AddStringToObject(root, "set_position_template","{ \"position\": {{ position }} }");
	// sprintf(text, "%s/garage.1/set", CFG_GetMQTTNetIdLocal());
	// cJSON_AddStringToObject(root, "set_position_topic", text);

	cJSON_AddStringToObject(root, "state_closed", "closed");
	cJSON_AddStringToObject(root, "state_open", "open");
	cJSON_AddStringToObject(root, "payload_open", "{\"type\":\"control\",\"data\":\"open\"}");
	cJSON_AddStringToObject(root, "payload_close", "{\"type\":\"control\",\"data\":\"close\"}");
	cJSON_AddStringToObject(root, "payload_stop", "{\"type\":\"control\",\"data\":\"stop\"}");
	sprintf(text, "%s_cover_wifi_mqtt", CFG_GetMQTTNetIdLocal());
	cJSON_AddStringToObject(root, "unique_id", text);
   	cJSON_AddStringToObject(root,"value_template", "{{ value_json.state }}");

	msg = cJSON_PrintUnformatted(root);
	MQTT_PublishTopicToHomeAssistant_local(mqtt_client_local, msg, GARAGE_CONTROLER);
	cJSON_Delete(root);
	cJSON_Delete(dev);
	cJSON_Delete(arr);
	os_free(msg);
}


// this accepts cmnd/<clientId>/<xxx> to execute any supported console command
// Example 1: 
// Topic: cmnd/obk8C112233/power
// Payload: toggle
// this will toggle power
// Example 2:
// Topic: cmnd/obk8C112233/backlog
// Payload: echo Test1; power toggle; echo Test2
// will do echo, toggle power and do ecoh
//


void MQTT_PublishPrinterContentsToStat_local(obk_mqtt_publishReplyPrinter_t *printer, const char *statName) {
	const char *toUse;
	if (printer->allocated)
		toUse = printer->allocated;
	else
		toUse = printer->stackBuffer;
	MQTT_PublishStat_local(statName, toUse);
}
void MQTT_PublishPrinterContentsToTele_local(obk_mqtt_publishReplyPrinter_t *printer, const char *statName) {
	const char *toUse;
	if (printer->allocated)
		toUse = printer->allocated;
	else
		toUse = printer->stackBuffer;
	MQTT_PublishTele_local(statName, toUse);
}
int mqtt_printf255_local(obk_mqtt_publishReplyPrinter_t* request, const char* fmt, ...) {
	va_list argList;
	char tmp[256];
	int myLen;

	memset(tmp, 0, sizeof(tmp));
	va_start(argList, fmt);
	vsnprintf(tmp, 255, fmt, argList);
	va_end(argList);

	myLen = strlen(tmp);

	if (request->curLen + (myLen + 2) >= MQTT_STACK_BUFFER_SIZE) {
		if (request->curLen + (myLen + 2) >= MQTT_TOTAL_BUFFER_SIZE) {
			// TODO: realloc
			return 0;
		}
		// init alloced if needed
		if (request->allocated == 0) {
			request->allocated = malloc(MQTT_TOTAL_BUFFER_SIZE);
			strcpy(request->allocated, request->stackBuffer);
		}
		strcat(request->allocated, tmp);
	}
	else {
		strcat(request->stackBuffer, tmp);
	}
	request->curLen += myLen;
	return 0;
}
void MQTT_ProcessCommandReplyJSON_local(const char *cmd, const char *args, int flags) {
	obk_mqtt_publishReplyPrinter_t replyBuilder;
	memset(&replyBuilder, 0, sizeof(obk_mqtt_publishReplyPrinter_t));
	JSON_ProcessCommandReply(cmd, args, &replyBuilder, (jsonCb_t)mqtt_printf255_local, flags);
	if (replyBuilder.allocated != 0) {
		free(replyBuilder.allocated);
	}
}
int tasCmnd_local(obk_mqtt_request_t* request) {
	return 1;
	const char *p, *args;
    //const char *p2;

	p = MQTT_RemoveClientFromTopic_local(request->topic, "cmnd");
	if (p == 0) {
		p = MQTT_RemoveClientFromTopic_local(request->topic, "tele");
		if (p == 0) {
			p = MQTT_RemoveClientFromTopic_local(request->topic, "stat");
			if (p == 0) {

			}
		}
	}
	if (p == 0)
		return 1;

#if 1
	args = (const char *)request->received;
	// I think that our function get_received_local always ensured that
	// there is a NULL terminating character after payload of MQTT
	// So we can feed it directly as command
	CMD_ExecuteCommandArgs(p, args, COMMAND_FLAG_SOURCE_MQTT);
	MQTT_ProcessCommandReplyJSON_local(p, args, COMMAND_FLAG_SOURCE_MQTT);
#else
	int len = request->receivedLen;
	char copy[64];
	char *allocated;
	// assume a string input here, copy and terminate
	// Try to avoid free/malloc
	if (len > sizeof(copy) - 2) {
		allocated = (char*)malloc(len + 1);
		if (allocated) {
			strncpy(allocated, (char*)request->received, len);
			// strncpy does not terminate??!!!!
			allocated[len] = '\0';
		}
		// use command executor....
		CMD_ExecuteCommandArgs(p, allocated, COMMAND_FLAG_SOURCE_MQTT);
		if (allocated) {
			free(allocated);
		}
	}
	else {
		strncpy(copy, (char*)request->received, len);
		// strncpy does not terminate??!!!!
		copy[len] = '\0';
		// use command executor....
		CMD_ExecuteCommandArgs(p, copy, COMMAND_FLAG_SOURCE_MQTT);
	}
#endif
	// return 1 to stop processing callbacks here.
	// return 0 to allow later callbacks to process this topic.
	return 1;
}

//void MQTT_GetStats(int *outUsed, int *outMax, int *outFreeMem) {
//  mqtt_get_request_stats(mqtt_client_local,outUsed, outMax,outFreeMem);
//}

// copied here because for some reason renames in sdk?
static void MQTT_disconnect(mqtt_client_t* client)
{
	if (!client)
		return;
	// this is what it was renamed to.  why?
	LOCK_TCPIP_CORE();
	mqtt_disconnect(client);
	UNLOCK_TCPIP_CORE();

}

/* Called when publish is complete either with sucess or failure */
static void mqtt_pub_request_cb(void* arg, err_t result)
{
	if (result != ERR_OK)
	{
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Publish result: %d(%s)\n", result, get_error_name_local(result));
		mqtt_publish_errors++;
	}
}

// This publishes value to the specified topic/channel.
static OBK_Publish_Result MQTT_PublishTopicToClient(mqtt_client_t* client, const char* sTopic, const char* sChannel, const char* sVal, int flags, bool appendGet)
{
	err_t err;
	u8_t qos = 0; /* 0 1 or 2, see MQTT specification */
	u8_t retain = 0; /* No don't retain such crappy payload... */
	size_t sVal_len;
	char* pub_topic;
	char* pub_data;
	unsigned char mac[6];
	getMAC(mac);
    uint64_t num = 0;
    for (int index = 0; index < 6; index++) {
        num = (num << 8) | (byte)mac[index];
    }
	if (client == 0)
		return OBK_PUBLISH_WAS_DISCONNECTED;

	if (flags & OBK_PUBLISH_FLAG_MUTEX_SILENT)
	{
		if (MQTT_Mutex_Take(100) == 0)
		{
			return OBK_PUBLISH_MUTEX_FAIL;
		}
	}
	else {
		if (MQTT_Mutex_Take(500) == 0)
		{
			// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "MQTT_PublishTopicToClient: mutex failed for %s=%s\r\n", sChannel, sVal);
			return OBK_PUBLISH_MUTEX_FAIL;
		}
	}
	if (flags & OBK_PUBLISH_FLAG_RETAIN)
	{
		retain = 1;
	}
	// global tool
	if (CFG_HasFlag(OBK_FLAG_MQTT_ALWAYSSETRETAIN))
	{
		retain = 0;
	}
	if (flags & OBK_PUBLISH_FLAG_FORCE_REMOVE_GET)
	{
		appendGet = false;
	}


	LOCK_TCPIP_CORE();
	int res = mqtt_client_is_connected(client);
	UNLOCK_TCPIP_CORE();

	if (res == 0)
	{
		g_my_reconnect_mqtt_after_time = 5;
		MQTT_Mutex_Free();
		return OBK_PUBLISH_WAS_DISCONNECTED;
	}

	g_timeSinceLastMQTTPublish_local = 0;

	pub_topic = (char*)os_malloc(31); //5 for /get
	if ((pub_topic != NULL) && (sVal != NULL))
	{
		sVal_len = strlen(sVal);
		// sprintf(pub_topic, "%s%s", sTopic, (appendGet == true ? "/mqtt" : ""));
		sprintf(pub_topic, "%"PRIu64"/garage.1/state", num);
		if (sVal_len < 128)
		{
			// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Publishing val %s to %s retain=%i\n", sVal, pub_topic, retain);
		}
		else {
			// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Publishing val (%d bytes) to %s retain=%i\n", sVal_len, pub_topic, retain);
		}
		LOCK_TCPIP_CORE();
		if (appendGet == true) {
			pub_data = (char*)os_malloc(strlen("[{\"id\":\"switch.\",\"state\":\"\"}]") + strlen(sChannel) + (strcmp(sVal,"0") == 0  ? 3 : 2) + 1);
			sprintf(pub_data, "[{\"id\":\"switch.%s\",\"state\":\"%s\"}]", sChannel, (strcmp(sVal,"0") == 0 ? "off" : "on"));
			err = mqtt_publish(client, pub_topic, pub_data, strlen(pub_data), qos, retain, mqtt_pub_request_cb, 0);
		} else {
			err = mqtt_publish(client, pub_topic, sVal, strlen(sVal), qos, retain, mqtt_pub_request_cb, 0);
		}
		UNLOCK_TCPIP_CORE();
		os_free(pub_topic);
		os_free(pub_data);

		if (err != ERR_OK)
		{
			if (err == ERR_CONN)
			{
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: ERR_CONN aka %d\n", err);
			}
			else if (err == ERR_MEM) {
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: ERR_MEM aka %d\n", err);
				g_memoryErrorsThisSession_local++;
			}
			else {
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: %d\n", err);
			}
			mqtt_publish_errors++;
			MQTT_Mutex_Free();
			return OBK_PUBLISH_MEM_FAIL;
		}
		mqtt_published_events++;
		MQTT_Mutex_Free();
		return OBK_PUBLISH_OK;
	}
	else {
		MQTT_Mutex_Free();
		return OBK_PUBLISH_MEM_FAIL;
	}
}
static OBK_Publish_Result MQTT_PublishTopicToClientData_local(mqtt_client_t* client, const char* sVal)
{	
	int flags = 0;
	bool appendGet = true;
	err_t err;
	u8_t qos = 0; /* 0 1 or 2, see MQTT specification */
	u8_t retain = 0; /* No don't retain such crappy payload... */
	size_t sVal_len;
	char* pub_topic;
	unsigned char mac[6];
	getMAC(mac);
    uint64_t num = 0;
    for (int index = 0; index < 6; index++) {
        num = (num << 8) | (byte)mac[index];
    }
	if (client == 0)
		return OBK_PUBLISH_WAS_DISCONNECTED;

	if (flags & OBK_PUBLISH_FLAG_MUTEX_SILENT)
	{
		if (MQTT_Mutex_Take(100) == 0)
		{
			return OBK_PUBLISH_MUTEX_FAIL;
		}
	}
	else {
		if (MQTT_Mutex_Take(500) == 0)
		{
			return OBK_PUBLISH_MUTEX_FAIL;
		}
	}

	LOCK_TCPIP_CORE();
	int res = mqtt_client_is_connected(client);
	UNLOCK_TCPIP_CORE();

	if (res == 0)
	{
		g_my_reconnect_mqtt_after_time = 5;
		MQTT_Mutex_Free();
		return OBK_PUBLISH_WAS_DISCONNECTED;
	}

	g_timeSinceLastMQTTPublish_local = 0;

	pub_topic = (char*)os_malloc(15+16); //5 for /get
	if ((pub_topic != NULL))
	{
		sprintf(pub_topic, "%"PRIu64"/garage.1/state", num);
		LOCK_TCPIP_CORE();
		err = mqtt_publish(client, pub_topic, sVal, strlen(sVal), qos, retain, mqtt_pub_request_cb, 0);
		UNLOCK_TCPIP_CORE();
		os_free(pub_topic);

		if (err != ERR_OK)
		{
			if (err == ERR_CONN)
			{
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: ERR_CONN aka %d\n", err);
			}
			else if (err == ERR_MEM) {
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: ERR_MEM aka %d\n", err);
				g_memoryErrorsThisSession_local++;
			}
			else {
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: %d\n", err);
			}
			mqtt_publish_errors++;
			MQTT_Mutex_Free();
			return OBK_PUBLISH_MEM_FAIL;
		}
		mqtt_published_events++;
		MQTT_Mutex_Free();
		return OBK_PUBLISH_OK;
	}
	else {
		MQTT_Mutex_Free();
		return OBK_PUBLISH_MEM_FAIL;
	}
}
OBK_Publish_Result MQTT_PublishTopicToHomeAssistant_local(mqtt_client_t* client, const char* sVal, int type)
{	
	int flags = 0;
	bool appendGet = true;
	err_t err;
	u8_t qos = 0; /* 0 1 or 2, see MQTT specification */
	u8_t retain = 1; /* No don't retain such crappy payload... */
	size_t sVal_len;
	char* pub_topic;
	unsigned char mac[6];
	getMAC(mac);
    uint64_t num = 0;
    for (int index = 0; index < 6; index++) {
        num = (num << 8) | (byte)mac[index];
    }
	if (client == 0)
		return OBK_PUBLISH_WAS_DISCONNECTED;

	if (flags & OBK_PUBLISH_FLAG_MUTEX_SILENT)
	{
		if (MQTT_Mutex_Take(100) == 0)
		{
			return OBK_PUBLISH_MUTEX_FAIL;
		}
	}
	else {
		if (MQTT_Mutex_Take(500) == 0)
		{
			return OBK_PUBLISH_MUTEX_FAIL;
		}
	}

	LOCK_TCPIP_CORE();
	int res = mqtt_client_is_connected(client);
	UNLOCK_TCPIP_CORE();

	if (res == 0)
	{
		g_my_reconnect_mqtt_after_time = 5;
		MQTT_Mutex_Free();
		return OBK_PUBLISH_WAS_DISCONNECTED;
	}

	g_timeSinceLastMQTTPublish_local = 0;

	pub_topic = (char*)os_malloc(64); //5 for /get
	if ((pub_topic != NULL))
	{
		switch (type) {
			case SWITCH_LOCK:
				sprintf(pub_topic, "homeassistant/switch/%"PRIu64"/switch/config", num);
				break;
			case DOOR_SENSOR:
				sprintf(pub_topic, "homeassistant/binary_sensor/%"PRIu64"/contact/config", num);
				break;
			case GARAGE_CONTROLER:
				sprintf(pub_topic, "homeassistant/cover/%"PRIu64"/cover/config", num);
				break;
			case TAMPER_SENSOR:
				sprintf(pub_topic, "homeassistant/binary_sensor/%"PRIu64"/tamper/config", num);
				break;	
			default:
				sprintf(pub_topic, "%"PRIu64"/garage.1/state", num);
				break;
		}
		
		LOCK_TCPIP_CORE();
		err = mqtt_publish(client, pub_topic, sVal, strlen(sVal), qos, retain, mqtt_pub_request_cb, 0);
		UNLOCK_TCPIP_CORE();
		os_free(pub_topic);

		if (err != ERR_OK)
		{
			if (err == ERR_CONN)
			{
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: ERR_CONN aka %d\n", err);
			}
			else if (err == ERR_MEM) {
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: ERR_MEM aka %d\n", err);
				g_memoryErrorsThisSession_local++;
			}
			else {
				// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: %d\n", err);
			}
			mqtt_publish_errors++;
			MQTT_Mutex_Free();
			return OBK_PUBLISH_MEM_FAIL;
		}
		mqtt_published_events++;
		MQTT_Mutex_Free();
		return OBK_PUBLISH_OK;
	}
	else {
		MQTT_Mutex_Free();
		return OBK_PUBLISH_MEM_FAIL;
	}
}
static OBK_Publish_Result MQTT_PublishMain(mqtt_client_t* client, const char* sChannel, const char* sVal, int flags, bool appendGet)
{
	return MQTT_PublishTopicToClient(mqtt_client_local, CFG_GetMQTTGroupTopic(), sChannel, sVal, flags, appendGet);
}

OBK_Publish_Result MQTT_PublishTele_local(const char* teleName, const char* teleValue)
{
	char topic[64];
	snprintf(topic, sizeof(topic), "tele/%s", CFG_GetMQTTClientId());
	return MQTT_PublishTopicToClient(mqtt_client_local, topic, teleName, teleValue, 0, false);
}
OBK_Publish_Result MQTT_ReturnState_local()
{
	cJSON *json = cJSON_CreateObject();
	if(CFG_GetEnableSensor() == 1){
		if(door_sensor == 0){
			garage_state = 1;
			curtain_position = 100;
		}
		else{
			garage_state = 0;
			curtain_position = 0;
		}
	} 
	else {
		curtain_position = 100 - CHANNEL_Get(CLOSE_PERCENT);
	}
    cJSON_AddStringToObject(json, "id", "garage.1");
    cJSON_AddStringToObject(json, "state", garage_state == 1 ? "open" : "closed");
    cJSON_AddNumberToObject(json, "position", curtain_position);
    cJSON_AddNumberToObject(json, "door_sensor", (CHANNEL_Get(DOOR_SENS) ? 0 : 1));
    cJSON_AddNumberToObject(json, "lock", CHANNEL_Get(LOCK));
    cJSON_AddNumberToObject(json, "sensor", CHANNEL_Get(SAFETY_SENS) ? 0 : 1);

	 char *dataStr = cJSON_PrintUnformatted(json);
	cJSON_Delete(json);
	MQTT_PublishTopicToClientData_local(mqtt_client_local, dataStr);
	os_free(dataStr);
	return 0;
}

OBK_Publish_Result MQTT_PublishStat_local(const char* statName, const char* statValue)
{
	char topic[64];
	snprintf(topic,sizeof(topic),"stat/%s", CFG_GetMQTTClientId());
	return MQTT_PublishTopicToClient(mqtt_client_local, topic, statName, statValue, 0, false);
}
/// @brief Publish a MQTT message immediately.
/// @param sTopic 
/// @param sChannel 
/// @param sVal 
/// @param flags
/// @return 
OBK_Publish_Result MQTT_Publish_local(const char* sTopic, const char* sChannel, const char* sVal, int flags)
{
	return MQTT_PublishTopicToClient(mqtt_client_local, sTopic, sChannel, sVal, flags, false);
}


////////////////////////////////////////
// called from tcp_thread context.
// we should do callbacks from one of our threads?
static void mqtt_incoming_data_cb(void* arg, const u8_t* data, u16_t len, u8_t flags)
{
	int i;
	// unused - left here as example
	//const struct mqtt_connect_client_info_t* client_info = (const struct mqtt_connect_client_info_t*)arg;

	// if we stored a topic in g_mqtt_request, then we found a matching callback, so use it.
	if (g_mqtt_request.topic[0])
	{
		// note: data is NOT terminated (it may be binary...).
		g_mqtt_request.received = data;
		g_mqtt_request.receivedLen = len;

		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT in topic %s", g_mqtt_request.topic);
		mqtt_received_events++;

		for (i = 0; i < numCallbacks; i++)
		{
			if (callbacks[i] == 0)
				continue;
			char* cbtopic = callbacks[i]->topic;
			if (!strncmp(g_mqtt_request.topic, cbtopic, strlen(cbtopic)))
			{
				MQTT_Post_Received_local(g_mqtt_request.topic, strlen(g_mqtt_request.topic), data, len);
				// if ANYONE is interested, store it.
				break;
				// note - callback must return 1 to say it ate the mqtt, else further processing can be performed.
				// i.e. multiple people can get each topic if required.
				if (callbacks[i]->callback(&g_mqtt_request))
				{
					return;
				}
			}
		}
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT topic not handled: %s", g_mqtt_request.topic);
	}
}


// run from userland (quicktick or wakeable thread)
int MQTT_process_received_local(){
	char *topic;
	int topiclen;
	unsigned char *data;
	int datalen;
	int found = 0;
	int count = 0;
	do{
		found = get_received_local(&topic, &topiclen, &data, &datalen);
		if (found){
			count++;
			strncpy(g_mqtt_request_cb.topic, topic, sizeof(g_mqtt_request_cb.topic));
			g_mqtt_request_cb.received = data;
			g_mqtt_request_cb.receivedLen = datalen;
			for (int i = 0; i < numCallbacks; i++)
			{
				char* cbtopic = callbacks[i]->topic;
				if (!strncmp(topic, cbtopic, strlen(cbtopic)))
				{
					// note - callback must return 1 to say it ate the mqtt, else further processing can be performed.
					// i.e. multiple people can get each topic if required.
					if (callbacks[i]->callback(&g_mqtt_request_cb))
					{
						// if no further processing, then break this loop.
						break;
					}
				}
			}
		}
	} while (found);

	return count;
}


////////////////////////////////
// called from tcp_thread context
static void mqtt_incoming_publish_cb(void* arg, const char* topic, u32_t tot_len)
{
	//const char *p;
	int i;
	// unused - left here as example
	//const struct mqtt_connect_client_info_t* client_info = (const struct mqtt_connect_client_info_t*)arg;

	// look for a callback with this URL and method, or HTTP_ANY
	g_mqtt_request.topic[0] = '\0';
	for (i = 0; i < numCallbacks; i++)
	{
		char* cbtopic = callbacks[i]->topic;
		if (strncmp(topic, cbtopic, strlen(cbtopic)))
		{
			strncpy(g_mqtt_request.topic, topic, sizeof(g_mqtt_request.topic) - 1);
			g_mqtt_request.topic[sizeof(g_mqtt_request.topic) - 1] = 0;
			break;
		}
	}
	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT client in mqtt_incoming_publish_cb topic %s\n", topic);
}

static void mqtt_request_cb_local(void* arg, err_t err)
{
	const struct mqtt_connect_client_info_t* client_info = (const struct mqtt_connect_client_info_t*)arg;
	if (err != 0) {
		// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "MQTT client \"%s\" request cb: err %d\n", client_info->client_id, (int)err);
	}
}

/////////////////////////////////////////////
// should be called in tcp_thread context.
static void mqtt_connection_cb(mqtt_client_t* client, void* arg, mqtt_connection_status_t status)
{
	int i;
	char tmp[CGF_MQTT_CLIENT_ID_SIZE + 16];
	const char* clientId;
	err_t err = ERR_OK;
	const struct mqtt_connect_client_info_t* client_info = (const struct mqtt_connect_client_info_t*)arg;
	LWIP_UNUSED_ARG(client);

	//   addLogAdv(LOG_INFO,LOG_FEATURE_MQTT,"MQTT client < removed name > connection cb: status %d\n",  (int)status);
	 //  addLogAdv(LOG_INFO,LOG_FEATURE_MQTT,"MQTT client \"%s\" connection cb: status %d\n", client_info->client_id, (int)status);

	if (status == MQTT_CONNECT_ACCEPTED)
	{
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_connection_cb: Successfully connected\n");

		// LOCK_TCPIP_CORE();
		mqtt_set_inpub_callback(mqtt_client_local,
			mqtt_incoming_publish_cb,
			mqtt_incoming_data_cb,
			LWIP_CONST_CAST(void*, &mqtt_client_info));
		// UNLOCK_TCPIP_CORE();

		// subscribe to all callback subscription topics
		// this makes a BIG assumption that we can subscribe multiple times to the same one?
		// TODO - check that subscribing multiple times to the same topic is not BAD
		for (i = 0; i < numCallbacks; i++) {
			if (callbacks[i]) {
				if (callbacks[i]->subscriptionTopic && callbacks[i]->subscriptionTopic[0]) {
					err = mqtt_sub_unsub(client,
						callbacks[i]->subscriptionTopic, 1,
						mqtt_request_cb_local, LWIP_CONST_CAST(void*, client_info),
						1);
					if (err != ERR_OK) {
						// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_subscribe to %s return: %d\n", callbacks[i]->subscriptionTopic, err);
					}
					else {
						// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_subscribed to %s\n", callbacks[i]->subscriptionTopic);
					}
				}
			}
		}

		clientId = CFG_GetMQTTNetIdLocal();

		snprintf(tmp, sizeof(tmp), "%s/connected", clientId);
		// LOCK_TCPIP_CORE();
		err = mqtt_publish(client, tmp, "online", strlen("online"), 2, true, mqtt_pub_request_cb, 0);
		// UNLOCK_TCPIP_CORE();
		if (err != ERR_OK) {
			// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Publish err: %d\n", err);
			if (err == ERR_CONN) {
				// g_my_reconnect_mqtt_after_time = 5;
			}
		}

		g_just_connected = 1;

		//mqtt_sub_unsub(client,
		//        "topic_qos1", 1,
		//        mqtt_request_cb_local, LWIP_CONST_CAST(void*, client_info),
		//        1);
		//mqtt_sub_unsub(client,
		//        "topic_qos0", 0,
		//        mqtt_request_cb_local, LWIP_CONST_CAST(void*, client_info),
		//        1);
	}
	else {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_connection_cb: Disconnected, reason: %d(%s)\n", status, get_callback_error_local(status));
	}
}

static void MQTT_do_connect(mqtt_client_t* client)
{
	const char* mqtt_userName, * mqtt_host, * mqtt_pass, * mqtt_clientID;
	int mqtt_port;
	int res;
	struct hostent* hostEntry;
	char will_topic[CGF_MQTT_CLIENT_ID_SIZE + 16];

	// mqtt_host = CFG_GetMQTTHost();
	mqtt_host = CFG_GetMQTTHostLocal();
	if (!mqtt_host[0]) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_host empty, not starting mqtt\r\n");
		snprintf(mqtt_status_message, sizeof(mqtt_status_message), "mqtt_host empty, not starting mqtt");
		return;
	}
	// mqtt_userName = CFG_GetMQTTUserName();
	// mqtt_pass = CFG_GetMQTTPass();
	mqtt_userName = CFG_GetMQTTUserNameLocal();
	mqtt_pass = CFG_GetMQTTPassLocal();
	mqtt_clientID = CFG_GetMQTTClientId();
	mqtt_port = CFG_GetMQTTPortLocal();

	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_userName %s\r\nmqtt_pass %s\r\nmqtt_clientID %s\r\nmqtt_host %s:%d\r\n",
	// 	mqtt_userName,
	// 	mqtt_pass,
	// 	mqtt_clientID,
	// 	mqtt_host,
	// 	mqtt_port
	// );

	// set pointer, there are no buffers to strcpy
	// empty field for us means "no password", etc,
	// but LWIP (without mods) expects a NULL pointer in that case...
	mqtt_client_info.client_id = mqtt_clientID;
	if(mqtt_pass[0] != 0) {
		mqtt_client_info.client_pass = mqtt_pass;
	} else {
		mqtt_client_info.client_pass = 0;
	}
	if(mqtt_userName[0] != 0) {
		mqtt_client_info.client_user = mqtt_userName;
	} else {
		mqtt_client_info.client_user = 0;
	}

	sprintf(will_topic, "%s/connected", CFG_GetMQTTNetIdLocal());
	mqtt_client_info.will_topic = will_topic;
	mqtt_client_info.will_msg = "offline";
	mqtt_client_info.will_retain = true,
		mqtt_client_info.will_qos = 0,

		hostEntry = gethostbyname(mqtt_host);
	if (NULL != hostEntry)
	{
		if (hostEntry->h_addr_list && hostEntry->h_addr_list[0]) {
			int len = hostEntry->h_length;
			if (len > 4) {
				// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_host resolves to addr len > 4\r\n");
				len = 4;
			}
			memcpy(&mqtt_ip_local, hostEntry->h_addr_list[0], len);
		}
		else {
			// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_host resolves no addresses?\r\n");
			snprintf(mqtt_status_message, sizeof(mqtt_status_message), "mqtt_host resolves no addresses?");
			return;
		}

		// host name/ip
		//ipaddr_aton(mqtt_host,&mqtt_ip_local);

		/* Initiate client and connect to server, if this fails immediately an error code is returned
		  otherwise mqtt_connection_cb will be called with connection result after attempting
		  to establish a connection with the server.
		  For now MQTT version 3.1.1 is always used */

		LOCK_TCPIP_CORE();
		res = mqtt_client_connect(mqtt_client_local,
			&mqtt_ip_local, mqtt_port,
			mqtt_connection_cb, LWIP_CONST_CAST(void*, &mqtt_client_info),
			&mqtt_client_info);
		UNLOCK_TCPIP_CORE();
		mqtt_connect_result = res;
		if (res != ERR_OK)
		{
			// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Connect error in mqtt_client_connect - code: %d (%s)\n", res, get_error_name_local(res));
			snprintf(mqtt_status_message, sizeof(mqtt_status_message), "mqtt_client_connect connect failed");
			if (res == ERR_ISCONN)
			{
				mqtt_disconnect(mqtt_client_local);
			}
		}
		else {
			mqtt_status_message[0] = '\0';
		}
	}
	else {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "mqtt_host %s not found by gethostbyname\r\n", mqtt_host);
		snprintf(mqtt_status_message, sizeof(mqtt_status_message), "mqtt_host %s not found by gethostbyname", mqtt_host);
	}
}

OBK_Publish_Result MQTT_PublishMain_StringInt_local(const char* sChannel, int iv)
{
	char valueStr[16];

	sprintf(valueStr, "%i", iv);

	return MQTT_PublishMain(mqtt_client_local, sChannel, valueStr, 0, true);

}

OBK_Publish_Result MQTT_PublishMain_StringFloat_local(const char* sChannel, float f)
{
	char valueStr[16];

	sprintf(valueStr, "%f", f);

	return MQTT_PublishMain(mqtt_client_local, sChannel, valueStr, 0, true);

}
OBK_Publish_Result MQTT_PublishMain_StringString_local(const char* sChannel, const char* valueStr, int flags)
{

	return MQTT_PublishMain(mqtt_client_local, sChannel, valueStr, flags, true);

}
double MQTT_MultiplierConfiguredOnChannel_local(int channel, int iVal) {
	// Init double variable
	double dVal = 0;

	// Checking if flag is set
	if (CFG_HasFlag(OBK_FLAG_PUBLISH_MULTIPLIED_VALUES)) {
		switch (CHANNEL_GetType(channel))
		{
		case ChType_Humidity_div10:
		case ChType_Temperature_div10:
		case ChType_Voltage_div10:
			dVal = (double)iVal / 10;
			break;
		case ChType_Frequency_div100:
		case ChType_Current_div100:
		case ChType_EnergyTotal_kWh_div100:
			dVal = (double)iVal / 100;
			break;
		case ChType_PowerFactor_div1000:
		case ChType_EnergyTotal_kWh_div1000:
		case ChType_EnergyExport_kWh_div1000:
		case ChType_EnergyToday_kWh_div1000:
		case ChType_Current_div1000:
			dVal = (double)iVal / 1000;
			break;
		default:
			break;
		}
	}

	// Returning double value. If it is zero then we dod nothing, or the value is indeed zero so it doesn't matter if we send a double or int.
	return dVal;
}
OBK_Publish_Result MQTT_ChannelChangeCallback_local(int channel, int iVal)
{
	char channelNameStr[8];
	char valueStr[16];
	int flags;

	// Getting double value if any, and converting if required
	double dVal = MQTT_MultiplierConfiguredOnChannel_local(channel, iVal);
	if (dVal == 0) {
		// Integer value
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Channel has changed! Publishing %i to channel %i \n", iVal, channel);
		sprintf(valueStr, "%i", iVal);
	}
	else {
		// Float value
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Channel has changed! Publishing %f to channel %i \n", dVal, channel);
		sprintf(valueStr, "%lf", dVal);
	}

	flags = 0;
	// MQTT_BroadcastTasmotaTeleSTATE_local();

	// String from channel number
	sprintf(channelNameStr, "%i", channel);

	// This will set RETAIN flag for all channels that are used for RELAY
	if (CFG_HasFlag(OBK_FLAG_MQTT_RETAIN_POWER_CHANNELS)) {
		if (CHANNEL_IsPowerRelayChannel(channel)) {
			flags |= OBK_PUBLISH_FLAG_RETAIN;
		}
	}

	return MQTT_PublishMain(mqtt_client_local, channelNameStr, valueStr, flags, true);
}
OBK_Publish_Result MQTT_ChannelPublish_local(int channel, int flags)
{
	char channelNameStr[8];
	char valueStr[16];
	int iValue;

	iValue = CHANNEL_Get(channel);

	// Getting double value if any, and converting if required
	double dVal = MQTT_MultiplierConfiguredOnChannel_local(channel, iValue);
	if (dVal == 0) {
		// Integer value
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Forced channel publish! Publishing val %i to %i", iValue, channel);
		sprintf(valueStr, "%i", iValue);
	}
	else {
		// Float value
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Forced channel publish! Publishing val %f to %i", dVal, channel);
		sprintf(valueStr, "%lf", dVal);
	}

	// String from channel number
	sprintf(channelNameStr, "%i", channel);

	// This will set RETAIN flag for all channels that are used for RELAY
	if (CFG_HasFlag(OBK_FLAG_MQTT_RETAIN_POWER_CHANNELS)) {
		if (CHANNEL_IsPowerRelayChannel(channel)) {
			flags |= OBK_PUBLISH_FLAG_RETAIN;
		}
	}

	return MQTT_PublishMain(mqtt_client_local, channelNameStr, valueStr, flags, true);
}
// This console command will trigger a publish of all used variables (channels and extra stuff)
commandResult_t MQTT_PublishAll_local(const void* context, const char* cmd, const char* args, int cmdFlags) {
	MQTT_PublishWholeDeviceState_Internal_local(true);
	return CMD_RES_OK;// TODO make return values consistent for all console commands
}
// This console command will trigger a publish of runtime variables
commandResult_t MQTT_PublishChannels_local(const void* context, const char* cmd, const char* args, int cmdFlags) {
	MQTT_PublishOnlyDeviceChannelsIfPossible_local();
	return CMD_RES_OK;// TODO make return values consistent for all console commands
}
commandResult_t MQTT_PublishCommand_local(const void* context, const char* cmd, const char* args, int cmdFlags) {
	const char* topic, * value;
	OBK_Publish_Result ret;

	Tokenizer_TokenizeString(args, 0);

	if (Tokenizer_GetArgsCount() < 2) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Publish command requires two arguments (topic and value)");
		return CMD_RES_NOT_ENOUGH_ARGUMENTS;
	}
	topic = Tokenizer_GetArg(0);
	value = Tokenizer_GetArg(1);

	ret = MQTT_PublishMain_StringString_local(topic, value, 0);

	return CMD_RES_OK;
}
// we have a separate command for integer because it can support math expressions
// (so it handled like $CH10*10, etc)
commandResult_t MQTT_PublishCommandInteger_local(const void* context, const char* cmd, const char* args, int cmdFlags) {
	const char* topic;
	int value;
	OBK_Publish_Result ret;

	Tokenizer_TokenizeString(args, 0);

	if (Tokenizer_GetArgsCount() < 2) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Publish command requires two arguments (topic and value)");
		return CMD_RES_NOT_ENOUGH_ARGUMENTS;
	}
	topic = Tokenizer_GetArg(0);
	value = Tokenizer_GetArgInteger(1);

	ret = MQTT_PublishMain_StringInt_local(topic, value);

	return CMD_RES_OK;
}

// we have a separate command for float because it can support math expressions
// (so it handled like $CH10*0.01, etc)
commandResult_t MQTT_PublishCommandFloat_local(const void* context, const char* cmd, const char* args, int cmdFlags) {
	const char* topic;
	float value;
	OBK_Publish_Result ret;

	Tokenizer_TokenizeString(args, 0);

	if (Tokenizer_GetArgsCount() < 2) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Publish command requires two arguments (topic and value)");
		return CMD_RES_NOT_ENOUGH_ARGUMENTS;
	}
	topic = Tokenizer_GetArg(0);
	value = Tokenizer_GetArgFloat(1);

	ret = MQTT_PublishMain_StringFloat_local(topic, value);

	return CMD_RES_OK;
}
/****************************************************************************************************
 *
 ****************************************************************************************************/
#define MQTT_TMR_DURATION      50

typedef struct BENCHMARK_TEST_INFO
{
	portTickType TestStartTick;
	portTickType TestStopTick;
	long msg_cnt;
	long msg_num;
	char topic[256];
	char value[256];
	float bench_time;
	float bench_rate;
	bool report_published;
} BENCHMARK_TEST_INFO;

#ifndef portTICK_RATE_MS
#define portTICK_RATE_MS ( ( portTickType ) 1000 / configTICK_RATE_HZ )
#endif

void MQTT_Test_Tick_local(void* param)
{
	BENCHMARK_TEST_INFO* info = (BENCHMARK_TEST_INFO*)param;
	int block = 1;
	err_t err;
	int qos = 0;
	int retain = 0;

	if (info != NULL)
	{
		while (1)
		{

			LOCK_TCPIP_CORE();
			int res = mqtt_client_is_connected(mqtt_client_local);
			UNLOCK_TCPIP_CORE();

			if (res == 0)
				break;
			if (info->msg_cnt < info->msg_num)
			{
				sprintf(info->value, "TestMSG: %li/%li Time: %i s, Rate: %i msg/s", info->msg_cnt, info->msg_num,
					(int)info->bench_time, (int)info->bench_rate);
				LOCK_TCPIP_CORE();
				err = mqtt_publish(mqtt_client_local, info->topic, info->value, strlen(info->value), qos, retain, mqtt_pub_request_cb, 0);
				UNLOCK_TCPIP_CORE();
				if (err == ERR_OK)
				{
					/* MSG published */
					info->msg_cnt++;
					info->TestStopTick = xTaskGetTickCount();
					/* calculate stats */
					info->bench_time = (float)(info->TestStopTick - info->TestStartTick);
					info->bench_time /= (float)(1000 / portTICK_RATE_MS);
					info->bench_rate = (float)info->msg_cnt;
					if (info->bench_time != 0.0)
						info->bench_rate /= info->bench_time;
					block--;
					if (block <= 0)
						break;
				}
				else {
					/* MSG not published, error occured */
					break;
				}
			}
			else {
				/* All messages publiched */
				if (info->report_published == false)
				{
					/* Publish report */
					sprintf(info->value, "Benchmark completed. %li msg published. Total Time: %i s MsgRate: %i msg/s",
						info->msg_cnt, (int)info->bench_time, (int)info->bench_rate);
					LOCK_TCPIP_CORE();
					err = mqtt_publish(mqtt_client_local, info->topic, info->value, strlen(info->value), qos, retain, mqtt_pub_request_cb, 0);
					UNLOCK_TCPIP_CORE();
					if (err == ERR_OK)
					{
						/* Report published */
						// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, info->value);
						info->report_published = true;
						/* Stop timer */
					}
				}
				break;
			}
		}
	}
}

commandResult_t MQTT_SetMaxBroadcastItemsPublishedPerSecond_local(const void* context, const char* cmd, const char* args, int cmdFlags)
{
	Tokenizer_TokenizeString(args, 0);

	if (Tokenizer_GetArgsCount() < 1) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Requires 1 arg");
		return CMD_RES_NOT_ENOUGH_ARGUMENTS;
	}
	g_maxBroadcastItemsPublishedPerSecond = Tokenizer_GetArgInteger(0);

	return CMD_RES_OK;
}
commandResult_t MQTT_SetBroadcastInterval_local(const void* context, const char* cmd, const char* args, int cmdFlags)
{
	Tokenizer_TokenizeString(args, 0);

	if (Tokenizer_GetArgsCount() < 1) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Requires 1 arg");
		return CMD_RES_NOT_ENOUGH_ARGUMENTS;
	}
	g_intervalBetweenMQTTBroadcasts = Tokenizer_GetArgInteger(0);

	return CMD_RES_OK;
}
static BENCHMARK_TEST_INFO* info = NULL;

#if WINDOWS

#elif PLATFORM_BL602
static void mqtt_timer_thread(void* param)
{
	while (1)
	{
		vTaskDelay(MQTT_TMR_DURATION);
		MQTT_Test_Tick_local(param);
	}
}
#elif PLATFORM_W600 || PLATFORM_W800
static void mqtt_timer_thread(void* param)
{
	while (1) {
		vTaskDelay(MQTT_TMR_DURATION);
		MQTT_Test_Tick_local(param);
	}
}
#elif PLATFORM_XR809
static OS_Timer_t timer;
#else
static beken_timer_t g_mqtt_timer;
#endif

commandResult_t MQTT_StartMQTTTestThread_local(const void* context, const char* cmd, const char* args, int cmdFlags)
{
	if (info != NULL)
	{
		/* Benchmark test already started */
		/* try to restart */
		info->TestStartTick = xTaskGetTickCount();
		info->msg_cnt = 0;
		info->report_published = false;
		return CMD_RES_OK;
	}

	info = (BENCHMARK_TEST_INFO*)os_malloc(sizeof(BENCHMARK_TEST_INFO));
	if (info == NULL)
	{
		return CMD_RES_ERROR;
	}

	memset(info, 0, sizeof(BENCHMARK_TEST_INFO));
	info->TestStartTick = xTaskGetTickCount();
	info->msg_num = 1000;
	sprintf(info->topic, "%s/benchmark", CFG_GetMQTTClientId());

#if WINDOWS

#elif PLATFORM_BL602
	xTaskCreate(mqtt_timer_thread, "mqtt", 1024, (void*)info, 15, NULL);
#elif PLATFORM_W600 || PLATFORM_W800
	xTaskCreate(mqtt_timer_thread, "mqtt", 1024, (void*)info, 15, NULL);
#elif PLATFORM_XR809
	OS_TimerSetInvalid(&timer);
	if (OS_TimerCreate(&timer, OS_TIMER_PERIODIC, MQTT_Test_Tick_local, (void*)info, MQTT_TMR_DURATION) != OS_OK)
	{
		printf("PIN_AddCommands timer create failed\n");
		return CMD_RES_ERROR;
	}
	OS_TimerStart(&timer); /* start OS timer to feed watchdog */
#else
	OSStatus result;

	result = rtos_init_timer(&g_mqtt_timer, MQTT_TMR_DURATION, MQTT_Test_Tick_local, (void*)info);
	ASSERT(kNoErr == result);
	result = rtos_start_timer(&g_mqtt_timer);
	ASSERT(kNoErr == result);
#endif
	return CMD_RES_OK;
}

/****************************************************************************************************
 *
 ****************************************************************************************************/

void MQTT_InitCallbacks_local() {
	char cbtopicbase[CGF_MQTT_CLIENT_ID_SIZE + 16];
	char cbtopicsub[CGF_MQTT_CLIENT_ID_SIZE + 16];

	const char* clientId;
	const char* groupId;
	unsigned char mac[6];
	getMAC(mac);
    uint64_t num = 0;
    for (int index = 0; index < 6; index++) {
        num = (num << 8) | (byte)mac[index];
    }
	MQTT_ClearCallbacks_local();
	g_mqtt_bBaseTopicDirty_local = 0;

	clientId = CFG_GetMQTTClientId();
	groupId = CFG_GetMQTTGroupTopic();

	// register the main set channel callback
		//###########################
	snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/garage.1/set", CFG_GetMQTTNetIdLocal());
	snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/garage.1/set", CFG_GetMQTTNetIdLocal());
	// note: this may REPLACE an existing entry with the same ID.  ID 1 !!!
	//addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Topic base: %s, Topic sub:%s", cbtopicbase, cbtopicsub);
	MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 0, channelSet_local);

	// so-called "Group topic", a secondary topic that can be set on multiple devices 
	// to control them together
	// register the TAS cmnd callback
	// if (*groupId) {
	// 	// register the main set channel callback
	// 	snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/", groupId);
	// 	snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/ws", groupId);
	// 	// note: this may REPLACE an existing entry with the same ID.  ID 2 !!!
	// 	MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 2, channelSet_local);
	// }

	// base topic
	// register the TAS cmnd callback
	// snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/cmnd", groupId);
	// snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/+", groupId);
	// // note: this may REPLACE an existing entry with the same ID.  ID 3 !!!
	// MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 2, tasCmnd_local);

	// so-called "Group topic", a secondary topic that can be set on multiple devices 
	// to control them together
	// register the TAS cmnd callback
	if (*groupId) {
		snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/garage.1/cmnd", CFG_GetMQTTNetIdLocal());
		snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/garage.1/cmnd", CFG_GetMQTTNetIdLocal());
		// note: this may REPLACE an existing entry with the same ID.  ID 4 !!!
		MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 1, tasCmnd_local);
	}

	// register the getter callback (send empty message here to get reply)
	snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/garage.1/get", CFG_GetMQTTNetIdLocal());
	snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/garage.1/get", CFG_GetMQTTNetIdLocal());
	// note: this may REPLACE an existing entry with the same ID.  ID 5 !!!
	MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 2, channelGet_local);

	// if (CFG_HasFlag(OBK_FLAG_DO_TASMOTA_TELE_PUBLISHES)) {
	// 	// test hack iobroker
	// 	snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/tele", groupId);
	// 	snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/+", groupId);
	// 	// note: this may REPLACE an existing entry with the same ID.  ID 6 !!!
	// 	MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 6, tasCmnd_local);

	// 	snprintf(cbtopicbase, sizeof(cbtopicbase), "%s/stat", groupId);
	// 	snprintf(cbtopicsub, sizeof(cbtopicsub), "%s/+", groupId);
	// 	// note: this may REPLACE an existing entry with the same ID.  ID 7 !!!
	// 	MQTT_RegisterCallback_local(cbtopicbase, cbtopicsub, 7, tasCmnd_local);
	// }
}
 // initialise things MQTT
 // called from user_main
void MQTT_init_local()
{
	// WINDOWS must support reinit
#ifdef WINDOWS
	mqtt_client_local = 0;
#endif

	MQTT_InitCallbacks_local();

	mqtt_initialised_local = 1;

	//cmddetail:{"name":"publish","args":"[Topic][Value]",
	//cmddetail:"descr":"Publishes data by MQTT. The final topic will be obk0696FB33/[Topic]/get. You can use argument expansion here, so $CH11 will change to value of the channel 11",
	//cmddetail:"fn":"MQTT_PublishCommand_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("publish", MQTT_PublishCommand_local, NULL);
	//cmddetail:{"name":"publishInt","args":"[Topic][Value]",
	//cmddetail:"descr":"Publishes data by MQTT. The final topic will be obk0696FB33/[Topic]/get. You can use argument expansion here, so $CH11 will change to value of the channel 11. This version of command publishes an integer, so you can also use math expressions like $CH10*10, etc.",
	//cmddetail:"fn":"MQTT_PublishCommand_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("publishInt", MQTT_PublishCommandInteger_local, NULL);
	//cmddetail:{"name":"publishFloat","args":"[Topic][Value]",
	//cmddetail:"descr":"Publishes data by MQTT. The final topic will be obk0696FB33/[Topic]/get. You can use argument expansion here, so $CH11 will change to value of the channel 11. This version of command publishes an float, so you can also use math expressions like $CH10*0.0, etc.",
	//cmddetail:"fn":"MQTT_PublishCommand_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("publishFloat", MQTT_PublishCommandFloat_local, NULL);
	//cmddetail:{"name":"publishAll","args":"",
	//cmddetail:"descr":"Starts the step by step publish of all available values",
	//cmddetail:"fn":"MQTT_PublishAll_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("publishAll", MQTT_PublishAll_local, NULL);
	//cmddetail:{"name":"publishChannels","args":"",
	//cmddetail:"descr":"Starts the step by step publish of all channel values",
	//cmddetail:"fn":"MQTT_PublishChannels_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("publishChannels", MQTT_PublishChannels_local, NULL);
	//cmddetail:{"name":"publishBenchmark","args":"",
	//cmddetail:"descr":"",
	//cmddetail:"fn":"MQTT_StartMQTTTestThread_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("publishBenchmark", MQTT_StartMQTTTestThread_local, NULL);
	//cmddetail:{"name":"mqtt_broadcastInterval","args":"[ValueSeconds]",
	//cmddetail:"descr":"If broadcast self state every 60 seconds/minute is enabled in flags, this value allows you to change the delay, change this 60 seconds to any other value in seconds. This value is not saved, you must use autoexec.bat or short startup command to execute it on every reboot.",
	//cmddetail:"fn":"MQTT_SetBroadcastInterval_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("mqtt_broadcastInterval", MQTT_SetBroadcastInterval_local, NULL);
	//cmddetail:{"name":"mqtt_broadcastItemsPerSec","args":"[PublishCountPerSecond]",
	//cmddetail:"descr":"If broadcast self state (this option in flags) is started, then gradually device info is published, with a speed of N publishes per second. Do not set too high value, it may overload LWIP MQTT library. This value is not saved, you must use autoexec.bat or short startup command to execute it on every reboot.",
	//cmddetail:"fn":"MQTT_SetMaxBroadcastItemsPublishedPerSecond_local","file":"mqtt/new_mqtt.c","requires":"",
	//cmddetail:"examples":""}
	// CMD_RegisterCommand("mqtt_broadcastItemsPerSec", MQTT_SetMaxBroadcastItemsPublishedPerSecond_local, NULL);
}

OBK_Publish_Result MQTT_DoItemPublishString_local(const char* sChannel, const char* valueStr)
{
	return MQTT_PublishMain(mqtt_client_local, sChannel, valueStr, OBK_PUBLISH_FLAG_MUTEX_SILENT, false);
}

OBK_Publish_Result MQTT_DoItemPublish_local(int idx)
{
	//int type;
	char dataStr[3 * 6 + 1];  //This is sufficient to hold mac value
	bool bWantsToPublish;

	switch (idx) {
	case PUBLISHITEM_SELF_STATIC_RESERVED_2:
	case PUBLISHITEM_SELF_STATIC_RESERVED_1:
		return OBK_PUBLISH_WAS_NOT_REQUIRED;

	case PUBLISHITEM_QUEUED_VALUES:
		return OBK_PUBLISH_OK;
		return PublishQueuedItems_local();

	case PUBLISHITEM_SELF_DYNAMIC_LIGHTSTATE:
	{
#if ENABLE_LED_BASIC
		if (LED_IsLEDRunning()) {
			return LED_SendEnableAllState();
		}
#endif
		return OBK_PUBLISH_WAS_NOT_REQUIRED;
	}
	case PUBLISHITEM_SELF_DYNAMIC_LIGHTMODE:
	{
#if ENABLE_LED_BASIC
		if (LED_IsLEDRunning()) {
			return LED_SendCurrentLightModeParam_TempOrColor();
		}
#endif
		return OBK_PUBLISH_WAS_NOT_REQUIRED;
	}
	case PUBLISHITEM_SELF_DYNAMIC_DIMMER:
	{
#if ENABLE_LED_BASIC
		if (LED_IsLEDRunning()) {
			return LED_SendDimmerChange();
		}
#endif
		return OBK_PUBLISH_WAS_NOT_REQUIRED;
	}
	case PUBLISHITEM_SELF_HOSTNAME:
		return OBK_PUBLISH_OK;
		return MQTT_DoItemPublishString_local("host", CFG_GetShortDeviceName());

	case PUBLISHITEM_SELF_BUILD:
		MQTT_ReturnState_local();
		return OBK_PUBLISH_OK;
		return MQTT_DoItemPublishString_local("build", g_build_str);

	case PUBLISHITEM_SELF_MAC:
		return OBK_PUBLISH_OK;
		return MQTT_DoItemPublishString_local("mac", HAL_GetMACStr(dataStr));

	case PUBLISHITEM_SELF_DATETIME:
		//Drivers are only built on BK7231 chips
#ifndef OBK_DISABLE_ALL_DRIVERS
		return OBK_PUBLISH_OK;
		if (DRV_IsRunning("NTP")) {
			sprintf(dataStr, "%d", NTP_GetCurrentTime());
			return MQTT_DoItemPublishString_local("datetime", dataStr);
		}
		else {
			return OBK_PUBLISH_WAS_NOT_REQUIRED;
		}
#else
		return OBK_PUBLISH_WAS_NOT_REQUIRED;
#endif

	case PUBLISHITEM_SELF_SOCKETS:
		return OBK_PUBLISH_OK;
		sprintf(dataStr, "%d", LWIP_GetActiveSockets());
		return MQTT_DoItemPublishString_local("sockets", dataStr);

	case PUBLISHITEM_SELF_RSSI:
		return OBK_PUBLISH_OK;
		sprintf(dataStr, "%d", HAL_GetWifiStrength());
		return MQTT_DoItemPublishString_local("rssi", dataStr);

	case PUBLISHITEM_SELF_UPTIME:
		return OBK_PUBLISH_OK;
		sprintf(dataStr, "%d", Time_getUpTimeSeconds());
		return MQTT_DoItemPublishString_local("uptime", dataStr);

	case PUBLISHITEM_SELF_FREEHEAP:
		return OBK_PUBLISH_OK;
		sprintf(dataStr, "%d", xPortGetFreeHeapSize());
		return MQTT_DoItemPublishString_local("freeheap", dataStr);

	case PUBLISHITEM_SELF_IP:
		g_firstFullBroadcast = false; //We published the last status item, disable full broadcast
		return OBK_PUBLISH_OK;
		return MQTT_DoItemPublishString_local("ip", HAL_GetMyIPString());

	default:
		break;
	}

	// Do not publish raw channel value for channels like PWM values, RGBCW has 5 raw channels.
	// We do not need raw values for RGBCW lights (or RGB, etc)
	// because we are using led_basecolor_rgb, led_dimmer, led_enableAll, etc
	// NOTE: negative indexes are not channels - they are special values
	bWantsToPublish = false;
	if (CHANNEL_ShouldBePublished(idx)) { //sua
		bWantsToPublish = true;
	}
#ifdef ENABLE_DRIVER_TUYAMCU
	// publish if channel is used by TuyaMCU (no pin role set), for example door sensor state with power saving V0 protocol
	// Not enabled by default, you have to set OBK_FLAG_TUYAMCU_ALWAYSPUBLISHCHANNELS flag
	if (!bWantsToPublish && CFG_HasFlag(OBK_FLAG_TUYAMCU_ALWAYSPUBLISHCHANNELS) && TuyaMCU_IsChannelUsedByTuyaMCU(idx)) {
		bWantsToPublish = true;
	}
#endif
	// TODO
	//type = CHANNEL_GetType(idx);
	if (bWantsToPublish) {
		return OBK_PUBLISH_OK;
		return MQTT_ChannelPublish_local(g_publishItemIndex_local, OBK_PUBLISH_FLAG_MUTEX_SILENT);
	}

	return OBK_PUBLISH_WAS_NOT_REQUIRED; // didnt publish
}

// from 5ms quicktick
int MQTT_RunQuickTick_local(){
#ifndef PLATFORM_BEKEN
	// on Beken, we use a one-shot timer for this.
	MQTT_process_received_local();
#endif
	return 0;
}

int g_timeSinceLastTasmotaTeleSent_local = 99;
int g_wantTasmotaTeleSend_local = 0;
void MQTT_BroadcastTasmotaTeleSENSOR_local() {
	bool bHasAnySensor = false;
#ifndef OBK_DISABLE_ALL_DRIVERS
	if (DRV_IsMeasuringPower()) {
		bHasAnySensor = true;
	}
#endif
	if (bHasAnySensor) {
		MQTT_ProcessCommandReplyJSON_local("SENSOR", "", COMMAND_FLAG_SOURCE_TELESENDER);
	}
}
void MQTT_BroadcastTasmotaTeleSTATE_local() {
	if (CFG_HasFlag(OBK_FLAG_DO_TASMOTA_TELE_PUBLISHES) == false) {
		return;
	}
	if (g_timeSinceLastTasmotaTeleSent_local < 1) {
		g_wantTasmotaTeleSend_local = 1;
		return;
	}
	MQTT_ProcessCommandReplyJSON_local("STATE", "", COMMAND_FLAG_SOURCE_TELESENDER);
	MQTT_BroadcastTasmotaTeleSENSOR_local();
	g_wantTasmotaTeleSend_local = 0;
}
// called from user timer.
int MQTT_RunEverySecondUpdate_local()
{
	if (!mqtt_initialised_local)
		return 0;

	if (Main_HasWiFiConnected() == 0)
	{
		mqtt_reconnect_local = 0;
		if (Main_HasFastConnect()) {
			mqtt_loopsWithDisconnected_local = LOOPS_WITH_DISCONNECTED + 1;
		}
		else {
			mqtt_loopsWithDisconnected_local = LOOPS_WITH_DISCONNECTED - 2;
		}
		return 0;
	}

	// take mutex for connect and disconnect operations
	if (MQTT_Mutex_Take(100) == 0)
	{
		return 0;
	}
	if (g_mqtt_bBaseTopicDirty_local) {
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT base topic is dirty, will reinit callbacks and reconnect\n");
		MQTT_InitCallbacks_local();
		mqtt_reconnect_local = 5;
	}

	// reconnect if went into MQTT library ERR_MEM forever loop
	if (g_memoryErrorsThisSession_local >= 5)
	{
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT will reconnect soon to fix ERR_MEM errors\n");
		g_memoryErrorsThisSession_local = 0;
		mqtt_reconnect_local = 5;
	}

	int res = 0;
	if (mqtt_client_local){
		LOCK_TCPIP_CORE();
		res = mqtt_client_is_connected(mqtt_client_local);
		UNLOCK_TCPIP_CORE();
	}
	// if asked to reconnect (e.g. change of topic(s))
	if (mqtt_reconnect_local > 0)
	{
		mqtt_reconnect_local--;
		// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT has pending reconnect in %i\n", mqtt_reconnect_local);
		if (mqtt_reconnect_local == 0)
		{
			// then if connected, disconnect, and then it will reconnect automatically in 2s
			if (mqtt_client_local && res)
			{
				// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "MQTT will now do a forced reconnect\n");
				MQTT_disconnect(mqtt_client_local);
				mqtt_loopsWithDisconnected_local = LOOPS_WITH_DISCONNECTED - 2;
			}
		}
	}

	if (mqtt_client_local == 0 || res == 0)
	{
		//addLogAdv(LOG_INFO,LOG_FEATURE_MAIN, "Timer discovers disconnected mqtt %i\n",mqtt_loopsWithDisconnected_local);
#if WINDOWS
#elif PLATFORM_BL602
#elif PLATFORM_W600 || PLATFORM_W800
#elif PLATFORM_XR809
#elif PLATFORM_BK7231N || PLATFORM_BK7231T || PLATFORM_BK7238
		if (OTA_GetProgress() == -1)
#endif
		{
			mqtt_loopsWithDisconnected_local++;
			if (mqtt_loopsWithDisconnected_local > LOOPS_WITH_DISCONNECTED)
			{
				if (mqtt_client_local == 0)
				{
					LOCK_TCPIP_CORE();
					mqtt_client_local = mqtt_client_new();
					UNLOCK_TCPIP_CORE();
				}
				else
				{
					LOCK_TCPIP_CORE();
					mqtt_disconnect(mqtt_client_local);
#if defined(MQTT_CLIENT_CLEANUP)
					mqtt_client_cleanup(mqtt_client_local);
#endif
					UNLOCK_TCPIP_CORE();
				}
				MQTT_do_connect(mqtt_client_local);
				mqtt_connect_events++;
				mqtt_loopsWithDisconnected_local = 0;
			}
		}
		MQTT_Mutex_Free();
		return 0;
	}
	else {
		// things to do in our threads on connection accepted.
		if (g_just_connected){
			g_just_connected = 0;
			// publish TELE
			// MQTT_BroadcastTasmotaTeleSTATE_local();
			// publish all values on state
			if (CFG_HasFlag(OBK_FLAG_MQTT_BROADCASTSELFSTATEONCONNECT)) {
				MQTT_PublishWholeDeviceState_local();
			}
			else {
				//MQTT_PublishOnlyDeviceChannelsIfPossible_local();
			}
		}

		MQTT_Mutex_Free();
		// below mutex is not required any more

		// it is connected
		g_timeSinceLastTasmotaTeleSent_local++;
		if (g_wantTasmotaTeleSend_local) {
			// MQTT_BroadcastTasmotaTeleSTATE_local();
		}
		g_timeSinceLastMQTTPublish_local++;
#if WINDOWS
#elif PLATFORM_BL602
#elif PLATFORM_W600 || PLATFORM_W800
#elif PLATFORM_XR809
#elif PLATFORM_BK7231N || PLATFORM_BK7231T || PLATFORM_BK7238
		if (OTA_GetProgress() != -1)
		{
			// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "OTA started MQTT will be closed\n");
			LOCK_TCPIP_CORE();
			mqtt_disconnect(mqtt_client_local);
			UNLOCK_TCPIP_CORE();
			return 1;
		}
#endif

		if (CFG_HasFlag(OBK_FLAG_DO_TASMOTA_TELE_PUBLISHES)) {
			static int g_mqtt_tasmotaTeleCounter = 0;
			g_mqtt_tasmotaTeleCounter++;
			if (g_mqtt_tasmotaTeleCounter % 3 == 0) {
				// MQTT_BroadcastTasmotaTeleSENSOR_local();
			}
			if (g_mqtt_tasmotaTeleCounter > 120) {
				g_mqtt_tasmotaTeleCounter = 0;
				// MQTT_BroadcastTasmotaTeleSTATE_local();
			}
		}

		// do we want to broadcast full state?
		// Do it slowly in order not to overload the buffers
		// The item indexes start at negative values for special items
		// and then covers Channel indexes up to CHANNEL_MAX
		//Handle only queued items. Don't need to do this separately if entire state is being published.
		if ((g_MqttPublishItemsQueued_local > 0) && !g_bPublishAllStatesNow_local)
		{
			PublishQueuedItems_local();
			return 1;
		}
		else if (g_bPublishAllStatesNow_local)
		{
			// Doing step by a step a full publish state
			//if (g_timeSinceLastMQTTPublish_local > 2)
			{
				OBK_Publish_Result publishRes;
				int g_sent_thisFrame = 0;
				while (g_publishItemIndex_local < CHANNEL_MAX)
				{
					publishRes = MQTT_DoItemPublish_local(g_publishItemIndex_local);
					if (publishRes != OBK_PUBLISH_WAS_NOT_REQUIRED)
					{
						if (false) {
							// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "[g_bPublishAllStatesNow_local] item %i result %i\n", g_publishItemIndex_local, publishRes);
						}
					}
					// There are several things that can happen now
					// OBK_PUBLISH_OK - it was required and was published
					if (publishRes == OBK_PUBLISH_OK)
					{
						g_sent_thisFrame++;
						if (g_sent_thisFrame >= g_maxBroadcastItemsPublishedPerSecond)
						{
							g_publishItemIndex_local++;
							break;
						}
					}
					// OBK_PUBLISH_MUTEX_FAIL - MQTT is busy
					if (publishRes == OBK_PUBLISH_MUTEX_FAIL
						|| publishRes == OBK_PUBLISH_WAS_DISCONNECTED)
					{
						// retry the same later
						break;
					}
					// OBK_PUBLISH_WAS_NOT_REQUIRED
					// The item is not used for this device
					g_publishItemIndex_local++;
				}

				if (g_publishItemIndex_local >= CHANNEL_MAX)
				{
					// done
					g_bPublishAllStatesNow_local = 0;
				}
			}
		}
		else {
			// not doing anything
			if (CFG_HasFlag(OBK_FLAG_MQTT_BROADCASTSELFSTATEPERMINUTE))
			{
				// this is called every second
				g_secondsBeforeNextFullBroadcast--;
				if (g_secondsBeforeNextFullBroadcast <= 0)
				{
					g_secondsBeforeNextFullBroadcast = g_intervalBetweenMQTTBroadcasts;
					MQTT_PublishWholeDeviceState_local();
				}
			}
		}
	}
	return 1;
}

MqttPublishItem_t* get_queue_tail_local(MqttPublishItem_t* head) {
	if (head == NULL) { return NULL; }

	while (head->next != NULL) {
		head = head->next;
	}
	return head;
}

MqttPublishItem_t* find_queue_reusable_item_local(MqttPublishItem_t* head) {
	while (head != NULL) {
		if (MQTT_QUEUE_ITEM_IS_REUSABLE(head)) {
			return head;
		}
		head = head->next;
	}
	return head;
}

/// @brief Queue an entry for publish and execute a command after the publish.
/// @param topic 
/// @param channel 
/// @param value 
/// @param flags
/// @param command Command to execute after the publish
void MQTT_QueuePublishWithCommand_local(const char* topic, const char* channel, const char* value, int flags, PostPublishCommands command) {
	MqttPublishItem_t* newItem;
	if (g_MqttPublishItemsQueued_local >= MQTT_MAX_QUEUE_SIZE) {
		// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Unable to queue! %i items already present\r\n", g_MqttPublishItemsQueued_local);
		return;
	}

	if ((strlen(topic) > MQTT_PUBLISH_ITEM_TOPIC_LENGTH) ||
		(strlen(channel) > MQTT_PUBLISH_ITEM_CHANNEL_LENGTH) ||
		(strlen(value) > MQTT_PUBLISH_ITEM_VALUE_LENGTH)) {
		// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "Unable to queue! Topic (%i), channel (%i) or value (%i) exceeds size limit\r\n",
		// 	strlen(topic), strlen(channel), strlen(value));
		return;
	}

	//Queue data for publish. This might be a new item in the queue or an existing item. This is done to prevent
	//memory fragmentation. The total queue length is limited to MQTT_MAX_QUEUE_SIZE.

	if (g_MqttPublishQueueHead_local == NULL) {
		g_MqttPublishQueueHead_local = newItem = os_malloc(sizeof(MqttPublishItem_t));
		newItem->next = NULL;
	}
	else {
		newItem = find_queue_reusable_item_local(g_MqttPublishQueueHead_local);

		if (newItem == NULL) {
			newItem = os_malloc(sizeof(MqttPublishItem_t));
			newItem->next = NULL;
			get_queue_tail_local(g_MqttPublishQueueHead_local)->next = newItem; //Append new item
		}
	}

	//os_strcpy does copy ending null character.
	os_strcpy(newItem->topic, topic);
	os_strcpy(newItem->channel, channel);
	os_strcpy(newItem->value, value);
	newItem->command = command;
	newItem->flags = flags;

	g_MqttPublishItemsQueued_local++;
	// addLogAdv(LOG_INFO, LOG_FEATURE_MQTT, "Queued topic=%s/%s, %i items in queue", newItem->topic, newItem->channel, g_MqttPublishItemsQueued_local);
}

/// @brief Add the specified command to the last entry in the queue.
/// @param command 
void MQTT_InvokeCommandAtEnd_local(PostPublishCommands command) {
	MqttPublishItem_t* tail = get_queue_tail_local(g_MqttPublishQueueHead_local);
	if (tail == NULL){
		// addLogAdv(LOG_ERROR, LOG_FEATURE_MQTT, "InvokeCommandAtEnd invoked but queue is empty");
	}
	else {
		tail->command = command;
	}
}

/// @brief Queue an entry for publish.
/// @param topic 
/// @param channel 
/// @param value 
/// @param flags
void MQTT_QueuePublish_local(const char* topic, const char* channel, const char* value, int flags) {
	MQTT_QueuePublishWithCommand_local(topic, channel, value, flags, None);
}


/// @brief Publish MQTT_QUEUED_ITEMS_PUBLISHED_AT_ONCE queued items.
/// @return 
OBK_Publish_Result PublishQueuedItems_local() {
	OBK_Publish_Result result = OBK_PUBLISH_WAS_NOT_REQUIRED;

	int count = 0;
	MqttPublishItem_t* head = g_MqttPublishQueueHead_local;

	//The next actionable item might not be at the front. The queue size is limited to MQTT_QUEUED_ITEMS_PUBLISHED_AT_ONCE
	//so this traversal is fast.
	//addLogAdv(LOG_INFO,LOG_FEATURE_MQTT,"PublishQueuedItems_local g_MqttPublishItemsQueued_local=%i",g_MqttPublishItemsQueued_local );
	while ((head != NULL) && (count < MQTT_QUEUED_ITEMS_PUBLISHED_AT_ONCE) && (g_MqttPublishItemsQueued_local > 0)) {
		if (!MQTT_QUEUE_ITEM_IS_REUSABLE(head)) {  //Skip reusable entries
			count++;
			result = MQTT_PublishTopicToClient(mqtt_client_local, head->topic, head->channel, head->value, head->flags, false);
			MQTT_QUEUE_ITEM_SET_REUSABLE(head); //Flag item as reusable
			g_MqttPublishItemsQueued_local--;   //decrement queued count

			//Stop if last publish failed
			if (result != OBK_PUBLISH_OK) break;

			switch (head->command) {
			case None:
				break;
			case PublishAll:
				// MQTT_PublishWholeDeviceState_Internal_local(true);
				break;
			case PublishChannels:
				// MQTT_PublishOnlyDeviceChannelsIfPossible_local();
				break;
			}
		}
		else {
			//addLogAdv(LOG_INFO,LOG_FEATURE_MQTT,"PublishQueuedItems_local item skipped reusable");
		}

		head = head->next;
	}

	return result;
}


/// @brief Is MQTT sub system ready and connected?
/// @return 
bool MQTT_IsReady_local() {
	int res = 0;
	if (mqtt_client_local){
		LOCK_TCPIP_CORE();
		res = mqtt_client_is_connected(mqtt_client_local);
		UNLOCK_TCPIP_CORE();
	}
	return mqtt_client_local && res;
}

