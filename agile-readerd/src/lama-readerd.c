#include "lama-readerd.h"
#include "/usr/local/Cellar/json-c/0.9/include/json/json.h"

#define DBG(fmt ...) do { \
	printf(fmt); \
} while(0);

#define __FN__ __FUNCTION__
#define UNIX_PATH_MAX 128
#define MAX_READ 4096
#define BACKLOG 8192

typedef struct memchunk {
	unsigned char *memory;
	size_t len;
	int *fd;
	int bytes_wrote;
} memchunk;

typedef struct readerd_cfg {
	char sock_path[UNIX_PATH_MAX];
	int sock_domain;
	int sock_type;
	int unlink_on_start;
	int num_workers;
} readerd_cfg;


void	readerd_show_config(struct readerd_cfg *cfg)
{
	DBG("[%s] sock_path: %s\n", __FN__, cfg->sock_path);
	DBG("[%s] sock_domain: %d\n", __FN__, cfg->sock_domain);
	DBG("[%s] sock_type: %d\n", __FN__, cfg->sock_type);
	DBG("[%s] unlink_on_start: %d\n", __FN__, cfg->unlink_on_start);
}

int	readerd_server_sock(int *sfd, struct readerd_cfg *cfg)
{
	struct sockaddr_un saddr;

	memset(&saddr, 0, sizeof(struct sockaddr_un));
	saddr.sun_family = AF_UNIX;

	sprintf(saddr.sun_path, "%s", cfg->sock_path);
	if(cfg->unlink_on_start) {
		if(unlink(cfg->sock_path) < 0) {
			perror("unlink");
		}
		DBG("[%s] unlink(%s) success\n", __FN__, cfg->sock_path);
	}
	if(unlikely((*sfd = socket(cfg->sock_domain, cfg->sock_type, 0)) == -1)) {
		perror("socket"); return -1;
	}
	DBG("[%s] Created %s @ %p:%p:%d\n", __FN__, cfg->sock_path, &sfd, sfd, *sfd);
	if(unlikely(bind(*sfd, (struct sockaddr *)&saddr, sizeof(struct sockaddr_un)) < 0)) {
		perror("bind"); 
		return -1;
	}
	if(cfg->sock_type == SOCK_STREAM) {
		/* Only call listen for non-datagram */
		if(unlikely(listen(*sfd, BACKLOG) < 0)) {
			perror("listen");
			return -1;
		}
	}
	return 0;
}

int	readerd_accept(int sfd)
{
	int cfd;
	struct sockaddr_un saddr;
	unsigned int len = sizeof(struct sockaddr_un);

	memset(&saddr, 0, sizeof(saddr));

	if(unlikely((cfd = accept(sfd, (struct sockaddr *)&saddr, &len)) < 0)) {
		perror("accept");
		if(cfd < 0 && errno == EMFILE) {
			DBG("[%s] backlog filled, FIXME: keep a emergency descriptor around\n", __FN__);
		}
		return -1;
	}
	return cfd;
}

void * safe_realloc(void *ptr, size_t size)
{
	if(ptr) {
		return realloc(ptr, size);
	} else {
		return malloc(size);
	}
}

static size_t cb_curl_write_memory(void *ptr, size_t size, size_t nmemb, void *data)
{
	int bytes_wrote = 0;
	size_t realsize = size*nmemb;
	struct memchunk *chunk = (struct memchunk *)data;

	bytes_wrote = write(*chunk->fd, ptr, realsize);
	DBG("[%s] bytes_wrote:%d chunk->fd: %d size passed:%ld\n", __FN__, bytes_wrote, *chunk->fd, size);

	chunk->memory = safe_realloc(chunk->memory, chunk->len + realsize + 1);
	if(chunk->memory) {
		memcpy(&(chunk->memory[chunk->len]), ptr, realsize);
		chunk->len += realsize;
		chunk->memory[chunk->len] = 0;
	}
	return realsize;
}

int	transport_read(int *client_sock, char *url, unsigned long offset, unsigned long size)
{
	CURL *curl;
	CURLcode res;

	struct memchunk chunk;
	long http_code;

	char range_str[1024];
	chunk.memory = NULL;
	chunk.len = 0;
	chunk.fd = client_sock;

	snprintf(range_str, sizeof(range_str), "%jd-%jd", offset, offset + (size - 1));
	DBG("[%s] range_str=%s\n", __FN__, range_str);

	char agent_str[1024];

	sprintf(agent_str, "lama-readerd/1.0 %s", range_str);
	curl = curl_easy_init();

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cb_curl_write_memory);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, agent_str);
	curl_easy_setopt(curl, CURLOPT_RANGE, range_str);

	res = curl_easy_perform(curl);
	if(curl_easy_getinfo(curl, CURLINFO_HTTP_CODE, &http_code) == CURLE_OK) {
		DBG("[%s] url=%s http_code=%jd\n", __FN__, url, http_code);
	}
	curl_easy_cleanup(curl);

	if(chunk.memory) {
		DBG("[%s] freeing chunk.len: %ld\n", __FN__, chunk.len);
		free(chunk.memory);
	}

	return 0;
}

int	main(int argc, char **argv)
{
	int sfd = 0;
	struct readerd_cfg cfg;

	memset(&cfg, 0, sizeof(struct readerd_cfg));
	strcpy(cfg.sock_path, "/tmp/lama-readerd.sock");
	cfg.unlink_on_start = 1;
	cfg.sock_domain = PF_UNIX;
	cfg.sock_type = SOCK_STREAM;
	cfg.num_workers = 10;

	setvbuf(stdout, NULL, _IOLBF, 4096);
	readerd_show_config(&cfg);
	if(readerd_server_sock(&sfd, &cfg) < 0) {
		DBG("[%s] error\n", __FN__);
		return -1;
	}
	while(1)
	{
		char buf[MAX_READ];
		int bytes;
		int cfd = readerd_accept(sfd);
		bytes = read(cfd, buf, MAX_READ);
		DBG("[%s] read %d bytes\n", __FN__, bytes);
		if(bytes > 0) {
			struct json_object *base_obj, *url_obj, *offset_obj, *size_obj;

			/* FIXME, json below will segfault if the payload isnt really json
			 */
			base_obj = json_tokener_parse(buf);
			url_obj = json_object_object_get(base_obj, "url");
			char *url = (char *)json_object_get_string(url_obj);
			DBG("[%s] %s\n", __FN__, url);

			offset_obj = json_object_object_get(base_obj, "offset");
			int offset = json_object_get_int(offset_obj);
			DBG("[%s] %d\n", __FN__, offset);

			size_obj = json_object_object_get(base_obj, "size");
			int size = json_object_get_int(size_obj);
			DBG("[%s] %d\n", __FN__, size);

			transport_read(&cfd, url, offset, size);
			close(cfd);
			DBG("[%s] closed client socket\n", __FN__);

			json_object_put(base_obj);
			json_object_put(url_obj);
			json_object_put(offset_obj);
			json_object_put(size_obj);
		} else {
			DBG("[%s] bytes is 0, skipping unpack\n", __FN__);
		}
	}

return 0;
}
