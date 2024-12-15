#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 199309L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <syslog.h>
#include <semaphore.h>
#include <limits.h>
#include <linux/limits.h>
#include <time.h>
#include <sys/time.h>
#include <sys/statvfs.h>
#include <stdbool.h>
#include <arpa/inet.h>

/**
 * 
 * REMCP SERVER - remote copy server 
 * by Vinicius Guazzelli Dias - 2024
 * 
 */


// ********** constants **********

#define PORT 5150
#define THREAD_POOL_SIZE 8
#define MAX_TRANSFER_RATE 1000000 // 1 MB/s
#define COMMAND_PIPE "/tmp/remcp_command_pipe"
#define RESPONSE_PIPE "/tmp/remcp_response_pipe"
#define REMOTE 0
#define LOCAL 1
#define HANDSHAKE_REQUEST "REMCP?\n"
#define HANDSHAKE_RESPONSE "YEP\n"
#define HANDSHAKE_RETRY "TRYAGAIN\n"
#define HANDSHAKE_TIMEOUT 5
#define HANDSHAKE_RETRY_BACKOFF 1000
#define HANDSHAKE_RETRY_MAX 3
#define MAX_PORT_ATTEMPTS 10
#define TRUE 1
#define FALSE 0


// ********** global variables **********

int server_socket_fd;

int current_sim_requests = 0;
pthread_mutex_t current_sim_requests_mutex = PTHREAD_MUTEX_INITIALIZER;

int request_buffer[THREAD_POOL_SIZE];
int buffer_count = 0;
pthread_mutex_t request_buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_t thread_pool[THREAD_POOL_SIZE];
pthread_t local_command_thread;

// maybe create a full and empty semaphore (see later)
sem_t request_available;

int response_pipe_fd = -1;

// ********** structs and enums **********

enum RequestType {
    CMD_STATS = 0,
    CMD_GET,
    CMD_PUT,
    CMD_INVALID
};

static const char* REQUEST_STRINGS[] = {
    [CMD_STATS] = "STAT",
    [CMD_GET]  = "GET",
    [CMD_PUT]  = "PUT"
};

struct Request {
    enum RequestType type;
    char path[PATH_MAX];
    long offset;
    long size;
};

typedef enum {
    HS_SUCCESS = 0,
    HS_RETRY = 1,
    HS_FAILURE = 2
} HandshakeStatus;


// ********** prototypes **********

void cleanup(void);
void daemonize(void);
int create_pipes(void);
int start_local_command_thread(void);
int create_thread_pool(int pool_size, pthread_t *thread_pool);
int start_server(void);
int receive_connections(void);
void *process_request(void *arg);
void *process_local_command(void *arg);
int handle_download_command(char *ip, char *remote_path, char *local_path);
int handle_upload_command(char *local_path, char *ip, char *remote_path);
int handle_remote_stats_command(char *ip);
void handle_stats_command(void);
int handle_stat(int sock_fd, int local);
int parse_command(char *command);
int check_disk_space(const char *path, off_t required_size);
int accept_handshake(int sock_fd);
HandshakeStatus initiate_handshake(int sock_fd);
int send_retry_message(int sock_fd);
struct Request parse_request(const char* input);
int handle_request(struct Request req, int sock_fd);
int connect_to_peer(char *ip);
int send_remote_message(int sock_fd, const char *data, size_t len);
ssize_t receive_remote_with_timeout(int sock_fd, char *buffer, size_t len, int timeout_sec);
int reject_handshake(int sock_fd);
ssize_t recv_line_with_timeout(int sock_fd, char *buffer, size_t max_len, int timeout_sec);


// ********** server utils **********

void cleanup() {
    sem_destroy(&request_available);
    pthread_mutex_destroy(&request_buffer_mutex);
    pthread_mutex_destroy(&current_sim_requests_mutex);
    close(server_socket_fd);
    unlink(COMMAND_PIPE);
    unlink(RESPONSE_PIPE);
    syslog(LOG_INFO, "[main]Cleanup complete");
    // TODO: close all sockets in the request_buffer and being processed by the thread pool
}

void daemonize() {
    pid_t pid, sid;
    // Fork off the parent process
    pid = fork();
    if (pid < 0) {
        fprintf(stderr, "Failed to fork process: %s (daemonize)\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    // Exit parent process
    if (pid > 0) {
        printf("REMCP SERVER Daemon process started with PID: %d\n", pid);
        exit(EXIT_SUCCESS);
    }
    // From this point on, we're in the child process (which has pid = 0)
    // Change the file mode mask
    umask(0);
    // Open logs before closing standard streams
    openlog("REMCP_server_daemon", LOG_PID, LOG_DAEMON);
    // Create a new SID for the child process
    sid = setsid();
    if (sid < 0) {
        fprintf(stderr, "Failed to create SID: %s (daemonize)\n", strerror(errno));
        syslog(LOG_ERR, "[main] Failed to create SID: %s (daemonize)", strerror(errno));
        exit(EXIT_FAILURE);
    }
    // Change the current working directory
    if ((chdir("/")) < 0) {
        fprintf(stderr, "Failed to change directory: %s (daemonize)\n", strerror(errno));
        syslog(LOG_ERR, "[main] Failed to change directory: %s (daemonize)", strerror(errno));
        exit(EXIT_FAILURE);
    }
    // Now we can close standard file descriptors
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
    syslog(LOG_INFO, "[main] ********** Daemonized successfully ********** ");
}

int create_thread_pool(int pool_size, pthread_t *thread_pool){
    for (int i = 0; i < pool_size; i++) {
        if(pthread_create(&thread_pool[i], NULL, process_request, NULL) != 0) {
            syslog(LOG_ERR, "[main] Failed to create thread %d: %s (create_thread_pool)", i, strerror(errno));
            return -1;
        }
        if(pthread_detach(thread_pool[i]) != 0) {
            syslog(LOG_ERR, "[main] Failed to detach thread %d: %s (create_thread_pool)", i, strerror(errno));
            return -1;
        }
    }
    syslog(LOG_INFO, "[main] Thread pool created");
    return 0;
}

int start_local_command_thread() {
    if(pthread_create(&local_command_thread, NULL, process_local_command, NULL) != 0) {
        syslog(LOG_ERR, "Failed to create local command thread: %s (start_local_command_thread)", strerror(errno));
        return -1;
    }
    syslog(LOG_INFO, "Local command thread created");
    return 0;
}

int create_pipes() {
    if (mkfifo(COMMAND_PIPE, 0666) == -1) {
        if (errno != EEXIST) {  // Ignore error if pipe already exists
            syslog(LOG_ERR, "[main] mkfifo failed command thread: %s (local_command_handler)", strerror(errno));
            return -1;
        }
    }
    syslog(LOG_INFO, "[main] Command pipe created");

    if (mkfifo(RESPONSE_PIPE, 0666) == -1) {
        if (errno != EEXIST) {  // Ignore error if pipe already exists
            syslog(LOG_ERR, "[main] mkfifo failed response thread: %s (local_command_handler)", strerror(errno));
            return -1;
        }
    }
    syslog(LOG_INFO, "[main] Response pipe created");
    return 0;
}

int start_server(){
    struct sockaddr_in servaddr;
    int port = PORT;

    server_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_fd < 0) {
        syslog(LOG_ERR, "[main] Failed to create socket: %s (start_server)", strerror(errno));
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

    for (;;) {
        servaddr.sin_port = htons(port);
        if (bind(server_socket_fd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == 0) {
            break;
        }
        port++;
    }
    listen(server_socket_fd, SOMAXCONN);
    syslog(LOG_INFO, "[main] Started server on port %d\n", port);
}

int send_retry_message(int sock_fd) {
    char *message = "TRYAGAIN\n";
    send(sock_fd, message, strlen(message), 0);
    syslog(LOG_INFO, "[main] Sent retry message");
    return 0;
}

int receive_connections() {
    struct sockaddr_in cliaddr;
    socklen_t clilen;
    
    for (;;) {
        int *new_sock = malloc(sizeof(int));
        if (new_sock == NULL) {
            syslog(LOG_ERR, "[main] Failed to allocate memory for new socket");
            continue;
        }

        clilen = sizeof(cliaddr);
        *new_sock = accept(server_socket_fd, (struct sockaddr*)&cliaddr, &clilen);
        if (*new_sock < 0) {
            free(new_sock);
            syslog(LOG_ERR, "[main] Failed to accept connection: %s", strerror(errno));
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        // Convert IP from binary to string format
        inet_ntop(AF_INET, &(cliaddr.sin_addr), client_ip, INET_ADDRSTRLEN);
        
        // Log the client's IP and port
        syslog(LOG_INFO, "[main] Accepted connection from %s:%d", 
               client_ip, 
               ntohs(cliaddr.sin_port));

        pthread_mutex_lock(&request_buffer_mutex);
        if (buffer_count < THREAD_POOL_SIZE) {
            if (accept_handshake(*new_sock) != 0) {
                continue;
            }
            syslog(LOG_INFO, "[main] Accepted REQUEST from %s:%d", 
               client_ip, 
               ntohs(cliaddr.sin_port));
            request_buffer[buffer_count] = *new_sock;
            buffer_count++;
            free(new_sock);
            sem_post(&request_available);  // Signal that new work is available
        } else {
            if (reject_handshake(*new_sock) != 0) {
                continue;
            }
            syslog(LOG_INFO, "[main] Rejected REQUEST from %s:%d due to overload. Sending retry message...", 
               client_ip, 
               ntohs(cliaddr.sin_port));
            send_retry_message(*new_sock);
            close(*new_sock);
            free(new_sock);
        }
        pthread_mutex_unlock(&request_buffer_mutex);
    }
    return 0;
}

// ********** local client operations **********

void send_local_message(char *message, int close_pipe) {
    if (response_pipe_fd == -1) { 
        syslog(LOG_INFO, "[local command handler] Opening response pipe (write mode)");
        response_pipe_fd = open(RESPONSE_PIPE, O_WRONLY);
        if (response_pipe_fd < 0) {
            syslog(LOG_ERR, "[local command handler] Failed to open pipe command thread: %s (send_local_message)", strerror(errno));
            return;
        }
    }
    //syslog(LOG_INFO, "[local command handler] Sending message to client: %s", message);
    write(response_pipe_fd, message, strlen(message));
    if(close_pipe) {
        syslog(LOG_INFO, "[local command handler] Closing response pipe");
        close(response_pipe_fd);
        response_pipe_fd = -1;
    }
}

void handle_stats_command() {
    handle_stat(0, LOCAL);
}

int handle_remote_stats_command(char *ip) {
    syslog(LOG_INFO, "[local command handler] Handling remote STATS command from IP %s", ip);
    syslog(LOG_INFO, "[local command handler] Connecting to peer %s\n", ip);
    int sock_fd = connect_to_peer(ip);
    if (sock_fd < 0) {
        send_local_message("ERROR Failed to connect to peer\n", TRUE);
        syslog(LOG_ERR, "[local command handler] Failed to connect to peer %s\n", ip);
        return -1;
    }
    // send message to peer requesting stats
    syslog(LOG_INFO, "[local command handler] Sending STATS command to peer");
    send_remote_message(sock_fd, "STATS\n", 6);
    // receive response and send to local client
    char response[256];
    syslog(LOG_INFO, "[local command handler] Receiving response from peer");
    // TODO: maybe this receive_remote_with_timeout is not working
    ssize_t bytes = receive_remote_with_timeout(sock_fd, response, sizeof(response), 5);
    if (bytes < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to receive response");
        return -1;
    }
    response[bytes] = '\0';
    syslog(LOG_INFO, "[local command handler] Sending response to local client");
    send_local_message(response, TRUE);
    close(sock_fd);
    return 0;
}

int check_disk_space(const char *path, off_t required_size) {
    struct statvfs stat;
    char dir_path[PATH_MAX];
    strncpy(dir_path, path, PATH_MAX);
    
    // Get directory path by removing filename
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash != NULL) {
        *last_slash = '\0';  // Truncate at last slash
    }
    
    // Get filesystem stats for the directory
    if (statvfs(dir_path, &stat) != 0) {
        syslog(LOG_ERR, "[local command handler] Failed to get filesystem stats: %s", strerror(errno));
        return -1;
    }
    
    // Calculate available space
    uint64_t available_bytes = stat.f_bsize * stat.f_bavail;
    
    // Add some margin (e.g., 1MB or 5% of required size)
    uint64_t margin = 1048576;  // 1MB
    if (required_size * 0.05 > margin) {
        margin = required_size * 0.05;
    }
    
    // Check if we have enough space
    if (available_bytes < (required_size + margin)) {
        syslog(LOG_ERR, "[local command handler] Not enough disk space. Need: %lu bytes, Available: %lu bytes", 
               required_size + margin, available_bytes);
        return 0;  // Not enough space
    }
    syslog(LOG_INFO, "[local command handler] Enough disk space. Need: %lu bytes (plus a %lu margin), Available: %lu bytes", 
               required_size, margin, available_bytes);
    return 1;  // Enough space available
}

int handle_download_command(char *ip, char *remote_path, char *local_path) {
    off_t offset; // offset to start downloading from
    struct stat st; // file stats
    int counter = 0; // counter to handle file name collisions
    char *ext_text; // file extension text
    char final_path[PATH_MAX]; // final path
    char base_path[PATH_MAX]; // base path
    char part_path[PATH_MAX]; // .part file path
    char *extension; // file extension

    syslog(LOG_INFO, "[local command handler] Handling DOWNLOAD command (remote IP: %s, remote path: %s, local path: %s)", ip, remote_path, local_path);
    syslog(LOG_INFO, "[local command handler] Connecting to peer %s", ip);
    int sock_fd = connect_to_peer(ip);
    if (sock_fd < 0) {
        syslog(LOG_INFO, "[local command handler] Handle Download failed to connect to peer... %s", ip);
        return -1;
    }
    syslog(LOG_INFO, "[local command handler] Connected to peer %s", ip);

    syslog(LOG_INFO, "[local command handler] Parsing local path %s", local_path);
    strncpy(base_path, local_path, PATH_MAX - 1);
    extension = strrchr(base_path, '.');
    if (extension) {
        ext_text = extension + 1;
        *extension = '\0';  // Temporarily split path and extension
    }
    do {
        if (counter == 0) {
            snprintf(final_path, PATH_MAX, "%s%s%s", base_path, 
                    ext_text ? "." : "",      // Add the dot back if there's an extension
                    ext_text ? ext_text : "");
        } else {
            int len = snprintf(final_path, PATH_MAX, "%s_%d%s%s", base_path, 
                    counter,
                    ext_text ? "." : "",      // Add the dot back if there's an extension
                    ext_text ? ext_text : "");
            if (len >= PATH_MAX) {
                syslog(LOG_ERR, "[local command handler] Path length exceeds PATH_MAX");
                send_local_message("ERROR Path length exceeds PATH_MAX\n", TRUE);
                return -1;
            }
        }
        counter++;
    } while (stat(final_path, &st) == 0 && counter < 1000);

    int len = snprintf(part_path, PATH_MAX, "%s.part", final_path);
    if (len >= PATH_MAX) {
        syslog(LOG_ERR, "[local command handler] Path length exceeds PATH_MAX");
        send_local_message("ERROR Path length exceeds PATH_MAX\n", TRUE);
        return -1;
    }

    char dir_path[PATH_MAX];
    strncpy(dir_path, final_path, PATH_MAX);
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash) {
        *last_slash = '\0';  // Trunca no último '/' para ter só o path do diretório
        
        // Verifica se diretório existe
        struct stat st = {0};
        if (stat(dir_path, &st) == -1) {
            syslog(LOG_INFO, "[remote command handler] Directory does not exist, creating: %s", dir_path);
            
            // Criar diretório recursivamente
            char tmp[PATH_MAX];
            char *p = NULL;
            
            snprintf(tmp, sizeof(tmp), "%s", dir_path);
            for (p = tmp + 1; *p; p++) {
                if (*p == '/') {
                    *p = 0;
                    mkdir(tmp, 0755);
                    *p = '/';
                }
            }
            mkdir(tmp, 0755);
            
            // Verificar se criação foi bem sucedida
            if (stat(dir_path, &st) == -1) {
                syslog(LOG_ERR, "[remote command handler] Failed to create directory: %s", strerror(errno));
                return -1;
            }
        }
    }

    syslog(LOG_INFO, "[local command handler] Final path: %s", final_path);
    syslog(LOG_INFO, "[local command handler] Base path: %s", base_path);
    syslog(LOG_INFO, "[local command handler] Extension: %s", ext_text);
    syslog(LOG_INFO, "[local command handler] .part file path: %s", part_path);

    syslog(LOG_INFO, "[local command handler] Checking if .part file exists");
    if (stat(part_path, &st) == 0) {
        syslog(LOG_INFO, "[local command handler] .part file exists.Offset: %ld", offset);
        offset = st.st_size;  // Resume from end of partial file
    }
    else {
        offset = 0;
        syslog(LOG_INFO, "[local command handler] .part file does not exist.Offset: %ld", offset);
    }
    
    // send GET message to peer
    syslog(LOG_INFO, "[local command handler] Sending GET message to peer");
    char message[PATH_MAX + 16];
    snprintf(message, sizeof(message), "GET %s %ld\n", remote_path, offset);
    send_remote_message(sock_fd, message, strlen(message));
    
    // await for peer response (CONFIRM size\n)
    char response[256];
    syslog(LOG_INFO, "[local command handler] Awaiting response from peer");
    ssize_t bytes = recv_line_with_timeout(sock_fd, response, sizeof(response), 5);
    if (bytes < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to receive <CONFIRM SIZE> response");
        return -1;
    }
    if (strncmp(response, "CONFIRM", 7) != 0) {
        syslog(LOG_ERR, "[local command handler] Invalid <CONFIRM SIZE>response");
        return -1;
    }
    if(strncmp(response, "ERROR", 5) == 0) {
        syslog(LOG_ERR, "[local command handler] Error response from peer: %s", response);
        send_local_message(response, TRUE);
        return -1;
    }
    char *space = strchr(response, ' ');
    if (!space) {
        syslog(LOG_ERR, "[local command handler] Malformed <CONFIRM SIZE> response - no space found");
        return -1;
    }

    // Skip past the space
    space++;

    // Convert string to long
    char *endptr;
    errno = 0;  // Reset errno before calling strtol
    long size = strtol(space, &endptr, 10);

    // Check for conversion errors
    if (errno == ERANGE) {
        syslog(LOG_ERR, "[local command handler] <CONFIRM SIZE> Size value out of range");
        return -1;
    }
    if (endptr == space) {
        syslog(LOG_ERR, "[local command handler] <CONFIRM SIZE> No number found in response");
        return -1;
    }
    if (*endptr != '\n' && *endptr != '\0') {
        syslog(LOG_ERR, "[local command handler] <CONFIRM SIZE> Extra characters after number");
        return -1;
    }

    syslog(LOG_INFO, "[local command handler] <CONFIRM SIZE> message received! Size: %ld", size);

    // calcular o size-offset e verificar se ha memoria suficiente para receber o restante do arquivo
    syslog(LOG_INFO, "[local command handler] Calculating disk space for download");
    if (check_disk_space(final_path, (size - offset)) != 1) {
        send_remote_message(sock_fd, "ERROR No disk space\n", 14);
        send_local_message("ERROR No disk space\n", TRUE);
        syslog(LOG_ERR, "[local command handler] No disk space for download");
        close(sock_fd);
        return -1;
    }
    syslog(LOG_INFO, "[local command handler] Sending RDY message to peer");
    send_remote_message(sock_fd, "RDY\n", 4);

    char download_buffer[128];
    char local_message[256];
    ssize_t bytes_read;
    ssize_t total_bytes_read = 0;
    int initial_chunk = 128;
    int is_small = FALSE;

    if (size < 128) {
        is_small = TRUE;
    }

    int file_fd;
    if (offset == 0) {
        if (is_small) {
            syslog(LOG_INFO, "[local command handler] Creating immediate small file");
            // Criar arquivo diretamente sem .part
            file_fd = open(final_path, O_CREAT | O_WRONLY, 0666);
        } else {
            // creating the .part file
            syslog(LOG_INFO, "[local command handler] Initiating first chunk download of the .part file");
            while (total_bytes_read < initial_chunk) {
                bytes_read = recv(sock_fd, 
                            (download_buffer + total_bytes_read), 
                            (initial_chunk - total_bytes_read), 
                            0);
            
            if (bytes_read > 0) {
                total_bytes_read += bytes_read;
            }
            else if (bytes_read == 0) {
                // Connection closed by peer
                syslog(LOG_ERR, "[local command handler] Connection closed by peer before receiving initial chunk");
                return -1;
            }
            else {  // bytes_read < 0
                if (errno == EINTR) {
                    // Interrupted system call, try again
                    continue;
                }
                    syslog(LOG_ERR, "[local command handler] Error receiving initial chunk: %s", strerror(errno));
                    return -1;
                }
            }
            syslog(LOG_INFO, "[local command handler] Creating .part file %s", final_path);
            syslog(LOG_INFO, "[local command handler] Total bytes read (should be 128): %ld", total_bytes_read);
            send_local_message("Creating .part file...\n", FALSE);
            file_fd = open(part_path, O_CREAT | O_WRONLY, 0666);
            if (file_fd < 0) {
                syslog(LOG_ERR, "[local command handler] Failed to create .part file: %s", strerror(errno));
                return -1;
            }
            write(file_fd, download_buffer, total_bytes_read);
            offset = total_bytes_read;
            syslog(LOG_INFO, "[local command handler] .part file created");
            syslog(LOG_INFO, "[local command handler] Offset: %ld", offset);
        }
    } else {
        syslog(LOG_INFO, "[local command handler] Opening .part file and resuming download");
        file_fd = open(part_path, O_CREAT | O_WRONLY, 0666);
    }
    
    if (file_fd < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to open file: %s", strerror(errno));
        return -1;
    }
    

    lseek(file_fd, offset, SEEK_SET);
    send_local_message("\033[?25l", FALSE);
    int last_percentage = -1;
    
    while ((bytes_read = recv(sock_fd, download_buffer, sizeof(download_buffer), 0)) > 0) {
        write(file_fd, download_buffer, bytes_read);
        total_bytes_read += bytes_read;
        
        int current_percentage = (int)((100.0f * (total_bytes_read + offset)) / size);
        if (current_percentage != last_percentage) {
            snprintf(local_message, sizeof(local_message), 
                    "\rDOWNLOAD progress ... %d%%", 
                    current_percentage);
            send_local_message(local_message, FALSE);
            last_percentage = current_percentage;
        }
    }
    send_local_message("\033[?25h\n", FALSE);

    if (bytes_read < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to receive data from peer: %s", strerror(errno));
        send_local_message("\nERROR Failed to receive data from peer\n", TRUE);
        close(sock_fd);
        close(file_fd);
        return -1;
    }

    if (bytes_read == 0) {  // Connection closed by peer
        syslog(LOG_INFO, "[local command handler] Connection closed by peer");
        syslog(LOG_INFO, "total_bytes_read: %ld, offset: %ld, size: %ld", total_bytes_read, offset, size);
        if (total_bytes_read == size) {
            // Transfer complete
            close(file_fd);  // Close file before rename
            if (!is_small) {
                if (rename(part_path, final_path) != 0) {
                    syslog(LOG_ERR, "[local command handler] Failed to rename file: %s", strerror(errno));
                    send_local_message("\nERROR Failed to rename file\n", TRUE);
                    close(sock_fd);
                    return -1;
                }
            }
            syslog(LOG_INFO, "[local command handler] Download complete");
            send_local_message("\nDownload complete\n", TRUE);
        } else {
            // Incomplete transfer
            syslog(LOG_ERR, "[local command handler] Download incomplete. Remote server disconnected. Expected %ld bytes, got %ld", 
                size, total_bytes_read + offset);
            send_local_message("\nDownload incomplete. Remote server disconnected\n", TRUE);
            close(sock_fd);
            close(file_fd);
            return -1;
        }
    }

    close(sock_fd);
    return 0;
}

int handle_upload_command(char *local_path, char *ip, char *remote_path) {
    struct stat st; // file stats
    off_t offset; // offset to start uploading from
    char response[1024];

    syslog(LOG_INFO, "[local command handler] Handling UPLOAD command (local path: %s, remote IP: %s, remote path: %s)", local_path, ip, remote_path);

    if (stat(local_path, &st) != 0) {
        syslog(LOG_ERR, "[local command handler] File not found. Sending error response to client");
        snprintf(response, sizeof(response), "ERROR File not found\n");
        send_local_message(response, TRUE);
        return -1;
    }
    
    syslog(LOG_INFO, "[local command handler] File found. Connecting to peer %s", ip);
    int sock_fd = connect_to_peer(ip);
    if (sock_fd < 0) {
        return -1;
    }
    syslog(LOG_INFO, "[local command handler] Connected to peer %s", ip);

    syslog(LOG_INFO, "[local command handler] Sending PUT message to peer");
    off_t size = st.st_size;
    char message[PATH_MAX + 16];
    snprintf(message, sizeof(message), "PUT %s %ld\n", remote_path, size);
    send_remote_message(sock_fd, message, strlen(message));

    // await for peer response (CONFIRM offset\n)
    ssize_t bytes = recv_line_with_timeout(sock_fd, response, sizeof(response), 5);
    if (bytes < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to receive <CONFIRM OFFSET> response");
        return -1;
    }
    if (strncmp(response, "CONFIRM", 7) != 0) {
        syslog(LOG_ERR, "[local command handler] Invalid <CONFIRM OFFSET> response");
        return -1;
    }
    char *space = strchr(response, ' ');
    if (!space) {
        syslog(LOG_ERR, "[local command handler] Malformed <CONFIRM OFFSET> response - no space found");
        return -1;
    }
    space++;
    offset = strtol(space, NULL, 10);
    syslog(LOG_INFO, "[local command handler] <CONFIRM OFFSET> offset: %ld", offset);

    syslog(LOG_INFO, "[local command handler] Opening file %s", local_path);
    int file_fd = open(local_path, O_RDONLY);
    if (file_fd < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to open file: %s (handle_upload_command)", strerror(errno));
        return -1;
    }

    syslog(LOG_INFO, "[local command handler] Seeking to offset %ld", offset);
    lseek(file_fd, offset, SEEK_SET);

    // enviar o arquivo em partes de 1MB
    char upload_buffer[1048576];
    char local_message[256];
    ssize_t bytes_read;
    ssize_t total_bytes_read = 0;

    send_local_message("Uploading file...\n", FALSE);
    send_local_message("\033[?25l", FALSE);
    int last_percentage = -1;
    while ((bytes_read = read(file_fd, upload_buffer, sizeof(upload_buffer))) > 0) {
        send(sock_fd, upload_buffer, bytes_read, 0);
        total_bytes_read += bytes_read;
        
        int current_percentage = (int)((100.0f * (total_bytes_read + offset)) / size);
        if (current_percentage != last_percentage) {
            snprintf(local_message, sizeof(local_message), 
                    "\rUPLOAD progress ... %d%%", 
                    current_percentage);
            send_local_message(local_message, FALSE);
            fflush(stdout);
            last_percentage = current_percentage;
        }
    }
    send_local_message("\033[?25h\n", FALSE);

    if ((total_bytes_read + offset) != size) {
        syslog(LOG_ERR, "[local command handler] Upload incomplete. Expected %ld bytes, got %ld", size, total_bytes_read + offset);
        send_local_message("\nUpload incomplete. Remote server disconnected\n", TRUE);
        close(sock_fd);
        close(file_fd);
        return -1;
    } else {
        syslog(LOG_INFO, "[local command handler] Upload complete");
        close(sock_fd);
        close(file_fd);
        send_local_message("\nUpload complete\n", TRUE);
        return 0;
    }

}

int parse_command(char *command) {

    syslog(LOG_INFO, "[local command handler] Parsing command: %s", command);
    syslog(LOG_INFO, "[local command handler] Size of parsing command: %ld", strlen(command));

    char *newline = strchr(command, '\n');

    if (!newline) {
        send_local_message("Invalid command - no newline\n", TRUE);
        syslog(LOG_ERR, "[local command handler] Parsed invalid command - no newline");
        return -1;  // Invalid command - no newline
    }
    syslog(LOG_INFO, "[local command handler] Newline found");
    *newline = '\0';  // Temporarily remove newline for parsing

    if (strcmp(command, "ON?") == 0) {
        syslog(LOG_INFO, "[local command handler] Parsed ON? command");
        send_local_message("ON!\n", TRUE);
        return 0;
    }
    syslog(LOG_INFO, "[local command handler] Not ON? command");
    // QUIT command
    if (strcmp(command, "QUIT") == 0) {
        syslog(LOG_INFO, "[local command handler] Parsed QUIT command");
        send_local_message("Quiting REMCP server...\n", TRUE);
        cleanup();
        syslog(LOG_INFO, "[local command handler] Exiting REMCP server. Thanks for using!");
        exit(0);
    }
    syslog(LOG_INFO, "[local command handler] Not QUIT command");
    // STATS command (local and remote)
    if (strncmp(command, "STATS", 5) == 0) {
        if (strlen(command) == 5) {
            syslog(LOG_INFO, "[local command handler] Parsed STATS command (local)");
            handle_stats_command();
            return 0;
        } else {
            syslog(LOG_INFO, "[local command handler] Parsed STATS command (remote)");
            char *ip = command + 6;  // Skip "STATS "
            handle_remote_stats_command(ip);
            return 0;
        }
    }
    syslog(LOG_INFO, "[local command handler] Not STATS command");
    // For DOWNLOAD and UPLOAD, find the separator ':'
    char *colon = strchr(command, ':');
    if (!colon) {
        send_local_message(" [local command handler] Parsed invalid format (download/upload) - must have a colon somewhere\n", TRUE);
        return -1;  // Invalid format - must have a colon somewhere
    }
    syslog(LOG_INFO, "[local command handler] Colon found");
    // Split at the colon
    *colon = '\0';

    // Check what comes before the colon
    char *first_part = command + 4; // skips 'GET ' or 'PUT '
    char *second_part = colon + 1;

    // Find the space that separates the two main parts
    char *space;
    if (strncmp(command, "GET", 3) == 0) {
        // Format: IP:PATH PATH
        space = strchr(second_part, ' ');
        if (!space) {
            syslog(LOG_ERR, "[local command handler] Invalid GEET format - must have a space");
            send_local_message("Invalid GET format - must have a space\n", TRUE);
            return -1;  // Invalid format - must have a space
        }
        *space = '\0';
        char *ip = first_part;
        char *remote_path = second_part;
        char *local_path = space + 1;
        syslog(LOG_INFO, "[local command handler] DOWNLOAD command parsed");
        syslog(LOG_INFO, "[local command handler] Local path: %s, IP: %s, Remote path: %s", local_path, ip, remote_path);
        return handle_download_command(ip, remote_path, local_path);
    } else {
        space = strchr(first_part, ' ');
        if (!space) {
            syslog(LOG_ERR, "[local command handler] Invalid PUT format - must have a space");
            send_local_message("Invalid PUT format - must have a space\n", TRUE);
            return -1;  // Invalid format - must have a space
        }
        *space = '\0';
        char *local_path = first_part;
        char *ip = space + 1;
        char *remote_path = second_part;
        syslog(LOG_INFO, "[local command handler] UPLOAD command parsed");
        syslog(LOG_INFO, "[local command handler] Local path: %s, IP: %s, Remote path: %s", local_path, ip, remote_path);
        return handle_upload_command(local_path, ip, remote_path);
    }

    

    syslog(LOG_ERR, "[local command handler] Invalid command format");
    return -1;  // Invalid command format
}

void *process_local_command(void *arg) {
    char command_buffer[1024];
    char incoming_buffer[256];
    int command_pipe_fd;
    //int response_pipe_fd;
    

    for (;;) {
        // Open the command pipe for reading
        syslog(LOG_INFO, "[local command handler] Opening command pipe (read mode)");
        command_pipe_fd = open(COMMAND_PIPE, O_RDONLY);
        if (command_pipe_fd < 0) {
            syslog(LOG_ERR, "[local command handler] Failed to open command pipe: %s", strerror(errno));
            sleep(1);
            continue;
        }

        ssize_t bytes_read;
        ssize_t total_bytes_read = 0;

        // Read commands from the command pipe
        while ((bytes_read = read(command_pipe_fd, incoming_buffer, sizeof(incoming_buffer) - 1)) > 0) {
            incoming_buffer[bytes_read] = '\0';

            if (total_bytes_read + bytes_read >= sizeof(command_buffer)) {
                syslog(LOG_ERR, "[local command handler] Command buffer overflow");
                total_bytes_read = 0;
                memset(command_buffer, 0, sizeof(command_buffer));
                continue;
            }

            strncat(command_buffer, incoming_buffer, bytes_read);
            total_bytes_read += bytes_read;

            char *newline_pos = strchr(command_buffer, '\n');
            if (newline_pos) {
                size_t command_length = (newline_pos - command_buffer + 1);
                char temp_command[1024] = {0};
                strncpy(temp_command, command_buffer, command_length);
                temp_command[command_length] = '\0';

                syslog(LOG_INFO, "[local command handler] Received command: %s", temp_command);
                parse_command(temp_command);

                size_t remaining = total_bytes_read - command_length;
                memmove(command_buffer, command_buffer + command_length, remaining);
                command_buffer[remaining] = '\0';
                total_bytes_read = remaining;
            }
        }

        close(command_pipe_fd);
        //close(response_pipe_fd);
    }

    return NULL;
}


// ********** remote client operations **********

int send_remote_message(int sock_fd, const char *data, size_t len) {
    return send(sock_fd, data, len, 0);
}

ssize_t receive_remote_with_timeout(int sock_fd, char *buffer, size_t len, int timeout_sec) {
    struct timeval tv = {.tv_sec = timeout_sec, .tv_usec = 0};
    fd_set readfds;
    
    FD_ZERO(&readfds);
    FD_SET(sock_fd, &readfds);
    
    if (select(sock_fd + 1, &readfds, NULL, NULL, &tv) > 0) {
        return recv(sock_fd, buffer, len, 0);
    }
    return -1;  // Timeout
}

ssize_t recv_line_with_timeout(int sock_fd, char *buffer, size_t max_len, int timeout_sec) {
    size_t total_read = 0;
    struct timeval tv = {.tv_sec = timeout_sec, .tv_usec = 0};
    fd_set readfds;

    while (total_read < max_len - 1) {
        FD_ZERO(&readfds);
        FD_SET(sock_fd, &readfds);

        int ret = select(sock_fd + 1, &readfds, NULL, NULL, &tv);
        if (ret < 0) {
            // Error on select
            return -1;
        } else if (ret == 0) {
            // Timeout
            return -1; 
        }

        // If we got here, data is available to read
        ssize_t bytes_read = recv(sock_fd, &buffer[total_read], 1, 0);
        if (bytes_read < 0) {
            // recv error
            return -1;
        } else if (bytes_read == 0) {
            // Connection closed, if we read nothing, return 0
            break; 
        }

        if (buffer[total_read] == '\n') {
            total_read++;
            break; // Line complete
        }
        
        total_read++;
    }

    buffer[total_read] = '\0'; // Null-terminate the line
    return total_read;
}

HandshakeStatus initiate_handshake(int sock_fd) {
    char buffer[32];
    
    // Send REMCP?\n
    if (send_remote_message(sock_fd, HANDSHAKE_REQUEST, strlen(HANDSHAKE_REQUEST)) < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to send handshake request");
        return HS_FAILURE;
    }
    
    // Wait for YEP\n
    ssize_t bytes = recv_line_with_timeout(sock_fd, buffer, sizeof(buffer), HANDSHAKE_TIMEOUT);
    if (bytes < 0) {
        syslog(LOG_ERR, "[local command handler] Failed to receive handshake response from server");
        return HS_FAILURE;
    }

    if (bytes == 0) {
        syslog(LOG_ERR, "[local command handler] Connection closed by server");
        return HS_FAILURE;
    }
    
    if (strncmp(buffer, HANDSHAKE_RESPONSE, strlen(HANDSHAKE_RESPONSE)) != 0) {
        syslog(LOG_ERR, "[local command handler] Invalid handshake response from server");
        return HS_FAILURE;
    }

    if (strncmp(buffer, HANDSHAKE_RETRY, strlen(HANDSHAKE_RETRY)) == 0) {
        syslog(LOG_ERR, "[local command handler] Server busy. Retrying handshake");
        return HS_RETRY;
    }

    syslog(LOG_INFO, "[local command handler] Handshake successful");
    return HS_SUCCESS;  // Handshake successful
}

int accept_handshake(int sock_fd) {
    char buffer[32];
    
    // Wait for REMCP?\n
    ssize_t bytes = recv_line_with_timeout(sock_fd, buffer, sizeof(buffer), HANDSHAKE_TIMEOUT);
    if (bytes < 0) {
        syslog(LOG_ERR, "[main] Failed to receive handshake request");
        return -1;
    }

    if (bytes == 0) {
        syslog(LOG_ERR, "[main] Connection closed");
        return -1;
    }
    
    if (strncmp(buffer, HANDSHAKE_REQUEST, strlen(HANDSHAKE_REQUEST)) != 0) {
        syslog(LOG_ERR, "[main] Invalid handshake request");
        return -1;
    }
    
    // Send YEP\n
    if (send_remote_message(sock_fd, HANDSHAKE_RESPONSE, strlen(HANDSHAKE_RESPONSE)) < 0) {
        syslog(LOG_ERR, "[main] Failed to send handshake acceptance");
        return -1;
    }
    syslog(LOG_INFO, "[main] Handshake accepted");
    return 0;  // Handshake successful
}

int reject_handshake(int sock_fd) {
    char buffer[32];
    
    // Wait for REMCP?\n
    ssize_t bytes = recv_line_with_timeout(sock_fd, buffer, sizeof(buffer), HANDSHAKE_TIMEOUT);
    if (bytes < 0) {
        syslog(LOG_ERR, "[main] Failed to receive handshake request");
        return -1;
    }

    if (bytes == 0) {
        syslog(LOG_ERR, "[main] Connection closed");
        return -1;
    }
    
    if (strncmp(buffer, HANDSHAKE_REQUEST, strlen(HANDSHAKE_REQUEST)) != 0) {
        syslog(LOG_ERR, "[main] Invalid handshake request");
        return -1;
    }
    
    // Send TRYAGAIN\n
    if (send_remote_message(sock_fd, HANDSHAKE_RETRY, strlen(HANDSHAKE_RETRY)) < 0) {
        syslog(LOG_ERR, "[main] Failed to send handshake acceptance");
        return -1;
    }
    syslog(LOG_INFO, "[main] Handshake rejected");
    return 0;  // reject handshake successful
}

ssize_t read_line(int sock_fd, char *buffer, size_t max_len) {
    size_t total_read = 0;
    char c;
    ssize_t n;

    while (total_read < max_len - 1) {  // Leave space for null terminator
        n = recv(sock_fd, &c, 1, 0);    // Read one character at a time
        
        if (n < 0) {
            if (errno == EINTR) continue;  // Interrupted, try again
            return -1;                     // Error
        }
        if (n == 0) {                     // EOF
            break;
        }

        buffer[total_read++] = c;         // Store the character
        
        if (c == '\n') {                 // Found newline
            break;
        }
    }

    buffer[total_read] = '\0';           // Null terminate
    return total_read;
}

int connect_to_peer(char *ip) {
    syslog(LOG_INFO, "[local command handler] Running connect_to_peer() for peer %s", ip);
    int sock_fd;
    struct sockaddr_in server_addr;
    char response[256];
    const char *handshake = HANDSHAKE_REQUEST;
    int start_port = PORT;
    int max_attempts = MAX_PORT_ATTEMPTS;
    
    // Retry configuration
    const int max_retries = 3;
    const int initial_backoff_ms = 1000;  // Start with 1 second
    
    // Initialize socket address structure
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
        syslog(LOG_ERR, "[local command handler] Invalid IP address: %s (connect_to_peer)", ip);
        return -1;
    }

    // Try ports from start_port to start_port + max_attempts
    for (int port = start_port; port < start_port + max_attempts; port++) {
        syslog(LOG_INFO, "[local command handler] Trying ip: %s, port: %d", ip, port);
        int retry_count = 0;
        int backoff_ms = initial_backoff_ms;
        
        while (retry_count <= max_retries) {
            // Create socket
            sock_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (sock_fd < 0) {
                syslog(LOG_ERR, "[local command handler] Socket creation failed: %s (connect_to_peer)", strerror(errno));
                return -1;
            }

            // Set port
            server_addr.sin_port = htons(port);

            int flags = fcntl(sock_fd, F_GETFL, 0);
            fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);
            int ret = connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
            if (ret < 0 && errno != EINPROGRESS) {
                syslog(LOG_INFO, "[local command handler] connect() failed to peer %s, port %d", ip, port);
                close(sock_fd);
                break;  // Try next port
            }
            struct timeval tv = {.tv_sec = 2, .tv_usec = 0};
            fd_set writefds;
            FD_ZERO(&writefds);
            FD_SET(sock_fd, &writefds);

            char msg_buffer[256];
            int len = snprintf(msg_buffer, sizeof(msg_buffer), "Connecting to peer %s:%d\n", ip, port);
            if (len > sizeof(msg_buffer)) {
                syslog(LOG_ERR, "[local command handler] Message buffer in connect_to_peer() overflow");
                close(sock_fd);
                break;
            }
            send_local_message(msg_buffer, FALSE);

            int select_result = select(sock_fd + 1, NULL, &writefds, NULL, &tv);
            if (select_result > 0) {
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
                    // Connection failed
                    close(sock_fd);
                    break;
                }
                int flags = fcntl(sock_fd, F_GETFL, 0);
                fcntl(sock_fd, F_SETFL, flags & ~O_NONBLOCK);
                
                syslog(LOG_INFO, "[local command handler] Connection successful to peer %s, port %d", ip, port);
            }
            /*// Try to connect
            syslog(LOG_INFO, "[local command handler] running connect() to peer %s, port %d", ip, port);
            if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
                syslog(LOG_INFO, "[local command handler] connect() failed to peer %s, port %d", ip, port);
                close(sock_fd);
                break;  // Try next port
            }
            */
           syslog(LOG_INFO, "[local command handler] Initializing handshake with peer %s, port %d", ip, port);
            HandshakeStatus handshake_status = initiate_handshake(sock_fd);
           
            if (handshake_status == HS_FAILURE) {
                close(sock_fd);
                break;
            }
            else if(handshake_status == HS_SUCCESS){
                return sock_fd;
            } else if (handshake_status == HS_RETRY) {
                close(sock_fd);
                if (retry_count < max_retries) {
                    // Log retry attempt
                    syslog(LOG_INFO, "[local command handler] Server busy on port %d, retry %d/%d after %d ms", 
                           port, retry_count + 1, max_retries, backoff_ms);
                    char msg_buffer[256];
                    snprintf(msg_buffer, sizeof(msg_buffer), "[local command handler] Server busy on port %d, retry %d/%d after %d ms\n", 
                           port, retry_count + 1, max_retries, backoff_ms);
                    send_local_message(msg_buffer, FALSE);
                    
                    // Sleep with exponential backoff
                    usleep(backoff_ms * 1000);  // Convert ms to microseconds
                    
                    // Exponential backoff with some randomization
                    backoff_ms *= 2;
                    backoff_ms += rand() % 1000;  // Add up to 1 second of jitter
                    
                    retry_count++;
                    continue;

                } else {
                    syslog(LOG_INFO, "[local command handler] Server busy on port %d, max retries exceeded", port);
                    break;  // Try next port
                }
            }

            // Invalid response
            close(sock_fd);
            break;  // Try next port
        }
    }

    syslog(LOG_ERR, "[local command handler] Failed to connect to remote server after trying all ports (connect_to_peer)");
    return -1;
}

int handle_stat(int sock_fd, int local) {
    char response[256];
    int len;
    int current_requests;
    int divisor;

    syslog(LOG_INFO, "[local command handler] Handling stat");
    syslog(LOG_INFO, "[local command handler] Securing mutex");
    pthread_mutex_lock(&current_sim_requests_mutex);
    syslog(LOG_INFO, "[local command handler] Mutex secured");
    syslog(LOG_INFO, "[local command handler] Getting current_sim_requests: %d", current_sim_requests);
    current_requests = current_sim_requests;
    syslog(LOG_INFO, "[local command handler] Unlocking mutex");
    pthread_mutex_unlock(&current_sim_requests_mutex);

    syslog(LOG_INFO, "[local command handler] Local? %d", local);
    if (!local) {
        current_requests = current_requests - 1; // -1 because we are not counting the current stat request
    }

    if (current_requests == 0) {
        divisor = 1;
    } else {
        divisor = current_requests;
    }
    
    len = snprintf(response, sizeof(response), 
        "Current remote Requests: %d\n"
        "Max Simultaneous: %d\n"
        "Max Transfer Rate: %d bytes/sec\n"
        "Current Transfer Rate: %d bytes/sec\n",
        current_requests, 
        THREAD_POOL_SIZE,
        MAX_TRANSFER_RATE,
        (MAX_TRANSFER_RATE / divisor));
    
    if (len < 0 || len >= sizeof(response)) {
        syslog(LOG_ERR, "Stats message truncated");
        return -1;
    }
    if (!local) {
        syslog(LOG_INFO, "[remote command handler] Sending stat response to remote");
        ssize_t bytes_sent = send(sock_fd, response, len, 0);
        if (bytes_sent < 0) {
            syslog(LOG_ERR, "[remote command handler] Failed to send stat response: %s handle_stat", strerror(errno));
            return -1;
        }
    } else {
        syslog(LOG_INFO, "[local command handler] Sending stat response to local");
        send_local_message(response, TRUE);
    }
    return 0;
}

int handle_get(int sock_fd, char *path, off_t offset) {
    struct stat file_stat;
    char response[256];
    char buffer[8192];
    struct timespec start_time, end_time;
    long elapsed_usec;
    
    // Check if file exists and get its info
    if (stat(path, &file_stat) != 0) {
        syslog(LOG_ERR, "[remote command handler] File not found. Sending error response to client");
        snprintf(response, sizeof(response), "ERROR File not found\n");
        send(sock_fd, response, strlen(response), 0);
        return -1;
    }
    
    // Send confirmation with file size
    syslog(LOG_INFO, "[remote command handler] File found. Sending confirmation with file size to client");
    snprintf(response, sizeof(response), "CONFIRM %ld\n", file_stat.st_size);
    if (send(sock_fd, response, strlen(response), 0) <= 0) {
        return -1;
    }
    
    // Wait for client ready signal
    syslog(LOG_INFO, "[remote command handler] Waiting for client ready signal");
    char client_response[32];
    ssize_t bytes_read = read_line(sock_fd, client_response, sizeof(client_response));
    if (bytes_read <= 0 || strncmp(client_response, "RDY\n", 4) != 0) {
        if (bytes_read > 0) {
            syslog(LOG_ERR, "[remote command handler] Invalid client response: %s", client_response);
        }
        return -1;
    }
    syslog(LOG_INFO, "[remote command handler] Client ready signal received");
    
    // Open and send file
    syslog(LOG_INFO, "[remote command handler] Opening file %s", path);
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        snprintf(response, sizeof(response), "ERROR Cannot open file\n");
        send(sock_fd, response, strlen(response), 0);
        return -1;
    }
    syslog(LOG_INFO, "[remote command handler] File opened successfully");

    if (offset != 0) {
        if (lseek(fd, offset, SEEK_SET) == -1) {
            syslog(LOG_ERR, "Failed to seek to offset %ld: %s", offset, strerror(errno));
            close(fd);
            return -1;
        }
        syslog(LOG_INFO, "[remote command handler] Seeking to offset %ld", offset);
    }
    
    // Send file data with rate limiting
    syslog(LOG_INFO, "[remote command handler] Sending file data with rate limiting");
    while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0) {
        // Get start time
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        
        // Send data
        ssize_t bytes_sent = send(sock_fd, buffer, bytes_read, 0);
        if (bytes_sent < 0) {
            syslog(LOG_ERR, "[remote command handler] Failed to send file data: %s", strerror(errno));
            close(fd);
            return -1;
        }
        syslog(LOG_INFO, "[remote command handler] Sent %ld bytes of file data", bytes_sent);
        
        // Calculate how long this should take at max_transfer_rate
        pthread_mutex_lock(&current_sim_requests_mutex);
        int current_requests = current_sim_requests;
        pthread_mutex_unlock(&current_sim_requests_mutex);
        //syslog(LOG_INFO, "[remote command handler] Current transfer rate: %d bytes/sec", (MAX_TRANSFER_RATE / current_requests));
        long expected_usec = (bytes_sent * 1000000L) / (MAX_TRANSFER_RATE / current_requests);
        
        // Get actual elapsed time
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        elapsed_usec = (end_time.tv_sec - start_time.tv_sec) * 1000000 +
                      (end_time.tv_nsec - start_time.tv_nsec) / 1000;
        
        // Sleep if we're going too fast
        if (elapsed_usec < expected_usec) {
            usleep(expected_usec - elapsed_usec);
        }
    }

    syslog(LOG_INFO, "[remote command handler] File transfer completed");
    
    close(fd);
    return 0;
}

int handle_put(int sock_fd, char *path, ssize_t size) {
    off_t offset; // offset to start downloading from
    struct stat st; // file stats
    int counter = 0; // counter to handle file name collisions
    char *ext_text; // file extension text
    char final_path[PATH_MAX]; // final path
    char base_path[PATH_MAX]; // base path
    char part_path[PATH_MAX]; // .part file path
    char *extension; // file extension

    syslog(LOG_INFO, "[remote command handler] Parsing local path %s", path);
    strncpy(base_path, path, PATH_MAX - 1);
    extension = strrchr(base_path, '.');
    if (extension) {
        ext_text = extension + 1;
        *extension = '\0';  // Temporarily split path and extension
    }
    do {
        if (counter == 0) {
            snprintf(final_path, PATH_MAX, "%s%s%s", base_path, 
                    ext_text ? "." : "",      // Add the dot back if there's an extension
                    ext_text ? ext_text : "");
        } else {
            int len = snprintf(final_path, PATH_MAX, "%s_%d%s%s", base_path, 
                    counter,
                    ext_text ? "." : "",      // Add the dot back if there's an extension
                    ext_text ? ext_text : "");
            if (len >= PATH_MAX) {
                syslog(LOG_ERR, "[remote command handler] Path length exceeds PATH_MAX");
                return -1;
            }
        }
        counter++;
    } while (stat(final_path, &st) == 0 && counter < 1000);

    int len = snprintf(part_path, PATH_MAX, "%s.part", final_path);
    if (len >= PATH_MAX) {
        syslog(LOG_ERR, "[remote command handler] Path length exceeds PATH_MAX");
        return -1;
    }

    char dir_path[PATH_MAX];
    strncpy(dir_path, final_path, PATH_MAX);
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash) {
        *last_slash = '\0';  // Trunca no último '/' para ter só o path do diretório
        
        // Verifica se diretório existe
        struct stat st = {0};
        if (stat(dir_path, &st) == -1) {
            syslog(LOG_INFO, "[remote command handler] Directory does not exist, creating: %s", dir_path);
            
            // Criar diretório recursivamente
            char tmp[PATH_MAX];
            char *p = NULL;
            
            snprintf(tmp, sizeof(tmp), "%s", dir_path);
            for (p = tmp + 1; *p; p++) {
                if (*p == '/') {
                    *p = 0;
                    mkdir(tmp, 0755);
                    *p = '/';
                }
            }
            mkdir(tmp, 0755);
            
            // Verificar se criação foi bem sucedida
            if (stat(dir_path, &st) == -1) {
                syslog(LOG_ERR, "[remote command handler] Failed to create directory: %s", strerror(errno));
                return -1;
            }
        }
    }

    syslog(LOG_INFO, "[remote command handler] Final path: %s", final_path);
    syslog(LOG_INFO, "[remote command handler] Base path: %s", base_path);
    syslog(LOG_INFO, "[remote command handler] Extension: %s", ext_text);
    syslog(LOG_INFO, "[remote command handler] .part file path: %s", part_path);

    syslog(LOG_INFO, "[remote command handler] Checking if .part file exists");
    if (stat(part_path, &st) == 0) {
        syslog(LOG_INFO, "[remote command handler] .part file exists.Offset: %ld", offset);
        offset = st.st_size;  // Resume from end of partial file
    }
    else {
        offset = 0;
        syslog(LOG_INFO, "[remote command handler] .part file does not exist.Offset: %ld", offset);
    }
    
    // Send CONFIRM with offset
    syslog(LOG_INFO, "[remote command handler] Sending CONFIRM with offset: %ld", offset);
    char response[256];
    snprintf(response, sizeof(response), "CONFIRM %ld\n", offset);
    if (send(sock_fd, response, strlen(response), 0) <= 0) {
        syslog(LOG_ERR, "[remote command handler] Failed to send CONFIRM: %s", strerror(errno));
        return -1;
    }

    char download_buffer[128];
    char local_message[256];
    ssize_t bytes_read;
    ssize_t total_bytes_read = 0;
    int initial_chunk = 128;
    int is_small = FALSE;

    if (size < 128) {
        is_small = TRUE;
    }

    int file_fd;
    if (offset == 0) {
        if (is_small) {
            syslog(LOG_INFO, "[remote command handler] Creating immediate small file");
            // Criar arquivo diretamente sem .part
            file_fd = open(final_path, O_CREAT | O_WRONLY, 0666);
        } else {
            // creating the .part file
            syslog(LOG_INFO, "[remote command handler] Initiating first chunk upload of the .part file");
            while (total_bytes_read < initial_chunk) {
                bytes_read = recv(sock_fd, 
                            (download_buffer + total_bytes_read), 
                            (initial_chunk - total_bytes_read), 
                            0);
            
            if (bytes_read > 0) {
                total_bytes_read += bytes_read;
            }
            else if (bytes_read == 0) {
                // Connection closed by peer
                syslog(LOG_ERR, "[remote command handler] Connection closed by peer before receiving initial chunk");
                return -1;
            }
            else {  // bytes_read < 0
                if (errno == EINTR) {
                    // Interrupted system call, try again
                    continue;
                }
                    syslog(LOG_ERR, "[remote command handler] Error receiving initial chunk: %s", strerror(errno));
                    return -1;
                }
            }
            syslog(LOG_INFO, "[remote command handler] Creating .part file %s", final_path);
            syslog(LOG_INFO, "[remote command handler] Total bytes read (should be 128): %ld", total_bytes_read);
            file_fd = open(part_path, O_CREAT | O_WRONLY, 0666);
            if (file_fd < 0) {
                syslog(LOG_ERR, "[remote command handler] Failed to create .part file: %s", strerror(errno));
                return -1;
            }
            write(file_fd, download_buffer, total_bytes_read);
            offset = total_bytes_read;
            syslog(LOG_INFO, "[remote command handler] .part file created");
            syslog(LOG_INFO, "[remote command handler] Offset: %ld", offset);
        }
    } else {
        syslog(LOG_INFO, "[remote command handler] Opening .part file and resuming download");
        file_fd = open(part_path, O_CREAT | O_WRONLY, 0666);
    }
    
    if (file_fd < 0) {
        syslog(LOG_ERR, "[remote command handler] Failed to open file: %s", strerror(errno));
        return -1;
    }
    

    lseek(file_fd, offset, SEEK_SET);
    struct timespec start_time, end_time;
    long elapsed_usec;
    
    while ((bytes_read = recv(sock_fd, download_buffer, sizeof(download_buffer), 0)) > 0) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        write(file_fd, download_buffer, bytes_read);
        
        // Rate limiting
        pthread_mutex_lock(&current_sim_requests_mutex);
        int current_requests = current_sim_requests;
        pthread_mutex_unlock(&current_sim_requests_mutex);
        long expected_usec = (bytes_read * 1000000L) / (MAX_TRANSFER_RATE / current_requests);
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        elapsed_usec = (end_time.tv_sec - start_time.tv_sec) * 1000000 +
                      (end_time.tv_nsec - start_time.tv_nsec) / 1000;
        
        if (elapsed_usec < expected_usec) {
            usleep(expected_usec - elapsed_usec);
        }
        total_bytes_read += bytes_read;
    }

    if (bytes_read < 0) {
        syslog(LOG_ERR, "[remote command handler] Failed to receive data from peer: %s", strerror(errno));
        close(sock_fd);
        close(file_fd);
        return -1;
    }

    if (bytes_read == 0) {  // Connection closed by peer
        syslog(LOG_INFO, "[remote command handler] Connection closed by peer");
        syslog(LOG_INFO, "[remote command handler] total_bytes_read: %ld, offset: %ld, size: %ld", total_bytes_read, offset, size);
        if (total_bytes_read == size) {
            // Transfer complete
            close(file_fd);  // Close file before rename
            if (!is_small) {
                if (rename(part_path, final_path) != 0) {
                    syslog(LOG_ERR, "[remote command handler] Failed to rename file: %s", strerror(errno));
                    close(sock_fd);
                    return -1;
                }
            }
            syslog(LOG_INFO, "[remote command handler] Upload complete");
        } else {
            // Incomplete transfer
            syslog(LOG_ERR, "[remote command handler] Upload incomplete. Remote server disconnected. Expected %ld bytes, got %ld", 
                size, total_bytes_read + offset);
            close(sock_fd);
            close(file_fd);
            return -1;
        }
    }

    close(sock_fd);
    return 0;
}

struct Request parse_request(const char* input) {
    struct Request request = {CMD_INVALID, ""};  // Initialize with defaults
    syslog(LOG_INFO, "[remote command handler] Parsing request: /%s/", input);
    syslog(LOG_INFO, "[remote command handler] Request length: %ld", strlen(input));
    
    // Handle STAT command (simplest case)
    if (strncmp(input, "STATS\n", 5) == 0) {
        syslog(LOG_INFO, "[remote command handler] Parsed STATS request");
        request.type = CMD_STATS;
        return request;
    }
    
    // For GET and PUT, find the space between command and path
    char* space = strchr(input, ' ');
    if (space == NULL) {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid format");
        return request;  // Invalid format
    }
    
    // Get command length
    size_t cmd_len = space - input;
    
    // Check command type
    if (cmd_len == 3 && strncmp(input, "GET", 3) == 0) {
        syslog(LOG_INFO, "[remote command handler] Parsed GET request");
        request.type = CMD_GET;
    } else if (cmd_len == 3 && strncmp(input, "PUT", 3) == 0) {
        syslog(LOG_INFO, "[remote command handler] Parsed PUT request");
        request.type = CMD_PUT;
    } else {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid command");
        return request;  // Invalid command
    }
    
    // Copy path (skip space, copy until newline)
    space++;  // Skip the space
    char* next_space = strchr(space, ' ');
    if (next_space == NULL) {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid format (no newline)");
        request.type = CMD_INVALID;  // No newline found
        return request;
    }
    
    size_t path_len = next_space - space;
    if (path_len >= PATH_MAX) {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid format (path too long)");
        request.type = CMD_INVALID;  // Path too long
        return request;
    }
    syslog(LOG_INFO, "[remote command handler] Parsed path: %s", space);
    strncpy(request.path, space, path_len);
    request.path[path_len] = '\0';  // Null terminate

    next_space++;
    // Pointer that will mark where strtol stopped parsing
    char *endptr;
    errno = 0;  // Reset errno before calling strtol
    
    // Convert string to long integer
    long offset = strtol(next_space, &endptr, 10);
    
    // Check if we actually reached the \n
    if (*endptr != '\n') {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid format (strtol() didnt reach newline)");
        request.type = CMD_INVALID;  // No newline after number
        return request;
    }
    
    // Check for conversion errors
    if (errno == ERANGE) {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid format (number out of range)");
        request.type = CMD_INVALID;  // Number out of range
        return request;
    }
    if (endptr == next_space) {
        
        request.type = CMD_INVALID;  // No number found
        return request;
    }
    if (offset < 0) {
        syslog(LOG_INFO, "[remote command handler] Parsed (get/put) Invalid format (negative offset)");
        request.type = CMD_INVALID;  // Negative offset not allowed
        return request;
    }

    if (request.type == CMD_PUT) {
        syslog(LOG_INFO, "[remote command handler] Parsed PUT request with file size: %ld", offset);
        request.size = offset;
        request.offset = 0;
    } else {
        syslog(LOG_INFO, "[remote command handler] Parsed GET request with offset: %ld", offset);
        request.offset = offset;
        request.size = 0;
    }

    syslog(LOG_INFO, "[remote command handler] Returning request type: %d", request.type);
    syslog(LOG_INFO, "[remote command handler] Returning request path: %s", request.path);
    syslog(LOG_INFO, "[remote command handler] Returning request with offset (GET): %ld", request.offset);
    syslog(LOG_INFO, "[remote command handler] Returning request with file size (PUT): %ld", request.size);
    return request;
}

int handle_request(struct Request request, int sock_fd){
    syslog(LOG_INFO, "[remote command handler] Handling request type: %d", request.type);
    syslog(LOG_INFO, "[remote command handler] Handling request path: %s", request.path);
    syslog(LOG_INFO, "[remote command handler] Handling request offset: %ld", request.offset);

    switch(request.type) {
        case CMD_STATS:
            syslog(LOG_INFO, "[remote command handler] Handling STATS request");
            handle_stat(sock_fd, REMOTE);
            break;
            
        case CMD_GET:
            syslog(LOG_INFO, "[remote command handler] Handling GET request");
            handle_get(sock_fd, request.path, request.offset);
            break;
            
        case CMD_PUT:
            syslog(LOG_INFO, "[remote command handler] Handling PUT request");
            handle_put(sock_fd, request.path, request.size);
            break;
        default:
            break;
    }
    return 0;
}

void *process_request(void *arg) {
    while(1) {
        // Wait for work to be available
        sem_wait(&request_available);

        int sock_fd = -1;
        pthread_mutex_lock(&request_buffer_mutex);
        if (buffer_count > 0) {
            sock_fd = request_buffer[0];
            // Shift remaining requests
            for(int i = 0; i < buffer_count - 1; i++) {
                request_buffer[i] = request_buffer[i + 1];
            }
            buffer_count--;
        }
        pthread_mutex_unlock(&request_buffer_mutex);

        syslog(LOG_INFO, "[remote command handler] sock_fd: %d", sock_fd);

        if (sock_fd != -1) {
            
            // Read the actual request
            syslog(LOG_INFO, "[remote command handler] Reading request sent from client");
            char request_buffer[1024];
            ssize_t bytes = recv_line_with_timeout(sock_fd, request_buffer, sizeof(request_buffer), HANDSHAKE_TIMEOUT);
            if (bytes <= 0) {
                syslog(LOG_ERR, "[remote command handler] Failed to read request");
                close(sock_fd);
                continue;
            }

            // Process the request
            struct Request request = parse_request(request_buffer);
            syslog(LOG_INFO, "[remote command handler] Accessing current_sim_requests (mutex lock)");
            pthread_mutex_lock(&current_sim_requests_mutex);
            syslog(LOG_INFO, "[remote command handler] Secured mutex lock");
            current_sim_requests++;
            syslog(LOG_INFO, "[remote command handler] Incremented current_sim_requests");
            pthread_mutex_unlock(&current_sim_requests_mutex);
            syslog(LOG_INFO, "[remote command handler] Released mutex lock");

            syslog(LOG_INFO, "[remote command handler] Handling request");
            handle_request(request, sock_fd);
            close(sock_fd);

            syslog(LOG_INFO, "[remote command handler] Accessing current_sim_requests (mutex lock)");
            pthread_mutex_lock(&current_sim_requests_mutex);
            syslog(LOG_INFO, "[remote command handler] Secured mutex lock");
            current_sim_requests--;
            syslog(LOG_INFO, "[remote command handler] Decremented current_sim_requests");
            pthread_mutex_unlock(&current_sim_requests_mutex);
            syslog(LOG_INFO, "[remote command handler] Released mutex lock");
            
        }
    }
    return NULL;
}

// ***** driver function *****

int main(int argc, char **argv){

    if (sem_init(&request_available, 0, 0) != 0) {
        syslog(LOG_ERR, "[main] Failed to initialize semaphore: %s", strerror(errno));
    exit(EXIT_FAILURE);
    }
    
    daemonize();

    if (create_pipes() != 0) {
        exit(EXIT_FAILURE);
    }

    if (start_local_command_thread() != 0) {
        exit(EXIT_FAILURE);
    }

    if (create_thread_pool(THREAD_POOL_SIZE, thread_pool) != 0) {
        exit(EXIT_FAILURE);
    }

    start_server();

    // main loop
    receive_connections();


    return 0;
}