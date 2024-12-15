#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <linux/limits.h>
#include <time.h>
#include <sys/select.h>

/**
 * 
 * REMCP CLIENT - remote copy client 
 * by Vinicius Guazzelli Dias - 2024
 * 
 */


#define COMMAND_PIPE "/tmp/remcp_command_pipe"
#define RESPONSE_PIPE "/tmp/remcp_response_pipe"
#define FALSE 0
#define TRUE 1
#define TIMEOUT 2

int response_pipe_fd = -1;
int command_pipe_fd = -1;

typedef enum {
    DOWNLOAD,
    UPLOAD,
    STATS,
    QUIT
} CommandType;

typedef struct {
    CommandType type;
    char command[16];  // Fixed-size buffer for command
    char ip[INET_ADDRSTRLEN];  // Fixed-size buffer for IP
    char local_path[PATH_MAX];  // Fixed-size buffer for path
    char remote_path[PATH_MAX];  // Fixed-size buffer for path
    int is_valid; // 0 if invalid, 1 if valid
} Command;

Command init_command() {
    Command cmd = {
        .type = DOWNLOAD,
        .command = {0},    // Zero initialize arrays
        .ip = {0},
        .local_path = {0},
        .remote_path = {0},
        .is_valid = TRUE
    };
    return cmd;
}

int check_pipe_exists() {
    struct stat st;
    if (stat(COMMAND_PIPE, &st) < 0) {
        if (errno == ENOENT) {
            fprintf(stderr, "Error: Command pipe does not exist.\n");
            fprintf(stderr, "Make sure the REMCP server is running.\n");
            return 0;
        } else {
            fprintf(stderr, "Error accessing pipe: %s\n", strerror(errno));
            return 0;
        }
    }
    return 1;
}

int attach_to_pipes() {
    response_pipe_fd = open(RESPONSE_PIPE, O_RDONLY);
    command_pipe_fd = open(COMMAND_PIPE, O_WRONLY);
    if (response_pipe_fd < 0 || command_pipe_fd < 0) {
        return 0;
    }
    return 1;
}

int check_server_running() {
    // Open pipe for writing
    int write_fd = open(COMMAND_PIPE, O_WRONLY);
    if (write_fd < 0) {
        fprintf(stderr, "Error: Failed to open command pipe for writing: %s\n", strerror(errno));
        return 0;
    }

    // Send the check message
    write(write_fd, "ON?\n", 4);
    close(write_fd);  // Close write end

    // Open pipe for reading
    int read_fd = open(RESPONSE_PIPE, O_RDONLY);
    if (read_fd < 0) {
        fprintf(stderr, "Error: Failed to open response pipe for reading: %s\n", strerror(errno));
        return 0;
    }

    // Read response
    char buffer[256];
    int bytes_read = read(read_fd, buffer, sizeof(buffer));
    close(read_fd);  // Close read end

    if (bytes_read <= 0) {
        fprintf(stderr, "Error: No response from server\n");
        return 0;
    }

    buffer[bytes_read] = '\0';
    if (strncmp(buffer, "ON!", 3) == 0) {
        printf("Server is running.\n");
        return 1;
    }

    printf("Server is not running.\n");
    return 0;
}

void print_usage() {
    printf("\n");
    printf("*****         REMCP Usage         *****\n");
    printf("\n");
    printf("DOWNLOAD FILE:       remcp IP:PATH PATH\n");
    printf("UPLOAD FILE:         remcp PATH IP:PATH\n");
    printf("LOCAL SERVER STATS:  remcp stats\n");
    printf("REMOTE SERVER STATS: remcp stats IP\n");
    printf("SHUTDOWN SERVER:     remcp quit\n");
    printf("\n");
}

Command parse_commands(int argc, char **argv) {
    Command cmd = init_command();
    if (argc < 2 || argc > 3) {
        print_usage();
        cmd.is_valid = FALSE;
        return cmd;
    }
    if (argc == 2) {
        // local stats command
        if (strcmp(argv[1], "stats") == 0) {
            cmd.type = STATS;
            return cmd;
        }
        // quit command
        if (strcmp(argv[1], "quit") == 0) {
            cmd.type = QUIT;
            return cmd;
        }
    }
    if (argc == 3) {
        // remote stats command
        if (strcmp(argv[1], "stats") == 0) {
            cmd.type = STATS;
            if (sizeof(argv[2]) >= INET_ADDRSTRLEN) {
                fprintf(stderr, "Error: IP address is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.ip, sizeof(cmd.ip), "%s", argv[2]);
            return cmd;
        }
        // download command
        if (strchr(argv[1], ':') != NULL) {
            char *ip = strtok(argv[1], ":");
            char *remote_path = strtok(NULL, ":");
            if (remote_path[0] != '/') {
                fprintf(stderr, "Error: Remote Path must start with '/'\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            cmd.type = DOWNLOAD;

            if (sizeof(ip) >= INET_ADDRSTRLEN) {
                fprintf(stderr, "Error: IP address is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.ip, sizeof(cmd.ip), "%s", ip);

            if (sizeof(remote_path) >= PATH_MAX) {
                fprintf(stderr, "Error: Remote Path is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.remote_path, sizeof(cmd.remote_path), "%s", remote_path);

            if (sizeof(argv[2]) >= PATH_MAX) {
                fprintf(stderr, "Error: Local Path is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.local_path, sizeof(cmd.local_path), "%s", argv[2]);
            return cmd;
        }
        // upload command
        if (strchr(argv[2], ':') != NULL) {
            char *ip = strtok(argv[2], ":");
            char *path = strtok(NULL, ":");
            if (path[0] != '/') {
                fprintf(stderr, "Error: Remote Path must start with '/'\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            cmd.type = UPLOAD;

            if (sizeof(ip) >= INET_ADDRSTRLEN) {
                fprintf(stderr, "Error: IP address is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.ip, sizeof(cmd.ip), "%s", ip);

            if (sizeof(path) >= PATH_MAX) {
                fprintf(stderr, "Error: Remote Path is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.remote_path, sizeof(cmd.remote_path), "%s", path);

            if (sizeof(argv[1]) >= PATH_MAX) {
                fprintf(stderr, "Error: Local Path is too long.\n");
                cmd.is_valid = FALSE;
                return cmd;
            }
            snprintf(cmd.local_path, sizeof(cmd.local_path), "%s", argv[1]);
            return cmd;
        }

    }
    print_usage();
    cmd.is_valid = FALSE;
    return cmd;
}

int wait_for_response(int timeout_seconds) {
    char response_buffer[256];
    int response_fd = open(RESPONSE_PIPE, O_RDONLY);
    if (response_fd < 0) {
        fprintf(stderr, "Error: Failed to open response pipe: %s\n", strerror(errno));
        return 0;
    }

    // Setup select() timeout
    fd_set readfds;
    struct timeval tv;
    tv.tv_sec = timeout_seconds;
    tv.tv_usec = 0;

    while (1) {
        FD_ZERO(&readfds);
        FD_SET(response_fd, &readfds);

        int ready = select(response_fd + 1, &readfds, NULL, NULL, &tv);
        
        if (ready < 0) {
            if (errno == EINTR) continue;  // Interrupted, try again
            fprintf(stderr, "Error in select(): %s\n", strerror(errno));
            close(response_fd);
            return 0;
        }
        
        if (ready == 0) {
            fprintf(stderr, "Timeout waiting for response\n");
            close(response_fd);
            return 0;
        }

        // Data is available
        ssize_t read_bytes = read(response_fd, response_buffer, sizeof(response_buffer) - 1);
        if (read_bytes > 0) {
            response_buffer[read_bytes] = '\0';
            printf("Response:\n%s\n", response_buffer);
            close(response_fd);
            return 1;
        }
        else if (read_bytes == 0) {
            // Pipe closed
            fprintf(stderr, "Server closed pipe\n");
            close(response_fd);
            return 0;
        }
        else {
            if (errno == EINTR) continue;
            fprintf(stderr, "Error reading response: %s\n", strerror(errno));
            close(response_fd);
            return 0;
        }
    }
}

int send_message(char *msg) {
    int pipe_fd = open(COMMAND_PIPE, O_WRONLY);
    if (pipe_fd < 0) {
        fprintf(stderr, "Error: Failed to open command pipe: %s\n", strerror(errno));
        return 0;
    }
    char display_buffer[(PATH_MAX * 2) + INET_ADDRSTRLEN + 16];
    int len = snprintf(display_buffer, sizeof(display_buffer), "Sending message: %s\n", msg);
    if (len > sizeof(display_buffer)) {
        fprintf(stderr, "Error: Display buffer is too small\n");
        return 0;
    }
    char *newline = strchr(display_buffer, '\n');
    if (newline != NULL) {
        *newline = '\0';
    }
    printf("%s\n", display_buffer);
    write(pipe_fd, msg, strlen(msg));
    close(pipe_fd);

    char response_buffer[256];
    int response_fd = open(RESPONSE_PIPE, O_RDONLY);
    if (response_fd < 0) {
        fprintf(stderr, "Error: Failed to open response pipe: %s\n", strerror(errno));
        return 0;
    }

    // Read until pipe is closed
    while (1) {
        ssize_t read_bytes = read(response_fd, response_buffer, sizeof(response_buffer) - 1);
        
        if (read_bytes > 0) {
            // Got data
            response_buffer[read_bytes] = '\0';
            printf("%s", response_buffer);  // Print without extra newlines
            memset(response_buffer, 0, sizeof(response_buffer));
        }
        else if (read_bytes == 0) {
            // Pipe closed by server
            break;
        }
        else {
            // Error
            if (errno == EINTR) continue;  // Interrupted, try again
            fprintf(stderr, "Error reading from pipe: %s\n", strerror(errno));
            break;
        }
    }

    close(response_fd);

    // tentando superar
    /*
    char response_buffer[256];
    int response_fd = open(RESPONSE_PIPE, O_RDONLY);
    if (response_fd < 0) {
        fprintf(stderr, "Error: Failed to open response pipe: %s\n", strerror(errno));
        return 0;
    }
    ssize_t read_bytes = read(response_fd, response_buffer, sizeof(response_buffer));
    if (read_bytes > 0) {
        response_buffer[read_bytes] = '\0';
        printf("Response:\n%s\n", response_buffer);
    }
    close(response_fd);
    */
    
    
    //  abandonado
    /*if (!wait_for_response(TIMEOUT)) {  // 5 second timeout
        fprintf(stderr, "Failed to get response from server\n");
        return 0;
    }
    */

}

int issue_command(Command cmd) {
    if (cmd.is_valid == FALSE) {
        return 0;
    }
    char msg[(PATH_MAX * 2) + INET_ADDRSTRLEN + 16];

    switch (cmd.type) {
        case DOWNLOAD:
            snprintf(msg, sizeof(msg), "GET %s:%s %s\n", cmd.ip, cmd.remote_path, cmd.local_path);
            send_message(msg);
            break;
        case UPLOAD:
            snprintf(msg, sizeof(msg), "PUT %s %s:%s\n", cmd.local_path, cmd.ip, cmd.remote_path);
            send_message(msg);
            break;
        case STATS:
            if (cmd.ip[0] == '\0') {
                printf("Issueing local stats command...\n");
                printf(" ****** Local server stats ****** \n");
                snprintf(msg, sizeof(msg), "STATS\n");
            } else {
                printf("Issueing remote stats command...\n");
                printf(" ****** Remote server stats ****** \n");
                snprintf(msg, sizeof(msg), "STATS %s\n", cmd.ip);
            }
            send_message(msg);
            break;
        case QUIT:
            printf("Issueing QUIT command...\n");
            send_message("QUIT\n");
            break;
        default:
            break;
    }

    return 1;
}

int main(int argc, char **argv){

    Command cmd = parse_commands(argc, argv);
    if (cmd.is_valid == FALSE) {
        exit(EXIT_FAILURE);
    }

    if (check_pipe_exists() == 0) {
        exit(EXIT_FAILURE);
    }

    /*if (attach_to_pipes() == 0) {
        exit(EXIT_FAILURE);
    }*/

    if (check_server_running() == 0) {
        exit(EXIT_FAILURE);
    }

    if (issue_command(cmd) == 0) {
        printf("Error: Command failed.\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}