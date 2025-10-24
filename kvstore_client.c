#include <sys/socket.h>
#include <sys/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SOCKET_PATH "/tmp/kvstore.sock"
#define BUF_SIZE 256

// Error exit helper
static void die(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

int main(void)
{
    int fd;
    struct sockaddr_un addr;
    char buf[BUF_SIZE], cmd[BUF_SIZE];

    // Create socket
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1)
        die("socket");

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    // Connect to the server
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1)
        die("connect");

    printf("Connected to KV Store server at %s\n", SOCKET_PATH);
    printf("Type commands (SET key value / GET key / EXIT)\n\n");

    while (1)
    {
        printf("> ");
        fflush(stdout);

        // Read command from user
        if (!fgets(cmd, sizeof(cmd), stdin))
            break; // EOF or error

        // Remove trailing newline
        cmd[strcspn(cmd, "\n")] = 0;

        // Exit condition
        if (strcasecmp(cmd, "EXIT") == 0)
        {
            printf("Closing connection.\n");
            break;
        }

        // Send command to server
        if (write(fd, cmd, strlen(cmd)) == -1)
        {
            perror("write");
            break;
        }

        // Read server response
        ssize_t n = read(fd, buf, sizeof(buf) - 1);
        if (n > 0)
        {
            buf[n] = '\0';
            printf("[server] %s", buf);
        }
        else
        {
            printf("[server closed connection]\n");
            break;
        }
    }

    close(fd);
    return 0;
}

