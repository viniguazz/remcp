# remcp
A client/server Linux TCP/IP file transferer

####################################################
#       REMCP v1.0 by Vinicius Guazzelli Dias      #
#                 2024-12-14                       #
####################################################

Welcome to REMCP (a very humble work-in-progress)
service for file transfer over TCP/IP for Linux!

Even though it's not very robust or elegant, it 
serves its purpose! I plan to modularize, clean, 
refactor and improve the code in the future.

Any feedback or contributions are very welcome!

Thanks you very much for downloading!

***************** Intructions ***********************

Compile client:
    gcc client.c -o remcp

Compile server:
    gcc -pthread server.c -o remcp-server

Run server (daemon):
    ./remcp-server

Run client:
    ./remcp

***************** REMCP Usage ***********************

DOWNLOAD FILE:      remcp <ip:absolute_path> <path>
UPLOAD FILE:         remcp <path> <ip:absolute_path>
LOCAL SERVER STATS:  remcp stats
REMOTE SERVER STATS: remcp stats ip
SHUTDOWN SERVER:     remcp quit

*****************************************************

To watch the server logs:
    
    tail -f /var/log/syslog | grep REMCP

        or

    sudo journalctl -f | grep REMCP

*****************************************************

Only absolute paths are supported.
