import threading
from server import CreateServer
import sys

if __name__ == '__main__':
    CreateServer(sys.argv[1],int(sys.argv[2]))
    # print(sys.argv[1],flush=True)
    # thread = threading.Thread(target=CreateServer, args=[sys.argv[1],int(sys.argv[2])])
    # thread.start()
