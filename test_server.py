from server import CreateServer
import sys

if __name__ == '__main__':
    # 开启一个节点 第一个参数为节点序号，第二个参数为端口号
    CreateServer(sys.argv[1],int(sys.argv[2]))
    # print(sys.argv[1],flush=True)
    # thread = threading.Thread(target=CreateServer, args=[sys.argv[1],int(sys.argv[2])])
    # thread.start()
