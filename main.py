from msg_type import *
import time



if __name__ == "__main__":
    remsg = RequestMsg(time.time(),1,"getName",2,[1,2])
    print(remsg.Timestamp)
    print(1)
    for i in range(5):
        time.sleep(0.1)
        print(time.time())