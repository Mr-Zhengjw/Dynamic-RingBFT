import json
import requests
from server import *
from pbft_msg_type import *
from pbft_impl import *
import os
import sys
sys.path.append('./')


if __name__ == '__main__':

    # 开启服务
    # ss = json.dumps([{'nodeID': 1, 'myPort': 30000}])
    # res = requests.post("http://127.0.0.1:30000/startServer", json=ss)
    # print("respon为:\n{}".format(res.text))
    # print()

    # # req
    # reqMsg = RequestMsg(GetTimestamp(), 2, "get my name", 23, [[1, 2], [2, 2]])
    # reqMsgJson = json.dumps(reqMsg.__dict__)
    # res1 = requests.post("http://127.0.0.1:30000/req", json=reqMsgJson)
    # print("req respon为:{}".format(res1.text))

    # reqMsg = RequestMsg(GetTimestamp(), 2, "getMyName 30004 ", 23, [[1], [1]])
    # reqMsgJson = json.dumps(reqMsg.__dict__)
    # res1 = requests.post("http://127.0.0.1:30000/req", json=reqMsgJson)
    # print("req respon为:{}".format(res1.text))

    # preprepare
    # prePreMsg = PrePrepareMsg(1, 2, getDigest(reqMsg), reqMsgJson)
    # prePreMsgJson = json.dumps(prePreMsg.__dict__)
    # res2 = requests.post(
    #     "http://127.0.0.1:30000/preprepare", json=prePreMsgJson)
    # print("req respon为:\n{}".format(res2.text))

    # print()

    # sss = json.dumps([{'nodeID': 1, 'myPort': 30001}])
    # res = requests.post("http://127.0.0.1:30001/startServer", json=sss)
    # print("respon为:\n{}".format(res.text))
    # print()

    # res3 = requests.post("http://127.0.0.1:30001/req",json=reqMsgJson)
    # print(res3.text)

    # StartServer(30000)

    # time.sleep(10)
    # invReqMsg = InvolveRequestMsg(GetTimestamp(), "r1_4", 30004, 2)
    # invReqMsgJson = json.dumps(invReqMsg.__dict__)
    # res2 = requests.post(
    #     "http://127.0.0.1:30004/involveReq", json=invReqMsgJson)
    # print("invIeq respon为:{}".format(res2.text))

    invReqMsg = InvolveRequestMsg(GetTimestamp(), "r1_4", 30004, 0)
    invReqMsgJson = json.dumps(invReqMsg.__dict__)
    res2 = requests.post(
        "http://127.0.0.1:30000/involveReq", json=invReqMsgJson)
    print("invIeq respon为:{}".format(res2.text))
