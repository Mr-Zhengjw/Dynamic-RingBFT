import json
import requests
from server import *
from msg_type import *
from dynamic_ring_consensus import *
import os
import sys
sys.path.append('./')


if __name__ == '__main__':

    # 单分片事务 参数可见Msg_type.py
    # reqMsg = RequestMsg(GetTimestamp(), 2, "getMyName 30010", 23, [[1], [1]])
    # reqMsgJson = json.dumps(reqMsg.__dict__)
    # res1 = requests.post("http://127.0.0.1:30000/req", json=reqMsgJson)
    # print("req respon为:{}".format(res1.text))

    # 复杂跨分片事务
    # reqMsg = RequestMsg(GetTimestamp(), 2, "get my name", 23, [[1, 2], [2, 2]])
    # reqMsgJson = json.dumps(reqMsg.__dict__)
    # res1 = requests.post("http://127.0.0.1:30000/req", json=reqMsgJson)
    # print("req respon为:{}".format(res1.text))

    # DRC方法 需要线运行单分片事务
    # time.sleep(10)
    # invReqMsg = InvolveRequestMsg(GetTimestamp(), "r1_10", 30010, 2)
    # invReqMsgJson = json.dumps(invReqMsg.__dict__)
    # res2 = requests.post(
    #     "http://127.0.0.1:30010/involveReq", json=invReqMsgJson)
    # print("invIeq respon为:{}".format(res2.text))

    # NRC方法
    invReqMsg = InvolveRequestMsg(GetTimestamp(), "r1_10", 30010, 0)
    invReqMsgJson = json.dumps(invReqMsg.__dict__)
    res2 = requests.post(
        "http://127.0.0.1:30000/involveReq", json=invReqMsgJson)
    print("invIeq respon为:{}".format(res2.text))

    # RST方法
    # invReqMsg = InvolveRequestMsg(GetTimestamp(), "r1_4", 30004, 1)
    # invReqMsgJson = json.dumps(invReqMsg.__dict__)
    # res2 = requests.post(
    #     "http://127.0.0.1:30004/involveReq", json=invReqMsgJson)
    # print("invIeq respon为:{}".format(res2.text))
