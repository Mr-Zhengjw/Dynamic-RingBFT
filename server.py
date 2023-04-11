import json
import threading
from flask import Flask, request
from pbft_msg_type import *
from node import *


myNode = NewNode('', {"r1_0": "127.0.0.1:30000",
                      "r1_1": "127.0.0.1:30001",
                      "r1_2": "127.0.0.1:30002",
                      "r1_3": "127.0.0.1:30003",
                      "r2_0": "127.0.0.1:30010",
                      "r2_1": "127.0.0.1:30011",
                      "r2_2": "127.0.0.1:30012",
                      "r2_3": "127.0.0.1:30013",
                      "r2_4": "127.0.0.1:30014",
                      "r3_0": "127.0.0.1:30020",
                      "r3_1": "127.0.0.1:30021",
                      "r3_2": "127.0.0.1:30022",
                      "r3_3": "127.0.0.1:30023"},
                 View(100, "r1_0"), None, [], MsgBuffer([], [], [], [], [], [], []), [], [])
myServer = Server(1, myNode)
app = Flask(__name__)


def CreateServer(nodeID: str, myPort: int):
    myServer.Node.NodeID = nodeID
    app.run(host="127.0.0.1", port=myPort, threaded=True)


@ app.route('/startServer', methods=['POST'])
def StartServer():
    jsondata = request.get_json()
    data = json.loads(jsondata)[0]  # type: ignore
    return "req:{}".format(data)


@ app.route('/req', methods=['POST'])
def Req():
    jsondata = request.get_json()
    msg = DecodeRequestMsg(json.loads(jsondata))  # type: ignore
    myServer.Node.MsgEntrance.append(msg)
    # myServer.Node.GetReq(msg)
    return "request:{}".format(str(type(msg))+' ', msg)
    # return "request:{}".format(myServer.Node.MsgEntrance)


@ app.route('/involveReq', methods=['POST'])
def InvolveReq():
    jsondata = request.get_json()
    msg = DecodeInvolveRequestMsg(json.loads(jsondata))  # type: ignore
    myServer.Node.MsgEntrance.append(msg)
    return "request:{}".format(str(type(msg))+' ', msg)


@ app.route('/preprepare', methods=['POST'])
def PrePrepare():
    jsondata = request.get_json()
    data = json.loads(jsondata)  # type: ignore
    msg = DecodePrePrepareMsg(data)
    myServer.Node.MsgEntrance.append(msg)
    # myServer.Node.GetPrePrepare(msg)
    return "preprepare:{}".format(myServer.Node.MsgEntrance)


@ app.route('/prepare', methods=['POST'])
def Prepare():
    jsondata = request.get_json()
    data = json.loads(jsondata)  # type: ignore
    msg = DecodeVoteMsg(data)
    myServer.Node.MsgEntrance.append(msg)
    # myServer.Node.GetPrepare(msg)
    return "prepare:{}".format(myServer.Node.MsgEntrance)


@ app.route('/commit', methods=['POST'])
def Commit():
    jsondata = request.get_json()
    data = json.loads(jsondata)  # type: ignore
    msg = DecodeVoteMsg(data)
    myServer.Node.MsgEntrance.append(msg)
    # myServer.Node.GetCommit(msg)
    return "commit:{}".format(myServer.Node.MsgEntrance)


@ app.route('/reply', methods=['POST'])
def Reply():
    jsondata = request.get_json()
    data = json.loads(jsondata)  # type: ignore
    msg = DecodeReplyMsg(data)
    myServer.Node.MsgEntrance.append(msg)
    # print("Result: {} by {} on {}".format(msg.Result, msg.NodeID, msg.Timestamp),flush=True)
    # myServer.Node.MsgDelivery.append(msg)
    # print("Result: {} by {} on {}".format(myServer.Node.MsgDelivery[-1].Result, myServer.Node.MsgDelivery[-1].NodeID, myServer.Node.MsgDelivery[-1].Timestamp),flush=True)
    return "reply:{}".format(myServer.Node.MsgEntrance)


@ app.route('/globalforward', methods=['POST'])
def GlobalForward():
    jsondata = request.get_json()
    data = json.loads(jsondata)  # type: ignore
    msg = DecodeGlobalForwardMsg(data)
    myServer.Node.MsgEntrance.append(msg)
    return "globalforward:{}".format(myServer.Node.MsgEntrance)


@ app.route('/localforward', methods=['POST'])
def LocalForward():
    jsondata = request.get_json()
    data = json.loads(jsondata)  # type: ignore
    msg = DecodeLocalForwardMsg(data)
    myServer.Node.MsgEntrance.append(msg)
    return "localforward:{}".format(myServer.Node.MsgEntrance)


@ app.route('/hisInvolveReq', methods=['POST'])
def HisInvolveReq():
    jsondata = request.get_json()
    msg = DecodeRequestMsg(json.loads(jsondata))  # type: ignore
    myServer.Node.MsgEntrance.append(msg)
    return "request:{}".format(str(type(msg))+' ', msg)


@ app.route('/hisInvolvePrepare', methods=['POST'])
def HisInvolvePrepare():
    jsondata = request.get_json()
    msg = DecodeVoteMsg(json.loads(jsondata))  # type: ignore
    myServer.Node.MsgEntrance.append(msg)
    return "request:{}".format(str(type(msg))+' ', msg)


@ app.route('/hisInvolveCommit', methods=['POST'])
def HisInvolveCommit():
    jsondata = request.get_json()
    msg = DecodeVoteMsg(json.loads(jsondata))  # type: ignore
    myServer.Node.MsgEntrance.append(msg)
    return "request:{}".format(str(type(msg))+' ', msg)


if __name__ == '__main__':
    # app.run(host, port, debug, options)
    # ser = StartServer("1",30000)
    # test = Test("1",30000)
    # test.run()
    # conv = [{'nodeID': 1, 'myPort': 30000}]
    # s = json.dumps(conv)
    thread1 = threading.Thread(target=CreateServer, args=["p1_0", 30000])
    thread2 = threading.Thread(target=CreateServer, args=["r1_1", 30001])
    thread3 = threading.Thread(target=CreateServer, args=["r1_2", 30002])
    thread4 = threading.Thread(target=CreateServer, args=["r1_3", 30003])
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
