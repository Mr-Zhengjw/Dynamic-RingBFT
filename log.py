import sys
sys.path.append(".")
from msg_type import *
from dynamic_ring_consensus import GetTimestamp


# 记录消息日志
def LogMsg(msg):
    msgType = type(msg)
    if msgType == RequestMsg:
        print("[REQUEST] ClientID: {}, Timestamp: {}, Operation: {}".format(
            msg.ClientID, msg.Timestamp, msg.Operation))
    elif msgType == PrePrepareMsg:
        print("[PREPREPARE] ClientID: {}, Operation: {}, SequenceID: {d}".format(
            msg.RequestMsg.ClientID, msg.RequestMsg.Operation, msg.RequestMsg.SequenceID))
    elif msgType == VoteMsg:
        if msg.MsgType == MsgType.PREPAREMSG:
            print("[PREPARE] NodeID: {}".format(msg.NodeID))
        elif msg.MsgType == MsgType.COMMITMSG:
            print("[COMMIT] NodeID: {}".format(msg.NodeID))
    elif msgType == GlobalForwardMsg:
        print("[Global Sharing] ReqMsg:{}".format(msg.ReqMsg))

# 记录状态日志
def LogStage(stage: str, isDone: bool):
    if isDone:
        print("[STAGE-DONE] {}".format(stage), flush=True)
    else:
        print("[STAGE-BEGIN] {}".format(stage), flush=True)


if __name__ == "__main__":
    req = RequestMsg(GetTimestamp(), 2, "req", 2, [1, 2])
    prmsg = PrePrepareMsg(GetTimestamp(),2,"sda",req)
    msg = VoteMsg(2,2,2,2,MsgType.COMMITMSG,True)
    LogMsg(msg)
	# LogStage("stage",True)