import sys
sys.path.append(".")
from pbft_msg_type import *
from hashlib import sha256
import time
import json


# stage常量定义
class stage:
    # INITIAL = 0
    PREPREPARED = 1
    PREPARED = 2
    COMMITED = 3
    EXECUTED = 4
    INVOLVINGINITIAL = 5
    INVOLVINGPREPREPARED = 6
    INVOLVINGPREPARED = 7
    INVOLVINGCOMMITED = 8
    


f = 1


class MsgLogs:
    def __init__(self, reqMsg, invReqMsgs: list, prepareMsgs: dict, commitMsgs: dict, globalForwardMsgs: dict) -> None:
        self.ReqMsg = reqMsg
        self.PrepareMsgs = prepareMsgs
        self.InvReqMsgs = invReqMsgs
        self.CommitMsgs = commitMsgs
        self.GlobalForwardMsgs = globalForwardMsgs


# 每一个/组request在每个replica上都有一个state
class State:
    def __init__(self, viewID: int, msgLogs: MsgLogs, lastSequenceID, stage) -> None:
        self.ViewID = viewID
        self.MsgLogs = msgLogs
        self.LastSequenceID = lastSequenceID
        self.CurrentStage = stage

    # 主replica收到request时，开启共识，并发送preprepare消息
    def StartConsensus(self, request: RequestMsg):
        if request == None:
            return None
        sequenceID = GetTimestamp()
        if self.LastSequenceID != -1:
            while self.LastSequenceID >= sequenceID:
                sequenceID += 1
        request.SequenceID = sequenceID
        self.MsgLogs.ReqMsg = request
        dig = getDigest(request)
        # print("req degist : {}".format(dig),flush=True)
        if dig == None:
            return None
        print(dig, flush=True)
        self.CurrentStage = stage.PREPREPARED
        return PrePrepareMsg(self.ViewID, sequenceID, dig, request)

    # 其余replica收到preprepare消息
    def PrePrepare(self, nodeID, prePrepareMsg: PrePrepareMsg):
        self.MsgLogs.ReqMsg = prePrepareMsg.RequestMsg
        # 验证request
        if VerifyMsg(self,prePrepareMsg.ViewID, prePrepareMsg.SequenceID, prePrepareMsg.Digest) == False:
            return None

        # 更改本地状态
        self.CurrentStage = stage.PREPREPARED
        
        preMsg = VoteMsg(self.ViewID, prePrepareMsg.SequenceID, prePrepareMsg.Digest, nodeID, MsgType.PREPAREMSG,True)
        self.MsgLogs.PrepareMsgs[nodeID] = preMsg
        # 进行投票
        return preMsg


    def Prepare(self, nodeID, prepareMsg: VoteMsg):
        # 验证prepare消息
        # print("before verifyMsg",flush=True)
        if VerifyMsg(self,prepareMsg.ViewID, prepareMsg.SequenceID, prepareMsg.Digest) == False:
            return None,True
        # 记录prepare消息
        # print("pass verifyMsg",flush=True)
        self.MsgLogs.PrepareMsgs[prepareMsg.NodeID] = prepareMsg
        print("[Prepare-Vote]: {}".format(len(self.MsgLogs.PrepareMsgs)), flush = True)

        # 通过验证后，检查投票
        if Prepared(self):
            voteCount = 0
            for preMsg in self.MsgLogs.PrepareMsgs.keys():
                if self.MsgLogs.PrepareMsgs[preMsg].Vote:
                    voteCount += 1
            if voteCount <= (len(self.MsgLogs.PrepareMsgs) - voteCount):
                return None,False
            else:    
                self.CurrentStage = stage.PREPARED
                comMsg = VoteMsg(self.ViewID, prepareMsg.SequenceID, prepareMsg.Digest, nodeID, MsgType.COMMITMSG,True)
                self.MsgLogs.CommitMsgs[nodeID] = comMsg
                return comMsg,True
        return None,True


    def Commit(self, nodeID, commitMsg: VoteMsg):
        # 验证commit消息
        if VerifyMsg(self,commitMsg.ViewID, commitMsg.SequenceID, commitMsg.Digest) == False:
            return None, None, None, True     
        # 记录commit消息
        self.MsgLogs.CommitMsgs[commitMsg.NodeID] = commitMsg
        print("[Commit-Vote]: {}".format(len(self.MsgLogs.CommitMsgs)), flush = True)
        if Commited(self):
            # 本地执行request，得到结果
            voteCount = 0
            for preMsg in self.MsgLogs.CommitMsgs.keys():
                if self.MsgLogs.CommitMsgs[preMsg].Vote:
                    # print(self.MsgLogs.CommitMsgs[preMsg].Vote,flush=True)
                    voteCount += 1
            if voteCount <= (len(self.MsgLogs.CommitMsgs) - voteCount):
                result = False
                self.CurrentStage = stage.COMMITED
                # 获取commit摘要
                dig = getDigest(commitMsg)
                return (ReplyMsg(self.ViewID, self.MsgLogs.ReqMsg.Timestamp, self.MsgLogs.ReqMsg.ClientID, nodeID, result)), self.MsgLogs.ReqMsg, GlobalForwardMsg(self.MsgLogs.ReqMsg, commitMsg, dig), False
            else:
                result = True
                self.CurrentStage = stage.COMMITED
                # 获取commit摘要
                dig = getDigest(commitMsg)
                # 返回回复客户端的消息，请求消息以及全局转发的信息
                return ReplyMsg(self.ViewID, self.MsgLogs.ReqMsg.Timestamp, self.MsgLogs.ReqMsg.ClientID, nodeID, result), self.MsgLogs.ReqMsg, GlobalForwardMsg(self.MsgLogs.ReqMsg, commitMsg, dig), True
        return None,None,None, True


    def InvolveVerifyReq(self, nodeID, request: RequestMsg):
        if len(self.MsgLogs.InvReqMsgs) <= 2*f:
            return None
        if request == None:
            return None
        dig = getDigest(request)
        # print("InvolveVerifyReq degist : {}".format(dig),flush=True)
        if dig == None:
            return None
        # print(dig)    
        self.CurrentStage = stage.INVOLVINGPREPREPARED
        preMsg = VoteMsg(self.ViewID, request.SequenceID, dig, nodeID, MsgType.INVOLVEPREPAREMSG,True)
        self.MsgLogs.PrepareMsgs[nodeID] = preMsg
        # 进行投票
        return preMsg

    def InvolveVerifyPrepare(self, prepareMsg: VoteMsg, reqMsg: RequestMsg, prepareMsgs: dict):
        # print("InvolveVerifyPrepare",flush=True)
        # prepareMsg.MsgType = MsgType.PREPAREMSG
        if InvolveVerifyMsg(self, prepareMsg.ViewID, prepareMsg.SequenceID, prepareMsg.Digest,reqMsg) == False:
            # print("if False",flush=True)
            return True
        self.MsgLogs.PrepareMsgs[prepareMsg.NodeID] = prepareMsg
        if len(prepareMsgs) + 1 > 2*f:
            # print("len(prepareMsgs) + 1 > 2*f:",flush=True)
            voteCount = 0
            for preMsg in prepareMsgs.keys():
                if prepareMsgs[preMsg].Vote:
                    voteCount += 1
            if voteCount <= (len(prepareMsgs) - voteCount):
                return False
            else:    
                self.CurrentStage = stage.INVOLVINGPREPARED
                return True
        return False
    
    def InvolveVerifyCommit(self, commitMsg: VoteMsg, reqMsg: RequestMsg, commitMsgs: dict):
        if InvolveVerifyMsg(self, commitMsg.ViewID, commitMsg.SequenceID, commitMsg.Digest,reqMsg) == False:
            return True
        self.MsgLogs.CommitMsgs[commitMsg.NodeID] = commitMsg
        if len(commitMsgs)+1 >= 2*f:
            voteCount = 0
            for comMsg in commitMsgs.keys():
                if commitMsgs[comMsg].Vote:
                    voteCount += 1
            if voteCount <= (len(commitMsgs) - voteCount):
                return False
            else:    
                self.CurrentStage = stage.INVOLVINGCOMMITED
                return True
        return False

def verifyForward(state: State, globalForwardMsg: GlobalForwardMsg):
    digestGot = getDigest(globalForwardMsg.CommitMsg)
    if globalForwardMsg.Digest != digestGot:
        return False
    return True


def VerifyMsg(state: State, viewID: int, sequenceID: int, digGot: str):
    if state.ViewID != viewID:
        print("state.ViewID != viewID",flush=True)
        return False

    if state.LastSequenceID != -1 and state.LastSequenceID >= sequenceID:
        print("state.LastSequenceID != -1 and state.LastSequenceID >= sequenceID",flush=True)
        return False

    dig = getDigest(state.MsgLogs.ReqMsg)
    if digGot != dig:
        print("digGot != dig {} {}".format(digGot,dig),flush=True)
        return False
    
    return True

def InvolveVerifyMsg(state: State, viewID: int, sequenceID: int, digGot: str, reqMsg:RequestMsg):
    if state.ViewID != viewID:
        print("state.ViewID != viewID",flush=True)
        return False

    if state.LastSequenceID != -1 and state.LastSequenceID >= sequenceID:
        print("state.LastSequenceID != -1 and state.LastSequenceID >= sequenceID",flush=True)
        return False

    dig = getDigest(reqMsg)
    # print(dig,flush=True)
    # print(digGot,flush=True)
    if digGot != dig:
        print("digGot != dig {} {}".format(digGot,dig),flush=True)
        return False
    return True

# 验证能否完成prepare阶段
def Prepared(state: State):
    if state.MsgLogs.ReqMsg == None:
        return False
    if len(state.MsgLogs.PrepareMsgs) <= 2*f:
        print("len(state.MsgLogs.PrepareMsgs) {}".format(len(state.MsgLogs.PrepareMsgs)),flush=True)
        return False
    return True


# 验证能否完成commit阶段
def Commited(state: State):
    if Prepared(state) == False:
        return False
    if len(state.MsgLogs.CommitMsgs) <= 2*f:
        return False
    return True


# 收到一个request时，replica进行创建一个对应的state，用来记录当前request进行的情况
def CreatState(viewID: int, lastSequenceID: int):
    return State(viewID, MsgLogs(None, [], {}, {}, {}), lastSequenceID, None)


# 计算消息摘要
def getDigest(msg):
    if msg is None:
        return None
    m = sha256()
    m.update(bytes(json.dumps(msg.__dict__), encoding='utf-8'))
    return m.hexdigest().upper()


# 获取时间戳
def GetTimestamp():
    return int(round(time.time() * 1000000000))


def TimestampToDate(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp/1000000000))


# if __name__ == "__main__":
#     sta = CreatState(1, 2)
#     req = RequestMsg(GetTimestamp(), 2, "get my name", 23, [1, 2])
#     StartConsensus(sta, req)
#     # StartConsensus(2)
#     print(getDigest(None))
