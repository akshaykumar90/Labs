class Message(object):
  """the actual message sent over the wire"""
  def __init__(self, src, dest, kind, data):
    super(Message, self).__init__()
    self.src = src
    self.dest = dest
    self.kind = kind
    self.data = data

  # Used by MessagePasser.send
  def set_id(self, id):
    self.id = id
  
  # Required for multicast messaging
  def set_dest(self, dest):
    self.dest = dest

class TimeStampedMessage(Message):
  """docstring for TimeStampedMessage"""
  def __init__(self, src, dest, kind, data):
    super(TimeStampedMessage, self).__init__(src, dest, kind, data)

  def set_timestamp(self, ts):
    self.timestamp = ts

class RMessage(Message):
  """message suppoting reliable multicast delivery semantics"""
  def __init__(self, src, dest, kind, data):
    super(RMessage, self).__init__(src, dest, kind, data)

  # Multicast group ID for the destination group
  def set_mcast_gid(self, gid):
    self.gid = gid

  # Multicast sequence ID for the source process
  def set_src_seqid(self, seqid):
    self.seqid = seqid

  # Multicast acknowledgements of the form [<q,R(q)>]
  def set_acks(self, acks):
    self.acks = acks

class RCOMessage(RMessage, TimeStampedMessage):
  """message supporting reliable causally ordered multicast delivery semantics"""
  def __init__(self, src, dest, kind, data):
    super(RCOMessage, self).__init__(src, dest, kind, data)
    