from yaml import load
import socket
import pickle
from Queue import Queue
import logging
from copy import copy
import threading

from rule import SendRule, ReceiveRule
from message import Message, TimeStampedMessage, RMessage, RCOMessage
from clockservice import ClockServiceFactory

logging.basicConfig(filename="messagepasserlog.txt", \
  level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class MessagePasser(object):
  """sends and receives Messages"""
  def __init__(self, config_filename, local_name):
    super(MessagePasser, self).__init__()
    self.config_filename = config_filename
    self.local_name = local_name
    self.sendQueue = Queue()
    self.recQueue = Queue()
    self.appRecQueue = Queue()
    self.delayed_sent_messages = []
    self.delayed_received_messages = []
    self.uid = 0

  def set_clockservice(self, cs):
    self.cs = cs
    pass

  def update_config(self):
    # Zero out all the data structures
    self.nodes = {}
    self.group_owners = {}
    self.sendRules = []
    self.receiveRules = []

    # Read YAML config file into a dict
    fp = open(self.config_filename, "r")
    dataMap = load(fp)
    fp.close()

    # Read in new nodes mappings
    for config in dataMap['Configuration']:
      self.nodes[config['Name']] = (config['IP'], config['Port'])
      self.group_owners[config['Name']] = config['Group']

    # Set a unique node id based on alphabetical ordering
    nodes_keys = self.nodes.keys()
    nodes_keys.sort()
    self.nid = nodes_keys.index(self.local_name)

    # Read in new SendRules
    for sr in dataMap['SendRules']:
      nrule = {
        'action': None,
        'src': None,
        'dest': None,
        'kind': None,
        'id': None,
        'nth': None
      }
      for k,v in sr.iteritems():
        nrule[k.lower()] = v
      self.sendRules.append(SendRule(**nrule))

    # Read in new ReceiveRules
    for rr in dataMap['ReceiveRules']:
      nrule = {
        'action': None,
        'src': None,
        'dest': None,
        'kind': None,
        'id': None,
        'nth': None
      }
      for k,v in rr.iteritems():
        nrule[k.lower()] = v
      self.receiveRules.append(ReceiveRule(**nrule))

    # Update multicast setup as well
    self.setup_mcast()

  def setup_mcast(self):
    self.groups = {}
    nodes_keys = self.nodes.keys()
    nodes_keys.sort()
    # Populate <code>groups</code> with metadata of groups which the 
    # current process is part of.
    # gid(key) : unique group ID of the group (globally unique)
    # mid : member ID of the current process in the group (group-wise unique)
    # members : group members (names) in sorted order
    for owner, members in self.group_owners.iteritems():
      members.sort()
      if self.local_name in members:
        gid = nodes_keys.index(owner)
        mid = members.index(self.local_name)
        self.groups[gid] = {
          'members' : members,
          'mid' : mid,
        }

    # More metadata
    for gid,g in self.groups.iteritems():
      g['mcast_id'] = 0
      g['latest_seqids_nodes'] = {}
      for k in g['members']:
        g['latest_seqids_nodes'][k] = 0
      g['hold_back_queue'] = []
      g['mcast_msgs_sent'] = {}
      # Initialize multicast clock service
      cs_factory = ClockServiceFactory()
      cs_factory.set_nodes(len(g['members'])) # No. of nodes in the group
      cs_factory.set_host_node_id(g['mid']) # Member ID for *this* process
      g['mcast_vector_clock'] = cs_factory.get_clock("VECTOR")

    # Reference to group owned by this process
    self.mcast_group = self.groups[self.nid]

    # Hold back queue required for causal ordering of message delivery
    self.co_hold_back_queue = []

    # Event to notify of new messages on hold back queue for causal ordering
    self.new_messages_event = threading.Event()

  def process_send_queue(self):
    while True:
      msg = self.sendQueue.get()
      msg.set_id(self.uid)
      self.uid += 1
      msg.set_timestamp(self.cs.clock_tick())
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.connect(self.nodes[msg.dest])
      logging.debug("%s has been assigned socket name %s" % (self.local_name, s.getsockname()))
      s.shutdown(socket.SHUT_RD)
      s.sendall(pickle.dumps(msg))
      s.close()
    pass

  def send(self, msg):
    matched = False
    for rule in self.sendRules:
      if rule.match(msg):
        if rule.action == "drop":
          return # Packet dropped
        elif rule.action == "duplicate":
          self.sendQueue.put(msg)
          for dsm in self.delayed_sent_messages:
            self.sendQueue.put(dsm)
          self.sendQueue.put(msg)
          self.delayed_sent_messages = []
        elif rule.action == "delay":
          self.delayed_sent_messages.append(msg)
        matched = True
        break
    if not matched:
      self.sendQueue.put(msg)
      for dsm in self.delayed_sent_messages:
        self.sendQueue.put(dsm)
      self.delayed_sent_messages = []
    pass

  def r_multicast(self, msg):
    mcast_group = self.mcast_group
    mcast_id = mcast_group['mcast_id']
    msg.set_src_seqid(mcast_id)
    msg.set_acks(mcast_group['latest_seqids_nodes'].items())
    mcast_group['mcast_msgs_sent'][mcast_id] = msg
    mcast_id += 1
    for dest in mcast_group['members']:
      msg_copy = copy(msg)
      msg_copy.set_dest(dest)
      self.send(msg_copy)

  def co_multicast(self, msg):
    mcast_group = self.mcast_group
    ts = mcast_group['mcast_vector_clock'].clock_tick()
    msg.set_timestamp(ts)
    self.r_multicast(msg)

  def __match_message(self, msg):
    for rule in self.receiveRules:
      if rule.match(msg):
        return rule.action
    return None

  def receive_message(self):
    addr = self.nodes[self.local_name]
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(addr)
    s.listen(1)
    while True:
      logging.debug("Listening at %s" % (s.getsockname(),))
      sc, sockname = s.accept()
      logging.debug("We have accepted a connection from %s" % (sockname,))
      logging.debug("Socket connects %s and %s" % (sc.getsockname(), sc.getpeername()))
      sc.shutdown(socket.SHUT_WR)
      f = sc.makefile('rb')
      msg = pickle.load(f)
      f.close()
      sc.close()
      logging.debug('Socket closed')

      logging.debug("Message received - Src: %s - Kind: %s - ID: %s" % (msg.src, msg.kind, msg.id))
      action = self.__match_message(msg)
      if action is not None:
        if action == "drop":
          logging.debug("Packet from %s dropped" % (msg.src,))
          continue # Packet dropped
        elif action == "duplicate":
          self.recQueue.put(msg)
          for drm in self.delayed_received_messages:
            self.recQueue.put(drm)
          self.recQueue.put(msg)
          self.delayed_received_messages = []
        elif action == "delay":
          self.delayed_received_messages.append(msg)
      else:
        self.recQueue.put(msg)
        for drm in self.delayed_received_messages:
          self.recQueue.put(drm)
        self.delayed_received_messages = []

  def mcast_receive(self):
    while True:
      msg = self.recQueue.get()

      # Process only MCAST/MCAST_NACK messages
      if not msg.kind.startswith("MCAST"):
        self.appRecQueue.put(msg)
        continue

      logging.debug("MCAST Message received - Src: %s - Kind: %s - ID: %s" % (msg.src, msg.kind, msg.id))

      # References for destination group of message received
      msg_dest_group = self.groups[msg.gid]
      mcast_msgs_sent = msg_dest_group['mcast_msgs_sent']
      latest_seqids_nodes = msg_dest_group['latest_seqids_nodes']
      hold_back_queue = msg_dest_group['hold_back_queue']

      # Process all special MCAST* messages first
      if msg.kind == "MCAST_NACK":
        # Retransmit message from sent message cache
        rt_msg = copy(mcast_msgs_sent[msg.data])
        rt_msg.set_dest(msg.src)
        self.send(rt_msg)
        continue

      # Handle ALL remaining multicast messages
      if msg.seqid == latest_seqids_nodes[msg.src] + 1:
        self.__r_deliver_callback(msg)
        latest_seqids_nodes[msg.src] += 1
        # Deliver any outstanding messages on hold back queue
        all_msgs_from_src = filter(lambda m: m.src == msg.src, hold_back_queue)
        all_msgs_from_src.sort(key=lambda m: m.seqid)
        for hb_msg in all_msgs_from_src:
          if hb_msg.seqid == latest_seqids_nodes[msg.src] + 1:
            self.__r_deliver_callback(msg)
            latest_seqids_nodes[msg.src] += 1
            hold_back_queue.remove(hb_msg)
          else:
            break
      elif msg.seqid <= latest_seqids_nodes[msg.src]:
        # Discard the message
        pass
      else:
        # Append the message on hold back queue
        logging.debug("Putting message on holdback queue - Src: %s - Kind: %s - ID: %s" % (msg.src, msg.kind, msg.id))
        hold_back_queue.append(msg)
        # Send NACK to source node for missing messages
        nack_origin_node = TimeStampedMessage(self.local_name, msg.src, "MCAST_NACK", latest_seqids_nodes[msg.src])
        self.send(nack_origin_node)

      for q,rq in msg.acks:
        if rq > latest_seqids_nodes[q]:
          # We have missed some messages from q
          # Send NACK to q for them
          nack_q_node = TimeStampedMessage(self.local_name, q, "MCAST_NACK", latest_seqids_nodes[q])
          self.send(nack_q_node)

  def __r_deliver_callback(self, msg):
    # R-Deliver the message
    self.co_hold_back_queue.append(msg)
    self.new_messages_event.set()

  def co_deliver(self):
    while True:
      # <code>new_clock_ticks</code> emulates a timer - but one which increments 
      # only when new  messages are CO-Delivered - hence we need to run the thread 
      # again after each new message is CO-Delivered. This is not a shared variable 
      # - local to this thread only
      new_clock_ticks = True
      self.new_messages_event.wait()
      while new_clock_ticks:
        new_clock_ticks = False
        for msg in self.co_hold_back_queue:
          # References for destination group of message received
          msg_dest_group = self.groups[msg.gid]
          mcast_vector_clock = msg_dest_group['mcast_vector_clock']
          j = msg_dest_group['members'].index(msg.src)
          if j == msg_dest_group['mid']:
            self.appRecQueue.put(msg)
            self.co_hold_back_queue.remove(msg)
            self.mcast_vector_clock.clock_tick()
            new_clock_ticks = True
          elif mcast_vector_clock.co_next(msg.timestamp, j):
            self.appRecQueue.put(msg)
            self.co_hold_back_queue.remove(msg)
            mcast_vector_clock.clock_tick_j(j)
            new_clock_ticks = True
          else:
            pass
        self.new_messages_event.clear()

  def receive(self):
    if not self.appRecQueue.empty():
      new_msg = self.appRecQueue.get()
      # Adjust time accordingly
      self.cs.adjust_clock(new_msg.timestamp)
      return new_msg
    else:
      return None
