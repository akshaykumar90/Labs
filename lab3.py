import sys
import threading
from Queue import Queue
import time

from messagepasser import MessagePasser
from message import RCOMessage, TimeStampedMessage
from clockservice import ClockServiceFactory

# Globals
K = 3 # ACKs required to continue (= processes in the group)
lock_acquired = False
lock_acquired_event = threading.Event()
voted = False
requests_queue = Queue()
acks_received = 0

def process_received_messages(mp):
  while True:
    msg = mp.receive()
    while msg is not None:
      if msg.kind == "MCAST_CS_REQUEST":
        # On receipt of a request
        if lock_acquired or voted:
          # queue request without replying
          requests_queue.put(msg)
        else:
          # send reply
          cs_ack = TimeStampedMessage(self.local_name, msg.src, "CS_ACK", self.local_name)
          mp.send(cs_ack)
          voted = True
      elif msg.kind == "MCAST_CS_RELEASE":
        # On receipt of a release
        if !requests_queue.empty():
          # remove head of queue and send reply
          head = requests_queue.get()
          cs_ack = TimeStampedMessage(self.local_name, head.src, "CS_ACK", self.local_name)
          mp.send(cs_ack)
          voted = True
        else:
          voted = False
      elif msg.kind == "CS_ACK":
        acks_received += 1
        if acks_received == K:
          lock_acquired = True
          lock_acquired_event.set()
      else:
        # Ignore
      msg = mp.receive()
    time.sleep(0.5) # Sleep for half-a-second

if __name__ == '__main__':
  if len(sys.argv) == 3:
    config_filename = sys.argv[1]
    local_name = sys.argv[2]
    mp = MessagePasser(config_filename, local_name)
    mp.update_config()

    # Update K
    K = len(mp.mcast_group)

    # Initialize the clock service here
    # Should be done AFTER update_config() on MessagePasser
    cs_factory = ClockServiceFactory()
    cs_factory.set_nodes(len(mp.nodes)) # No. of nodes in the system
    cs_factory.set_host_node_id(mp.nid) # ID for *this* process
    # Uncomment for Vector Clock
    cs = cs_factory.get_clock("VECTOR")
    
    # Inject Clock Service into MessagePasser
    mp.set_clockservice(cs)

    # Start server thread(s)
    send_msgs_thread = threading.Thread(name="send_msgs_thread", target=mp.process_send_queue)
    recv_msgs_thread = threading.Thread(name="recv_msgs_thread", target=mp.receive_message)
    mcast_recv_msgs_thread = threading.Thread(name="mcast_recv_msgs_thread", target=mp.mcast_receive)
    co_deliver_msgs_thread = threading.Thread(name="co_deliver_msgs_thread", target=mp.co_deliver)
    send_msgs_thread.start()
    recv_msgs_thread.start()
    mcast_recv_msgs_thread.start()
    co_deliver_msgs_thread.start()

    # Start thread to process application messages received
    app_msgs_thread = threading.Thread(name="app_msgs_thread", target=process_received_messages, args=(mp,))
    app_msgs_thread.start()

    CLIENT_PROMPT = "[&]"

    while True:
      try:
        inp = raw_input(CLIENT_PROMPT)
      except EOFError:
        break
      sinput = inp.split()
      cmd = sinput[0]

      # Supported commands - enter | exit
      # Use `enter` to acquire the distributed lock before entering the critical
      # section. The process will wait until the lock is available.
      # Once acquired, the distributed lock can be released with the `exit` command
      # If the process does not own the distributed lock, this will be a no-op
      if cmd == 'enter':
        print "Waiting for mutex lock..."
        # Multicast request to all processes in its group
        cs_request = RCOMessage(mp.local_name, "", "MCAST_CS_REQUEST", mp.local_name)
        mp.co_multicast(cs_request)
        # Wait until lock is acquired
        lock_acquired_event.wait()
        print "Inside critical section. Type exit to quit."
      elif cmd == 'exit':
        if lock_acquired:
          print "Leaving critical section. Releasing mutex lock now."
          lock_acquired = False
          lock_acquired_event.clear()
          acks_received = 0
          # Multicast release to all processes in its group
          cs_release = RCOMessage(mp.local_name, "", "MCAST_CS_RELEASE", mp.local_name)
          mp.co_multicast(cs_release)
        else:
          print "Umm... You are not in a critical section..."
      else:
        print "usage: enter | exit"
  else:
    print >>sys.stderr, 'usage: lab3.py <config_filename> <local_name>'