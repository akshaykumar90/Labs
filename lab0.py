import sys
import threading

from messagepasser import MessagePasser
from message import Message, TimeStampedMessage
from clockservice import ClockServiceFactory

if __name__ == '__main__':
  if len(sys.argv) == 3:
    config_filename = sys.argv[1]
    local_name = sys.argv[2]
    mp = MessagePasser(config_filename, local_name)
    mp.update_config()

    # Initialize the clock service here
    # Should be done AFTER update_config() on MessagePasser
    cs_factory = ClockServiceFactory()
    cs_factory.set_nodes(len(mp.nodes)) # No. of nodes in the system
    cs_factory.set_host_node_id(mp.nid) # ID for *this* process
    # Logical Clock
    cs = cs_factory.get_clock("LOGICAL")
    # Uncomment for Vector Clock
    # cs = cs_factory.get_clock("VECTOR")
    
    # Inject Clock Service into MessagePasser
    mp.set_clockservice(cs)

    # Start server thread listening to messages
    listen_thread = threading.Thread(name="listen_thread", target=mp.receive_message)
    listen_thread.start()

    CLIENT_PROMPT = "[&]"

    while True:
      try:
        inp = raw_input(CLIENT_PROMPT)
      except EOFError:
        break
      sinput = inp.split()
      cmd = sinput[0]
      if cmd == 'send':
        dest = sinput[1]
        kind = sinput[2]
        txt = raw_input("Message:")
        new_msg_s = TimeStampedMessage(mp.local_name, dest, kind, txt)
        mp.send(new_msg_s)
      elif cmd == 'receive':
        new_msg_r = mp.receive()
        if new_msg_r is not None:
          print new_msg_r.src, "|", new_msg_r.kind, "|", new_msg_r.data
        else:
          print "No new messages"
      else:
        print "usage: send <dest> <kind> | receive"
  else:
    print >>sys.stderr, 'usage: lab0.py <config_filename> <local_name>'