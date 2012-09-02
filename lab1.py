import sys
import threading

from messagepasser import MessagePasser
from message import TimeStampedMessage
import ClockServiceFactory

logger_messages = []

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
    # cs = cs_factory.get_clock("LOGICAL")
    # Uncomment for Vector Clock
    cs = cs_factory.get_clock("VECTOR")
    
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
      if cmd == 'dump':
        # Receive ALL outstanding messages
        msg = mp.receive()
        while msg is not None:
          logger_messages.append(msg)
          msg = mp.receive()
        
        # Sort the logger messages according to their timestamp
        logger_messages.sort(cmp=lambda x,y: cs.compare(x.timestamp, y.timestamp))

        # Dump the messages to a file
        fp = open("logger.txt", "w")
        for item in logger_messages:
          fp.write(item.timestamp + "-" + item.src + "-" + item.kind + "\n")
        fp.close()
      else:
        print "usage: dump"
  else:
    print >>sys.stderr, 'usage: lab1.py <config_filename> <local_name>'