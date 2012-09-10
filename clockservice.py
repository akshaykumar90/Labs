class ClockServiceFactory(object):
  """Factory for Logical and Vector Clocks"""
  def __init__(self):
    super(ClockServiceFactory, self).__init__()

  def set_nodes(self, n):
    self.nodes = n

  def set_host_node_id(self, id):
    self.id = id

  def get_clock(self, ctype):
    if ctype == "LOGICAL":
      return LogicalClock()
    elif ctype == "VECTOR":
      return VectorClock(self.id, self.nodes)

class LogicalClock(object):
  """docstring for LogicalClock"""
  def __init__(self):
    super(LogicalClock, self).__init__()
    self.timestamp = 0

  def clock_tick(self):
    self.timestamp += 1
    return self.timestamp

  def adjust_clock(self, tm):
    self.timestamp = max(self.timestamp, tm) + 1
    return self.timestamp

class VectorClock(object):
  """docstring for VectorClock"""
  def __init__(self, id, nodes):
    super(VectorClock, self).__init__()
    self.nodes = nodes # No. of nodes in the system
    self.id = id  # Identifier of *this* node
    self.timestamp = [0] * nodes # Vector clock

  def clock_tick(self):
    self.timestamp[self.id] += 1
    return self.timestamp

  def clock_tick_j(self, j):
    self.timestamp[j] += 1
    return self.timestamp

  def adjust_clock(self, tm):
    for i,t in enumerate(tm):
      self.timestamp[i] = max(self.timestamp[i], t)
    return self.timestamp

  def __lessthanequalto(self, tl, tr):
    result = True
    for i,t in enumerate(tr):
      result = result and (tl[i] <= t)
    return result

  def __notequalto(self, tl, tr):
    result = False
    for i,t in enumerate(tr):
      result = result or (tl[i] != t)
    return result

  def __lessthan(self, tl, tr):
    return self.__lessthanequalto(tl,tr) and self.__notequalto(tl,tr)

  def compare(self, tl, tr):
    if self.__lessthan(tl, tr):
      return -1
    elif self.__lessthan(tr, tl):
      return 1
    else:
      return 0

  def co_next(self, vj, j):
    """Validates whether timestamp `vj` of a process `j` comes exactly next in 
       causal order using timestamp of `i` (this) process
       This holds true only when vj[j] = vi[j] + 1 and vj[k] <= vi[k] (k /= j)
    """
    if vj[j] != self.timestamp[j] + 1:
      return False

    result = True
    for k,t in enumerate(self.timestamp):
      if k != j: result = result and (vj[k] <= t)
    return result
