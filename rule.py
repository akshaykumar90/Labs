class Rule(object):
  """Base class for SendRule and ReceiveRule"""
  def __init__(self, action, src, dest, kind, id, nth):
    super(Rule, self).__init__()
    self.action = action
    self.src = src
    self.dest = dest
    self.kind = kind
    self.id = id
    self.nth = nth
    self.matched = 0

  def match(self,message):
    if self.src is not None and self.src != message.src:
      return False
    if self.dest is not None and self.dest != message.dest:
      return False
    if self.kind is not None and self.kind != message.kind:
      return False
    if self.id is not None and self.id != message.id:
      return False

    # If we have made this far, we have already matched
    self.matched += 1

    if self.nth is not None and self.matched % self.nth != 0:
      return False

    # By this point, we are safe to return True
    return True

class SendRule(Rule):
  """abstraction for a SendRule"""
  def __init__(self, action, src, dest, kind, id, nth):
    super(SendRule, self).__init__(action, src, dest, kind, id, nth)

class ReceiveRule(Rule):
  """abstraction for a ReceiveRule"""
  def __init__(self, action, src, dest, kind, id, nth):
    super(ReceiveRule, self).__init__(action, src, dest, kind, id, nth)