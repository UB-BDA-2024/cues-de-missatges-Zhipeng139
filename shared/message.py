import json

class MessageStrcuture:
    def __init__(self, action_type, data):
        self.action = action_type
        self.data = data
    
    def __str__(self):
        return f"action: {self.action}, data: {self.data}"
    
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    
    def to_dict(self):
        return {"action": self.action, "data": self.data}
    
    def keys(self):
        return self.data.keys()
    
    def values(self):
        return self.data.values()
    
    def items(self):
        return self.data.items()
    
    def get(self, key):
        if key in self.data:
            return self.data[key]
        else:
            return None

