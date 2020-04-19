import json

class FileHandling:

    def read_json_data(self):
        print("\nRedis up\n")
            
        try:
            self.file = open("demo.json","r")
            self.content = self.file.read()
            self.redis_data = json.loads(self.content)
            self.file.close()

            return self.redis_data
        except:
            self.file.close()
            return {}

    def write_json_data(self,redis_data):
        
        self.redis_data = redis_data
        print("\nRedis down (data stored in demo.json)")
        self.file = open("demo.json","w")
        self.content = json.dumps(self.redis_data, sort_keys=True, indent=4, separators=(",", ": "))
        self.file.write(self.content)
        self.file.close()

    def temp_save(self,redisData):
        self.redis_data = redisData
        self.file = open("demo.json","w")
        self.content = json.dumps(self.redis_data, sort_keys=True, indent=4, separators=(",", ": "))
        self.file.write(self.content)
        self.file.close()

    def temp_read(self):
        try:
            self.file = open("demo.json","r")
            self.content = self.file.read()
            self.redis_data = json.loads(self.content)
            self.file.close()

            return self.redis_data
        except:
            self.file.close()
            return {}
        
