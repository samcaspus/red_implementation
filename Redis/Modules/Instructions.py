
from datetime import datetime
from dateutil import parser
import time
from .FileHandling import FileHandling
delay_time = 20

class Instructions:

    def __init__(self):
        self.get_value = "value"
        self.creation_time = "creation_time"
        self.ttl = "ttl"
        self.lock = "lock"
        
        self.FileHandling = FileHandling()

    def wait_for_lock_to_unlock(self,usersInstruction,reddisData,command,key,value):
        if key in reddisData and self.lock in reddisData[key]:
            reddisData = self.FileHandling.temp_read()
            count = 0
            while reddisData[key][self.lock]:
                reddisData = self.FileHandling.temp_read()
                time.sleep(1)
                count+=1
                if count == delay_time:
                    return False
        return True

            
    def lrange(self,key,value,usersInstruction,reddisData,command):

        value = [int(p) for p in value.split()]
        start = value[0]
        end = value[1]
        if key not in reddisData:
            return "(nil) key not present",reddisData
        result = ""

        for i in range(start,end):
            result+=reddisData[key][self.get_value][i]+" "

        return result,reddisData

    def rpop(self,key,value,usersInstruction,reddisData,command):

        if key not in reddisData:
            return "(nil) key not present",reddisData
        
        reddisData[key][self.get_value].pop()
        
        return "OK",reddisData

    def lpop(self,key,value,usersInstruction,reddisData,command):

        if key not in reddisData:
            return "(nil) key not present",reddisData
        
        reddisData[key][self.get_value].pop(0)
        
        return "OK",reddisData

    def llen(self,key,value,usersInstruction,reddisData,command):            
        if key not in reddisData:
            return "(nil) key not present",reddisData
        
        return len(reddisData[key][self.get_value]),reddisData

    def rinsert(self,key,value,usersInstruction,reddisData,command):
        value = value.split() 
        if key not in reddisData:
            return "(nil) key not present",reddisData
        
        if value[0] not in reddisData[key][self.get_value]:
            return "(element not present to insert before)"
        else:
            reddisData[key][self.get_value].insert(reddisData[key][self.get_value].index(value[0])+1,value[1])
            return "OK",reddisData

    def linsert(self,key,value,usersInstruction,reddisData,command):
        value = value.split() 

        if key not in reddisData:
            return "(nil) key not present",reddisData
        
        if value[0] not in reddisData[key][self.get_value]:
            return "(element not present to insert before)"
        else:
            reddisData[key][self.get_value].insert(reddisData[key][self.get_value].index(value[0]),value[1])
            return "OK",reddisData

    def lindex(self,key,value,usersInstruction,reddisData,command):
        if key not in reddisData:
            return "(nil) key not present",reddisData
        
        value = int(value)
        if value>=len(reddisData[key][self.get_value]):
            return "(out of bounds)",reddisData
        
        return reddisData[key][self.get_value][value],reddisData

    def lpush(self,key,value,usersInstruction,reddisData,command):
        value = value.split()
        if key not in reddisData:
            reddisData[key] = {}
        
        if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
            return "(resource utilised by another user)",reddisData
        else:
            reddisData[key][self.lock] = True
            self.FileHandling.temp_save(reddisData)
            if self.get_value in reddisData[key]:
                reddisData[key][self.get_value]=value + reddisData[key][self.get_value]
            else:
                reddisData[key][self.get_value]=value

            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ""
            
            reddisData[key][self.lock] = False
            self.FileHandling.temp_save(reddisData)
            return "OK",reddisData

    def rpush(self,key,value,usersInstruction,reddisData,command):
        value = value.split()
        if key not in reddisData:
            reddisData[key] = {}
        
        if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
            return "(resource utilised by another user)",reddisData
        else:
            reddisData[key][self.lock] = True
            self.FileHandling.temp_save(reddisData)
            if self.get_value in reddisData[key]:
                reddisData[key][self.get_value]+=value
            else:
                reddisData[key][self.get_value]=value

            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ""
            
            reddisData[key][self.lock] = False
            self.FileHandling.temp_save(reddisData)
            return "OK",reddisData

    def exitt(self,key,value,usersInstruction,reddisData,command):
        return "BYE",reddisData


    def get(self,key,value,usersInstruction,reddisData,command):
    
        if key in reddisData:
            timeToLive = reddisData[key][self.ttl]

            if timeToLive=='':
                timeToLive = 1000
            else:
                timeToLive = int(reddisData[key][self.ttl])
            
            
            creationTime = parser.parse(reddisData[key][self.creation_time])
            today = datetime.today()
        
        
        if key in reddisData and  timeToLive != "" and timeToLive >= (today - creationTime).seconds:

            return reddisData[key][self.get_value],reddisData
        else:
            if key in reddisData:
                del reddisData[key]
            
            return "(nil)",reddisData


    def sett(self,key,value,usersInstruction,reddisData,command):
        if key not in reddisData:

            reddisData[key] = {}
        
        if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
            return "(resource utilised by another user)",reddisData
        else:            
            reddisData[key][self.lock] = True
            self.FileHandling.temp_save(reddisData)
            
            reddisData[key][self.get_value] = value
            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ""
            
            reddisData[key][self.lock] = False
            self.FileHandling.temp_save(reddisData)


            return "OK",reddisData


    def expire(self,key,value,usersInstruction,reddisData,command):

        if key not in reddisData or value.isdigit()==False:
            
            return "(key not present)",reddisData
        else:
            if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
                return "(resource utilised by another user)",reddisData
            else:
                reddisData[key][self.lock] = True
                self.FileHandling.temp_save(reddisData)
            
                reddisData[key][self.creation_time] = str(datetime.today())
                reddisData[key][self.ttl] = value
                
                reddisData[key][self.lock] = False
                self.FileHandling.temp_save(reddisData)
            
                return "OK",reddisData

    def zadd(self,key,value,usersInstruction,reddisData,command):
            
        if key not in reddisData:
            reddisData[key] = {}
            reddisData[key][self.get_value] = []
        
        data = value.split()

        if len(data)%2!=0:
            return "(key or value issue)",reddisData


        final_redis_pair_Data = []

        for i in range(0,len(data),2):
            final_redis_pair_Data.append([data[i],data[i+1]])

        if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
            return "(resource utilised by another user)",reddisData
        else:
            reddisData[key][self.lock] = True
            self.FileHandling.temp_save(reddisData)
            try:
                
                for DataInRedis in final_redis_pair_Data:
                    if DataInRedis not in reddisData[key][self.get_value]:
                        reddisData[key][self.get_value] += [DataInRedis]
                
                # reddisData[key][self.get_value] = list(set(reddisData[key][self.get_value]))
                reddisData[key][self.get_value].sort()
                reddisData[key][self.creation_time] = str(datetime.today())
                reddisData[key][self.ttl] = ""
                reddisData[key][self.lock] = False
                self.FileHandling.temp_save(reddisData)
            except:
                reddisData[key][self.lock] = False
                self.FileHandling.temp_save(reddisData)
                return "(Error wrong format data processed)",reddisData
            
            return "OK",reddisData
            

    
    def zrange(self,key,value,usersInstruction,reddisData,command):
        if key not in reddisData:
            return "(nil)",reddisData
        
        if type(reddisData[key][self.get_value]) != type([]):
            return "(Range type not in value score pair)",reddisData

        instructions = value.split()
        count = 0
        content = reddisData[key][self.get_value]
        
        start = int(instructions[0])
        end = int(instructions[1])

        
        #### if the range is invalid
        
        if start > len(content) or end > len(content):
            return "(Illegal range)",reddisData    
        
        if end==-1:
            end = len(content)+1
        

            ##### print key word has withscores then implement the display with scores
            
            if "withscores" in instructions:

                count = 0
                try:
                    
                    for i in range(start,end):
                        count+=1
                        print(count,") ",content[i][1])
                        count+=1
                        print(count,") ",content[i][0])
                    return "",reddisData
                    
                except:
                    return "",reddisData    

            else:
                
                try:    

                    for i in range(start,end):
                        count+=1
                        print(count,") ",content[i][0])
                    return "",reddisData
                
                except:
                    return "",reddisData
    

    def zrank(self,key,value,usersInstruction,reddisData,command):
        if key not in reddisData:
            return "(nil)",reddisData
        
        value = value.split()
        possition = 0

        for content in reddisData[key][self.get_value]:

            if content == value:
                return possition,reddisData
            
            possition+=1


        return "(nil)", reddisData

    def exist(self,key,value,usersInstruction,reddisData,command):
        if key not in reddisData:
            reddisData[key] = {}

        if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
            return "(resource utilised by another user)",reddisData
        else:
            reddisData[key][self.lock] = True
            self.FileHandling.temp_save(reddisData)
            

            reddisData[key][self.get_value] = ''
            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ''
            

            reddisData[key][self.lock] = False
            self.FileHandling.temp_save(reddisData)
            
            return "OK",reddisData
    
    def append(self,key,value,usersInstruction,reddisData,command):
        if key not in reddisData:
            return "(key not present)",reddisData
        
        if not self.wait_for_lock_to_unlock(usersInstruction,reddisData,command,key,value):
            return "(resource utilised by another user)",reddisData
        else: 
            reddisData[key][self.lock] = True
            self.FileHandling.temp_save(reddisData)
            
            reddisData[key][self.get_value]+=value+" "
            
            reddisData[key][self.lock] = False
            self.FileHandling.temp_save(reddisData)
            
        
        return "OK",reddisData


        
    def getrange(self,key,value,usersInstruction,reddisData,command):
        value = [int(p) for p in value.split()]
        start = value[0]
        end = value[1]

        if key not in reddisData:
            return "(key not present)",reddisData

        try:
            return reddisData[key][self.get_value][start:end],reddisData
        except:
            return "Illegal range or operation cannot be performed in this type of data",reddisData
    

    def dell(self,key,value,usersInstruction,reddisData,command):
        values = key.split()+value.split()

        key_words_not_present = []

        for key_word in values:
            if key_word in reddisData:
                del reddisData[key_word]
            else:
                key_words_not_present.append(key_word)

        if len(key_words_not_present) == 0:
            return "(deleted) "+str(len(values))+" items",reddisData
        else:
            return "(deleted) "+str(len(values)-len(key_words_not_present))+" items \n data not present to delete =>"+str(key_words_not_present),reddisData

    def ttll(self,key,value,usersInstruction,reddisData,command):

        if key not in reddisData:
            return "(nil) key does not exist",reddisData

        if key in reddisData:
            timeToLive = reddisData[key][self.ttl]

        if timeToLive!='':
            timeToLive = int(reddisData[key][self.ttl])
        
        
        creationTime = parser.parse(reddisData[key][self.creation_time])
        today = datetime.today()
        
        
        if key in reddisData and  timeToLive != "" and timeToLive >= (today - creationTime).seconds:

            return  str(timeToLive - (today - creationTime).seconds)+" seconds left" ,reddisData
        
        else:
            if key in reddisData and timeToLive=='0':
                del reddisData[key]
            
            return "(nil) key does not exist",reddisData


        
        return reddisData[key][self.ttl],reddisData
    

    def execute_instruction(self,usersInstruction,reddisData):

        command = usersInstruction[0].lower().strip()
     
        ##### setting up everything
        try:
            key = usersInstruction[1]
            value = ""
            for val in usersInstruction[2:]:
                value+=val+" "
            value = value.strip()

        except:
            pass

        
        instruction = {
            "lrange" : self.lrange,
            "rpop" : self.rpop,
            "lpop" : self.lpop,
            "llen" : self.llen,
            "rinsert": self.rinsert,
            "linsert" : self.linsert,
            "lindex" : self.lindex,
            "rpush" :self.rpush,
            "lpush" : self.lpush,
            "get": self.get,
            "set" : self.sett,
            "zadd": self.zadd,
            "zrange" : self.zrange,
            "zrank" : self.zrank,
            "exist": self.exist,
            "append":self.append,
            "getrange": self.getrange,
            "del" : self.dell,
            "ttl" : self.ttll,
            "expire":self.expire,
            "exit" : self.exitt,

        }

        if command in instruction:
            if command == "exit":
                return "exit",reddisData
            return instruction[command](key,value,usersInstruction,reddisData,command)
        else:
            return "error" ,reddisData



    ##### behaviour for various instructions 
    def behave(self,instructionResponse):
        
        if instructionResponse=="exit":
        
            return False
         
        elif instructionResponse=="error":
        
            print("Error Code not recognised\n")
            
        else:
            print(instructionResponse,"\n")
            
        return True

        
