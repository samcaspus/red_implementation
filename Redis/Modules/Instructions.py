
from datetime import datetime
from dateutil import parser

class Instructions:

    def __init__(self):
        self.get_value = "value"
        self.creation_time = "creation_time"
        self.ttl = "ttl"
        


    def execute_instruction(self,usersInstruction,reddisData):


        command = usersInstruction[0].lower()
     
        ##### setting up everything
        try:
            key = usersInstruction[1]
            value = ""
            for val in usersInstruction[2:]:
                value+=val+" "
            value = value.strip()

        except:
            pass

     
          
        ##### for exit purpose
        usersInstruction,reddisData
        if command == "exit":
            
            return "exit",reddisData

     
     
        ##### To get the data from the key

        if command == "get":
            
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



        ##### To set a data to redis                    
        if command == "set":
            if key not in reddisData:
               
                reddisData[key] = {}
                        
            reddisData[key][self.get_value] = value
            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ""


            return "OK",reddisData




        ##### expire command used to set a ttl (time to live)

        if command == "expire":
            if key not in reddisData or value.isdigit()==False:
                
                return "(key not present)",reddisData
            else:
                
                reddisData[key][self.creation_time] = str(datetime.today())
                reddisData[key][self.ttl] = value
                if value=="0":
                    del reddisData[key]
                
                return "OK",reddisData
            


        ###### adding elements in pair groups of set and storing them
        if command == "zadd":
            
            if key not in reddisData:
                reddisData[key] = {}
                reddisData[key][self.get_value] = []
            
            data = value.split()

            if len(data)%2!=0:
                return "(key or value issue)",reddisData


            final_redis_pair_Data = []

            for i in range(0,len(data),2):
                final_redis_pair_Data.append([data[i],data[i+1]])

            reddisData[key][self.get_value] += final_redis_pair_Data
            # reddisData[key][self.get_value] = list(set(reddisData[key][self.get_value]))
            reddisData[key][self.get_value].sort()
            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ""
            
            return "OK",reddisData
            


        ###### displaying the content added using zadd
        if command == "zrange":

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

        ####### to get the location of the element present in the list

        if command == "zrank":
            
            if key not in reddisData:
                return "(nil)",reddisData
            
            value = value.split()

            possition = 0

            for content in reddisData[key][self.get_value]:

                if content == value:
                    return possition,reddisData
                
                possition+=1


            return "(nil)", reddisData


        ###### making a key exist 
        if command =="exist":

            if key not in reddisData:
                reddisData[key] = {}

            reddisData[key][self.get_value] = ''
            reddisData[key][self.creation_time] = str(datetime.today())
            reddisData[key][self.ttl] = ''
            
            return "OK",reddisData


        ####### String append instruction
        if command == "append":

            if key not in reddisData:
                return "(key not present)",reddisData
            
            reddisData[key][self.get_value]+=value+" "
            return "OK",reddisData


        
        ######## get string range from one index to the other

        if command == "getrange":
            
            value = [int(p) for p in value.split()]
            start = value[0]
            end = value[1]

            if key not in reddisData:
                return "(key not present)",reddisData

            try:
                return reddisData[key][self.get_value][start:end],reddisData
            except:
                return "Illegal range or operation cannot be performed in this type of data",reddisData


        ##### to delete a key or group of keys
        if command == "del":

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


        ###### Final call if no options match       
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

        
