

# from datetime import datetime
# from dateutil import parser

# class ExecutionHandler:

#     def redis_exit(self,usersInstruction,reddisData):
#         return "exit",reddisData

#     def redis_get(self,usersInstruction,reddisData):

#         if key in reddisData:
#             timeToLive = reddisData[key][self.ttl]

#             if timeToLive=='':
#                 timeToLive = 1000
#             else:
#                 timeToLive = int(reddisData[key][self.ttl])
            
            
#             creationTime = parser.parse(reddisData[key][self.creation_time])
#             today = datetime.today()
        
        
#         if key in reddisData and  timeToLive != "" and timeToLive >= (today - creationTime).seconds:

#             return reddisData[key][self.get_value],reddisData
#         else:
#             if key in reddisData:
#                 del reddisData[key]
            
#             return "(nil)",reddisData


