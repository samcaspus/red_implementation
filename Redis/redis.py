import json
from Modules.Instructions import Instructions
from Modules.FileHandeling import FileHandeling
from threading import Thread


class Redis(Thread):
    '''
    importing all necessary header files

    '''
    # Commenting out for threading purpose

    # def __init__(self):
    #     self.Instructions = Instructions()
    #     self.FileHandeling = FileHandeling()

    ##### main method for redis to run

    def run(self):
        self.Instructions = Instructions()
        self.FileHandeling = FileHandeling()
        '''
        cut 1:

        If an Error is caused while execution of redis the code terminates
        
        cut 2:

        Take instruction as input in single line
        dividing it into a list 
        running it through instruction engine
        
        '''
        self.redisData = self.FileHandeling.read_json_data() 
            

        while True:
            self.redisData = self.FileHandeling.temp_read()
            usersInstruction = input(">>> ").split()
            #### collecting the response and the redis dictionary

            self.instructionResponse,self.redis_data = self.Instructions.execute_instruction(usersInstruction,self.redisData)

            if not self.Instructions.behave(self.instructionResponse):
                break
            
            
            #### below line can be commented if temp save is to be disabled
            self.FileHandeling.temp_save(self.redisData)

        self.FileHandeling.write_json_data(self.redisData)

    
redisObject = Redis()
redisObject.start()
