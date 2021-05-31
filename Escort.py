from time import sleep
import datetime
from random import random, randint
import threading
from threading import Event 
from collections import Counter
import math
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from matplotlib.animation import FuncAnimation




#######################################################################################
################################## 1-Log Entry Class ####################################
#######################################################################################
class LogEntry():
    
    
    def __init__(self, command, entry, TermNumber, commitIndex):

    	self.command = command
    	self.entry = entry
    	
    	# unique LOG identifiers
    	self.TermNumber = TermNumber
    	self.commitIndex = commitIndex
    	
    
    def print_entry(self):
    	return f"LOG[#Term={self.TermNumber}, #commitIndex={self.commitIndex}: Command={self.command} → Entry={self.entry}"
    	

#######################################################################################
################################## 2-Node Class #########################################
#######################################################################################
class Node():
    
    def __init__(self, ID, verbose=True):
        
        self.ID = ID
        self.TermNumber = 0
        
        self.LOG = []
        self.commitLength = 0
        self.sentLength = {} #length of log we sent to follower
        self.ackdLength = {} #length of log acknwoledged to be received by follower
        
        #self.commitIndex = 1
        self.votedFor = None
        self.Leader = None
        self.heartbeat = True
        self.timeout = None
        self.followers = None
        self.lastTimeStamp = None
        self.lastTimeStampString = None
        self.verbose = True
        self.coordinates=(randint(0, 100),randint(0, 100))
        
            
    
    ##################
    # PUBLIC FUNCTIONS 
    ##################
    def print_node(self, print_fn=True, additional=""):
        info = f"ID = {self.ID} | TermNumber = {self.TermNumber} | followers = {len(self.followers)} | {additional}"
        if print_fn:
            print(info)
        else:
            return info
        
    def get_timestamp(self):
        now = datetime.datetime.now()
        timestamp = f"[{now.day}-{now.month}-{now.year} {now.hour}:{now.minute}:{now.second}.{int(str(now.microsecond)[:3])}]"
        return now, timestamp


#######################################################################################
################################## 3-follower Class #########################################
#######################################################################################
class Follower(Node):
                
    def __init__(self, ID, verbose=True):
        super().__init__(ID)
        self.state = 'Follower' 
        self.interrupt_countdown = Event()
        self.countdown_stopped = True #for mutex
        self.votesReceived = []
        self.verbose = verbose
         
    #*****************
    # PUBLIC FUNCTIONS
    #*****************
    
    def reset_timeout(self, followers):
        """
        Behavior:     
            reset timeout of the Follower by:
             - The LEADER, OR
             - At initialization
        """
        
        #1. Kill the previous timeout thread
        self.interrupt_countdown.set()    #countdown interrupt button is clicked (True)
        
        # MUTEX LOCK the interrupt_countdown Event
        while self.countdown_stopped == False:
            continue;       
        
        # Unclick the button
        self.interrupt_countdown.clear()  #False
            
        #2. Start a new timeout thread 
        self.followers = followers
        reset_follower_timeout_thread = threading.Thread(target=self._follower_on,
                                                         name=f'Thread [ID={self.ID}] Timeout Reset')
        reset_follower_timeout_thread.start()
       
             
    #Recovery on crash       
    def become_follower_again(self, leader_object):
        """
        Behavior: 
            Copy my information from when I was a Leader to a Follower object
        Return: Follower object
        """
               
        # 1. COPY THE CONTENT OF THE LEADER
        self.LOG = leader_object.LOG
        self.TermNumber = leader_object.TermNumber
        self.followers = leader_object.followers
        self.commitLength = leader_object.commitLength
        
        # 2. RESET THE FOLLOWER'S INITIAL PROPERTIES
        # The Follower:
        # - waits for a new Leader
        # - to reset its timeout and restart it.
        # - Meanwhile, it remains idle
        self.state = 'Follower'
        self.votedFor = None
        self.Leader   = None 
        self.interrupt_countdown = Event()
        self.countdown_stopped = True #for mutex
        self.votesReceived = []
        self.sentLength = {}
        self.ackdLength = {}
         
        # Debug Printing
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        self.print_node(self.lastTimeStampString) #PRINT
              
        return self          
     
    #################
    #RECEIVE FUNCTION
    #################
    def receive(self, RPC,
                      sender, newEntries,   sender_TermNumber,
                      sender_logLength,     f_logLength_from_s,
                      sender_commitLength,  sender_prevLogTerm, vote):
        """
        RECEIVE FUNCITON
        """
        if self.state == 'Candidate':          
            if RPC == 'VoteResponse':
                VoterForMe = sender
                v_term     = sender_TermNumber
                self._c_on_receive_votes(VoterForMe, v_term, vote)
        
        
        if self.state == 'Follower':
            if RPC == 'heartbeat':
                leader = sender
                L_TermNumber = sender_TermNumber
                self._on_receive_heartbeat(leader, L_TermNumber)
                
            if RPC == 'RequestVotes':
                candidate             = sender
                candidate_TermNumber  = sender_TermNumber
                candidate_logLength   = sender_logLength
                candidate_lastLogTerm = sender_prevLogTerm            
                self._on_receive_vote_request(candidate, candidate_TermNumber, candidate_logLength, candidate_lastLogTerm)
                 
            if RPC == 'REPLICATELOG':
                leader             = sender
                L_TermNumber       = sender_TermNumber
                f_logLength_from_L = f_logLength_from_s
                L_commitLength     = sender_commitLength
                L_prevLogTerm      = sender_prevLogTerm
                self._on_receive_replicate_log(leader, L_TermNumber, f_logLength_from_L, L_commitLength, L_prevLogTerm, newEntries)
            
     
    ################
    # SEND FUNCTION
    ################  
    def send(self, RPC='RequestVotes', Candidate=None, vote=None, ack=None, appended=False):
        """
        SEND
            - Only if state == 'Candidate'
            - Only send to Followers
            - Only if state == 'Follower' and RPC == 'VoteResponse' or 'LogResponse'
        """
        if self.state == 'Candidate':
        
            if RPC == 'RequestVotes':
                self._c_RequestVotes()
            
        if self.state == 'Follower': 
            if RPC == 'VoteResponse':
                self._send_vote_responses(Candidate=Candidate, vote=vote) 
       
            if RPC == 'LogResponse':
                self._send_log_response(follower=self, follower_TermNumber=self.TermNumber, ack=ack, appended=appended)
    
    #******************
    # PRIVATE FUNCTIONS
    #******************

    #########################
    # PRIVATE  SEND FUNCTIONS 
    #########################   
    """
    RPC='LogResponse'
    """
    def _send_log_response(self, follower, ack, appended):
        Leader.receive(RPC='LogResponse',
                       follower=self, follower_TermNumber=self.TermNumber,
                       ack=ack, appended=appended)
          
    """
    RPC='VoteResponse'  
    """ 
    def _send_vote_responses(self, Candidate, vote):
        # send my vote to Candidate
        if self.state == 'Follower': 
            Candidate.receive(RPC='VoteResponse',
                              sender=self, newEntries=None,   sender_TermNumber=self.TermNumber,
                              sender_logLength=None,          f_logLength_from_s=None,
                              sender_commitLength=None,       sender_prevLogTerm=None, vote=vote)
    """
    RPC='RequestVotes'  
    """         
    def _c_RequestVotes(self):            
        if self.state == 'Candidate':
        
            # 1. CANDIDATE increments ts term + votes on itself
            self.TermNumber += 1
            self.votedFor = self
            self.votesReceived.append(self)

                        
            # 2. send RequestVotes RPCs to FOLLOWERs
            # Start Election
            lastLogTerm = 0
            if len(self.LOG) >0:
                lastLogTerm = self.LOG[len(self.LOG)-1].TermNumber # 0 if LOG is empty
            
            # Exclude self from the list of voters
            voters = self.followers[:self.ID-1]
            if self.ID < len(self.followers):
                voters += self.followers[self.ID:]
                        
            for follower in voters:
                follower.receive(RPC='RequestVotes', 
                                       sender=self, newEntries=None,   sender_TermNumber=self.TermNumber,
                                       sender_logLength=len(self.LOG), f_logLength_from_s=None,
                                       sender_commitLength=None,       sender_prevLogTerm=lastLogTerm, vote=None)
     
    ###########################
    # PRIVATE RECEIVE FUNCTIONS
    ###########################
    """
    RPC = 'REPLICATELOG'
    """
    def _on_receive_replicate_log(self, leader, L_TermNumber, f_logLength_from_L, L_commitLength, L_prevLogTerm, newEntries):
        #Follower receives entries from the Leader
        #We want our Term to be == to the Leader's Term
        #This means that all our latest LOG entries are consistent with the Leader 
        
        #1. Compare Terms
        if L_TermNumber > self.TermNumber:
            self.TermNumber = L_TermNumber
            self.votedFor = None
            self.Leader = leader
            self.state = 'Follower'
        
        
        if (L_TermNumber == self.TermNumber) and (self.state == 'Candidate'):
            self.state = 'Follower'
            self.Leader = leader
        
        #2. If Terms are the same, check if the LOG is the same
        #   Otherwise, send to the Leader to give us earlier LOG entries
        logAssert  = ((len(self.LOG) >=  f_logLength_from_L) and
                      ((f_logLength_from_L == 0) or 
                       (L_prevLogTerm == self.LOG[f_logLength_from_L-1].TermNumber)))
                      

        ack = 0
        appended = False
        
        #3. If Term is ok and LOG is ok, Follower appends to its LOG
        #  And send to the Leader the length of the acknolwedged LOG
        if L_TermNumber == self.TermNumber and logAssert:
            self.append_entries(f_logLength_from_L, L_commitLength, newEntries)
            ack = f_logLength_from_L + len(newEntries) # we received LOG up to ack length
        
        self.send(RPC='RequestVotes', Candidate=None, vote=None, ack=ack, appended=appended)
                     
    """
    RPC='heartbeat'  
    """                     
    def _on_receive_heartbeat(self, leader, Leader_TermNumber):
        """
        When Follower receives heartbeat, it
        kills its previous thread and restarts
        a new one with a new timeout.
        """
        
                            
        # Follower rejects Leader's heartbeat
        # If the Leader is stale, Follower becomes a Candidate
        if Leader_TermNumber < self.TermNumber:
            self.votedFor = None
            self.Leader = None
            
        # Follower sets the Leader to the Term's Leader
        # After receiving a heartbeat from it 💓
        else:
            self.Leader = leader
            self.state = 'Follower'
            self.reset_timeout(self.followers)
            

            
    """
    RPC='RequestVote'  
    """ 
    def _on_receive_vote_request(self, candidate, candidate_TermNumber, candidate_logLength, candidate_lastLogTerm):
        """
        Follower votes on a Candidate if it has
        a higher term number + an updated LOG + has not already voted
        If so, set the vote of this Follower to the Candidate.
        """

        vote = False 
        myLogTerm = 0
        
        if len(self.LOG) > 0:  
            myLogTerm = self.LOG[len(self.LOG)-1].TermNumber # 0 if LOG is empty
        
        logAssert  = ((candidate_lastLogTerm >  myLogTerm) or\
                     ((candidate_lastLogTerm == myLogTerm) and (candidate_logLength>=len(self.LOG))))
                     
        termAssert = ((candidate_TermNumber >  self.TermNumber) or\
                     ((candidate_TermNumber == self.TermNumber) and (self.votedFor in [None, candidate])))
        

        if logAssert and termAssert:
            self.TermNumber = candidate_TermNumber #MAKE v_TermNumber == c_TermNumber
            self.state = 'Follower'           
            self.votedFor = candidate
            vote = True       
        
        # send vote response
        self.send(RPC='VoteResponse', Candidate=candidate, vote=vote)
                
    """
    RPC='VoteResponse'  
    """ 
    def _c_on_receive_votes(self, voter, v_term, vote):
        if self.state == 'Candidate':
        
            if v_term == self.TermNumber and vote:
                #SUCESSFUL VOTE
                self.votesReceived.append(voter) #append new voter
                
                if len(self.votesReceived) >= math.ceil((len([self.followers])+1)/2): # This avoids split-vote          
                    voters = [v.ID for v in self.votesReceived]    
                    self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
                    
                    print("\n###################################################"\
                         f"\n[ID={self.ID}] Elections Ended and now I'm LEADER!"\
                         f"\n   my voters={voters} | {self.lastTimeStampString}"\
                          "\n###################################################\n")
                   
                    self.state = 'Leader'
                                     
                    # Exclude self from the list of voters
                    voters = self.followers[:self.ID-1]
                    if self.ID < len(self.followers):
                        voters += self.followers[self.ID:]
                        
                    for follower in voters:
                        self.sentLength[follower] = len(self.LOG)  #length of log we sent to follower
                        self.ackdLength[follower] = 0              #length of log acknwoledged to be received by follower
                        
                    # convert to Leader object
                    self.Leader = Leader.become_leader(self)
                    # Send heartbeats + REPLICATE LOG
                    self.Leader.leader_begin()
                    #self.Leader.send('REPLICATELOG')
                    sleep(2)
                    
                                                
                        
            elif v_term > self.TermNumber:
                #UNSUCCESSFUL VOTE
                if self.verbose:
                    print(f"[ID{self.ID}] I did not become a Leader :( ")
                self.TermNumber = v_term
                self.state = 'Follower'
                self.votedFor = None    
                # Finish Election
                
           
   
    #************************
    # OTHER PRIVATE FUNCTIONS 
    #************************     
       
          
    def _start_countdown(self):
        """
        Behavior
            while stop_countdown is not set yet
            or timeout did not reach the end yet
            keep the countdown
        """
        mini, maxi = (3,6) #mini, maxi = (150*1e-3,300*1e-3)
        self.timeout = randint(mini, maxi) #self.timeout = random()*(maxi-mini)+mini
        sec = 0
        
        while sec < self.timeout and (self.interrupt_countdown.is_set() == False):
            self.interrupt_countdown.wait(1) #WAIT
            sec+=1
        
    

    def _follower_on(self):      
        """
        Behavior:
            Follower starts random timeout
            and becomes a Candidate as soon as
            it reaches the end of its timeout if 
            it doesn't receive a hearbeat
        """  
        
        #1. Assert Follower Properties
        self.state = 'Follower'
        self.countdown_stopped = False
        
        # Print for Debugging
        self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
        
        #2. Start Timeout countdown   
        self._start_countdown() # < < < < < < < (WAIT)
        
        
        #3.a Check if a new Leader was elected
        if self.interrupt_countdown.is_set() == True:
        # If the timeout was interrupted
        # It means a new LEADER was elected
        # Follower waits until reset_timeout() is called at heartbeat RPC

            self.countdown_stopped = True
            sleep(0.01)
            return
        
        #3.b Else, become a Candidate and collect votes  
        else:  
            # This means that the timeout was reached  
            
            # print for Debugging
            self.lastTimeStamp, self.lastTimeStampString = self.get_timestamp()
            
            if self.verbose:
                print(f"\n Node {self.ID} is Candidate now.")

            self.state = 'Candidate'      
            self.Leader = None
            self.votedFor = None
                        
            # The Candidate will immediately increment its TermNumber 
            # and send RequestVotes RPCs to the rest of the nodes
            self.send(RPC='RequestVotes')
            
            
    def append_entries(self, f_logLength_from_L, L_commitLength, newEntries):
    
        if len(newEntries) > 0 and len(self.LOG) > f_logLength_from_L:
            #Check if Terms are cosnsitent. For example:
            #If Follower had entries from a Leader that crashed, it may have advanced Terms
            #If the Terms of the same LOG entry are different, we truncate the LOG and
            #discard LOG entries that are not committed
            if self.LOG[f_logLength_from_L].TermNumber != newEntries[0].TermNumber:
                self.LOG = self.LOG[:f_logLength_from_L-1]
        
        if f_logLength_from_L + len(newEntries) > len(self.LOG):
            a = len(self.LOG) - f_logLength_from_L
            b = len(newEntries) - 1
            for i in range(a,b):
                self.LOG.append(newEntries[i])
        
        # At this point, we have a LOG consistent with the LEADER's
        
        if L_commitLength > self.commitLength:
            self.commitLength = L_commitLength
            
    #################      


#######################################################################################
################################## 4-leader Class #########################################
#######################################################################################
class Leader(Node):
	    	
    def __init__(self, ID, verbose=True):
        super().__init__(ID)
        self.candidate_object = None
        self.heartbeat_timeout = 1 #50*1e-3
        self.shutdown_counter = 3 #SIMULATION   
        self.idle = True
        self.my_followers = None
        self.verbose = verbose
        
    
    ################
    # STATIC METHODS
    ################
    @staticmethod
    def become_leader(candidate_object):
    
        leader = Leader(candidate_object.ID)
        leader.candidate_object = candidate_object
        
        # Copy my information from when I was a Follower/Candidate
        leader.commitLength = candidate_object.commitLength
        leader.LOG = candidate_object.LOG
        leader.verbose = candidate_object.verbose
        leader.TermNumber = candidate_object.TermNumber
        leader.followers = candidate_object.followers
        
        # Exclude self from the list of voters
        my_followers = leader.followers[:leader.ID-1]
        if leader.ID < len(leader.followers):
            my_followers += leader.followers[leader.ID:]
        leader.my_followers = my_followers  
        
        leader.votedFor = candidate_object.votedFor
        leader.Leader = leader
        
        leader.sentLength = candidate_object.sentLength
        leader.ackdLength = candidate_object.ackdLength
        
        # Debug Printing
        leader.lastTimeStamp, leader.lastTimeStampString = leader.get_timestamp()
        leader.print_node(leader.lastTimeStampString) #PRINT
        
       
        del candidate_object
        
        return leader
    ################
        
           

    
    ##################
    # PUBLIC FUNCTIONS 
    ##################
    def leader_on_death(self):
       """
       # Copy info from my previous state as a Leader to Follower object
       Return: Follower object
       """
       
       leader_index = self.Leader.ID - 1
       self.followers[leader_index] = self.candidate_object
       self.candidate_object.become_follower_again(self)
       
       return self.candidate_object


    def leader_begin(self):
        infinite_heartbeats = threading.Thread(target=self._leader_on, name = "Thread Leader ❤️ + 📑️")
        infinite_heartbeats.start()
         
    def on_receive_CLIENT_request(self, flag, command='incrementY', reps=100):
        self.idle = False
               
        
        entry = flag 

        # go to zero part
        function2=self.go_Tozero
        step_x=self.coordinates[0]/(reps/5)
        step_y=self.coordinates[1]/(reps/5)

        plt.ion()
        fig, ax = plt.subplots()
        x, y = [],[]
        sc = ax.scatter(x,y)
        plt.xlim(-10,100)
        plt.ylim(-10,100)

        plt.draw()



        for z in range(int(reps/5)):
            #1. LEADER makes the change request from CLIENT

            list=[]
            L=100

            # self.coordinates= [self.coordinates[0], self.coordinates[1]+1]
            list.append(self.coordinates)
            #print(self.coordinates)
            i=0
            for follower in self.my_followers:
                follower.coordinates=[self.coordinates[0],self.coordinates[1]]
                i=i+1
                list.append(follower.coordinates)
            list=np.array(list)

            if z==0:
                x = [point[0] for point in list]
                y = [point[1] for point in list]
            else:
                x = [point[0] for point in entry]
                y = [point[1] for point in entry]
  

            sc.set_offsets(np.c_[x,y])
            fig.canvas.draw_idle()
            plt.pause(0.1)
            entry = function2(step_x,step_y)
            if self.verbose:
                sleep(1) # For simulation
            
            if self.verbose:
                print("############# LOG (Towards Zero flag at (0,0)) ################\n"\
                     f" Coordinates of Points are = \n {entry}...\n"\
                      "##################################\n")
            


            #2. LEADER SAVES THIS TO LOG
            newLogEntry = LogEntry(command, entry, self.TermNumber, self.commitLength) #, self.commitIndex)
            self.LOG.append(newLogEntry)
            self.ackdLength[self] = len(self.LOG)  
            
            #3. LEADER replicates the entry
            self.send(RPC='REPLICATELOG')






        function = self.go_Too100

        for _ in range(reps):
            #1. LEADER makes the change request from CLIENT
            
            entry = function(entry)
            if self.verbose:
                sleep(1) # For simulation
            
            if self.verbose:
                print("############# LOG (Towards 100 flag at (0,100)) ################\n"\
                     f" Coordinates of Points are = {entry}...\n"\
                      "##################################\n")
            
            #2. LEADER SAVES THIS TO LOG
            newLogEntry = LogEntry(command, entry, self.TermNumber, self.commitLength) #, self.commitIndex)
            self.LOG.append(newLogEntry)
            self.ackdLength[self] = len(self.LOG)  
            
            #3. LEADER replicates the entry
            self.send(RPC='REPLICATELOG')
            x = [point[0] for point in entry]
            y = [point[1] for point in entry]
            sc.set_offsets(np.c_[x,y])
            fig.canvas.draw_idle()
            plt.pause(0.1)


        
                
        flag = entry
        self.idle = True     
        return flag
     
    def replicate_log(self, follower):
    
        ids = [k.ID for k,v in self.sentLength.items()]
        
              
        f_logLength_from_L = self.sentLength[follower]
        newEntries = self.LOG[f_logLength_from_L:len(self.LOG)]
        
        # Get Term Number of the latest Log Entry SENT to the Follower
        prevLogTerm = 0 
        if f_logLength_from_L>0: 
            prevLogTerm = self.LOG[f_logLength_from_L-1].TermNumber
        
        # (leaderID, currentTerm, i, prevLogTerm, commitLength, entries)
        follower.receive(RPC='REPLICATELOG',
                         sender=self, newEntries=newEntries,    sender_TermNumber=self.TermNumber,
                         sender_logLength=None,                 f_logLength_from_s=f_logLength_from_L,
                         sender_commitLength=self.commitLength, sender_prevLogTerm=prevLogTerm, vote=None)
     
    ###########################
    # LIST OF CLIENT OPERATIONS
    ###########################
    def go_Too100(self, flag):
        list=[]
        L=100

        
        flag = [flag[0], flag[1]+1]
        self.coordinates= [self.coordinates[0], self.coordinates[1]+1]
        list.append(self.coordinates)
        #print(self.coordinates)
        i=0
        delta_theta=360/len(self.my_followers)
        for follower in self.my_followers:
            follower.coordinates=[self.coordinates[0]+3*math.cos(delta_theta*i*math.pi/180),self.coordinates[1]+3*math.sin(delta_theta*i*math.pi/180)]
            i=i+1
            list.append(follower.coordinates)
            #print(follower.coordinates)
        list=np.array(list)

        return list
    

    def go_Tozero(self, size_x,size_y):
        list=[]
        L=100

        
        
        self.coordinates= [self.coordinates[0]-size_x, self.coordinates[1]-size_y]
        list.append(self.coordinates)
        #print(self.coordinates)
        i=0
        delta_theta=360/len(self.my_followers)
        for follower in self.my_followers:
            follower.coordinates=[self.coordinates[0]+3*math.cos(delta_theta*i*math.pi/180),self.coordinates[1]+3*math.sin(delta_theta*i*math.pi/180)]
            i=i+1
            list.append(follower.coordinates)
            #print(follower.coordinates)
        list=np.array(list)

        return list

    def sum(self, data):
        # Example

        return None
        
    def sub(self, data):
        # Example
        return None
    ###########################   
                       
    ##################
    # RECEIVE FUNCTION
    ##################
    def receive(self, RCP, follower, follower_TermNumber, ack, appended):
        
        if RCP == 'LogResponse':
            if follower_TermNumber == self.TermNumber and self.state == 'Leader':
            
                if appended == True and ack >= ackdLength[follower]:
                    self.sentLength[follower] = ack
                    self.ackdLength[follower] = ack
                    self._CommitLogEntries()
                
                elif self.sentLength[follower] > 0:
                    self.sentLength[follower] -= 1
                    #consistency check
                    #retry with one entry earlier in the LOG (send extera entries)
                    #to fill the GAP between Follower and Leader
                    self.replicate_log(follower)
            
            elif follower_TermNumber > self.TermNumber:
                self.state = 'Follower'
                self.votedFor = None
                self.TermNumber = follower_TermNumber
                
                # Copy my information to my original Follower state
                # AND REPLACE the old Follower object
                self.leader_on_death()
            
                
    #################
    #  SEND  FUNCTION
    #################
    def send(self, RPC='heartebeat'):
        """
        args:
            followers: nodes
            newLogEntry
            commit_indexOfThePrecedingEntry
            TermNumberOfThePrecedingEntry
        return: FOLLOWERS and their responses (True or False)
        """
        if RPC == 'heartbeat':
            self._send_heartbeat_to_all()
        
        if RPC == 'REPLICATELOG': 
            self._replicate_log_to_all()
        
        # responses: rejections or acceptances of the RPC [True/False]
        
        
    #*****************************************************************************************#  
    
    ###################
    # PRIVATE FUNCTIONS
    ###################    
    
    
    
    def _leader_on(self):
            
        # RUNS AS A THREAD
        initial_counter = self.shutdown_counter
        #1. Now I am a LEADER,
        #   I periodically send hearbeats to Followers
        while self.idle:
        
            '''SIMULATE SHUTDOWN OF LEADER'''
            if self.verbose:
                self.shutdown_counter -= 1
                print(f"\n****Shutdown Simulation***** {self.shutdown_counter}/{initial_counter}")
                if self.shutdown_counter == 0:
                    print(f"XXX LEADER [{self.ID}] IS DEAD XXX\n")
                    self.leader_on_death()
                    return
                
            self.send(RPC='heartbeat')    #periodically heartbeat
            

            self.send(RPC='REPLICATELOG') #preiodically replicate
            
            sleep(self.heartbeat_timeout) 
     
    def _CommitLogEntries(self):
        """
        Commit Entries to Leader's LOG
        A LEADER can commit an entry if it's been acknolwedged by a quorum of nodes
        Quorum = more than half of the nodes
        """
        def acks(length):
            """
            Return: the number of nodes tha have an ackdLength >= length
                    i.e. how many nodes acknowledged a log up to "length" or later
            """
            return len([n for n in self.followers if self.ackdLength[n] >= length])
        
       
        minAcks = math.ceil(len([self.followers+1])/2)
        
        # - Look at all the possible LOG lengths
        # - For each length, find those for which
        #   the number of acks is >= to the minimum_quorum
        ready   = [l for l in range(1, len(self.LOG)) if acks(l) >= minAcks]
        
        # - If ready is not an empty set, it means there is
        #   at least one LOG entry that is ready to be committed
        # - max is the latest entry ready to be committed
        if len(ready) != 0 and\
           max(ready) > self.commitLength and\
           self.LOG[max(ready)-1].TermNumber == self.TermNumber: 
            
            # COMMIT
            self.commitLength = max(ready) 
            
                     
    ##########------#########
    # PRIVATE SEND  FUNCTIONS
    ##########-------########         
    
    # OPTION (1): SEND REPLICATE LOG RPCs TO ALL FOLLOWERS
    def _replicate_log_to_all(self):
          
        for follower in self.my_followers:
            self.replicate_log(follower)
                                              
	
    # OPTION (2): SEND HEARTBEATS TO FOLLOWERS
    def _send_heartbeat_to_all(self):              
        for follower in self.my_followers:
            follower.receive(RPC='heartbeat',
                             sender=self, newEntries=None, sender_TermNumber=self.TermNumber,
                             sender_logLength=None,        f_logLength_from_s=None,
                             sender_commitLength=None,     sender_prevLogTerm=None, vote=None)
    
   
    ###################        

#######################################################################################
################################## 5-raft #########################################
#######################################################################################

class Raft():
    
    def __init__(self, n=5, verbose=True):
        self.followers = [Follower(i+1, verbose=verbose) for i in range(n)]
        self.LEADER = None
        self.ElectionStart = False
        self.verbose = verbose
        
        # RAFT on start 
        # 1. Initialize Followers with timeouts
        # 2. Elect a Leader
        # 3. Leader starts periodically sending hearbeats to Followers
        # 4. Follower's timeouts are reset unless the Leader is down
        # 5. If a Leader is down, repeat step 2
        self._raft_on_start()
           
    
    def getLeader(self):
        for f in self.followers:
            if f.state == 'Leader':
                return f.Leader
        
    def command(self, flag, command='incrementY', reps=100):
        leader = None
        
        while leader == None:
            leader = self.getLeader()
            continue;
          
        self.LEADER = leader
        
        if self.verbose:
            print("\n\n************✰*************\n"\
                 f"RAFT: LEADER WAS FOUND [{self.LEADER.ID}]"\
                  "\n************✰*************\n")
              
        result =self.LEADER.on_receive_CLIENT_request(flag, command, reps=reps)
        list=result
        fig = plt.figure(figsize=(7,7))
        ax = plt.axes(xlim=(0,L),ylim=(0,L))
        scatter=ax.scatter(list[:,0], list[:,1])

        def update(frame_number):
            i=1
            if i>1:
                i=i+1
            list[0,1]=list[0,1]+i
            list[1,1]=list[0,1]-1+i
            list[2,1]=list[0,1]+1+i
            scatter.set_offsets(list[:])
            return scatter,

        anim = FuncAnimation(fig, update, interval=10)
        plt.show()
        
        return result
   
    # PRIVATE FUNCTION
    ###########################
    def _raft_on_start(self):
                    
        #   Spawn the FOLLOWERs           
        #   Followers run as Threads #(THREAD 🧵)
        #   - They die when they reach timeout and become candidates
        #   - Else, they wait for heartbeats to reset the timeout
        
        print("Initialization of Servers...\n")
        for Follower in self.followers:
            Follower.reset_timeout(self.followers) #(THREAD 🧵)
        
        
        # RUN NETWORK INFINITELY
        print("\n##########" \
              "Starting" \
              "############\n")