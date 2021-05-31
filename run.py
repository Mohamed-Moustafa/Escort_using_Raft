#import python libraries
from time import sleep
from Escort import Raft
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from matplotlib.animation import FuncAnimation
from random import randint


n = 8      # number of nodes
flag = (0,0) # nodes intialized randomly and need to go take flag first

# 1. initializing the raft
verbose = True # to simulate the results
raft = Raft(n, verbose=verbose)

if verbose:
    sleep(3)

# 2. send CLIENT request
print("\nMove to take flag from (0,0) then go to (0,100)")
result = raft.command(flag=flag, reps=100)


