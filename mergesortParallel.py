from multiprocessing import Pool
import time, random, sys
import multiprocessing


def main():

    N = 21838024        #The size of the array
    if len(sys.argv) > 1:  
        N = int(sys.argv[1])

    #We fill the array with random numbers and make a backup to always have the original one
    lystbck = [random.random()*21838024 for x in range(N)]


    #Normal mergesort
    lyst = list(lystbck)

    print ("\n\nUnsorted array is") #We print the hole original array
    for i in range(N): 
        print ("%d" %lyst[i]),


    start = time.time()             #start time
    lyst = mergesort(lyst)
    elapsed = time.time() - start   #stop time

    if not isSorted(lyst):      #We see if the array have been sorted
        print('Sequential mergesort did not sort. oops.')
    
    print('Sequential mergesort: %f sec' % (elapsed))


    #So that cpu usage shows a lull.
    time.sleep(3)
	# REVISADO Y FINALIZADO

    #Now, parallel mergesort. You re-create the array, so it's new again
    lyst = list(lystbck)
    start = time.time()
    n = multiprocessing.cpu_count() #We get the number of cores of the machine

    #Call the parallel mergeSort
    lyst = mergeSortParallel(lyst, n)

    elapsed = time.time() - start

    if not isSorted(lyst):      #We see if the array have been sorted
        print('mergeSortParallel did not sort. oops.')

    print('Parallel mergesort: %f sec' % (elapsed))

    
    print ("\n\nSorted array via parallel is") #We print the hole sorted array via parallel
    for i in range(N): 
        print ("%d" %lyst[i]),

    time.sleep(3)


def merge(left, right):         #returns a merged and sorted version of the two already-sorted arrays
    ret = []
    li = ri = 0
    while li < len(left) and ri < len(right):   #we look the array, looking the hole piece
        if left[li] <= right[ri]:               #if we still are at the search zone, we continue iterating
            ret.append(left[li])
            li += 1
        else:
            ret.append(right[ri])
            ri += 1
    if li == len(left):
        ret.extend(right[ri:])
    else:
        ret.extend(left[li:])
    return ret

def mergesort(lyst):                #It returns a copy of the array but already merged
    if len(lyst) <= 1:
        return lyst
    ind = len(lyst)//2
    return merge(mergesort(lyst[:ind]), mergesort(lyst[ind:]))  #it calls again itself to continue digging inside the array

def mergeWrap(AandB):   #calls the merge
    a,b = AandB
    return merge(a,b)

def mergeSortParallel(lyst, n): #the main method of mergeSort in parallel, taking into acount the cores of your machine
    numproc = n
    #Divides the indices
    endpoints = [int(x) for x in linspace(0, len(lyst), numproc+1)]
    #It transforms the array into x arrays
    args = [lyst[endpoints[i]:endpoints[i+1]] for i in range(numproc)]

	#Instantiate a Pool ()
    pool = Pool(processes = numproc)
    sortedsublists = pool.map(mergesort, args)
	#Performs mergesort on all the 1/numproc of the array

    #At this point, we have lots of sorted arrays and now we just have to get them back together and it returns the sorted array
    while len(sortedsublists) > 1:
        args = [(sortedsublists[i], sortedsublists[i+1]) \
				for i in range(0, len(sortedsublists), 2)]
        sortedsublists = pool.map(mergeWrap, args)

    return sortedsublists[0]
    

def linspace(a,b,nsteps):       #returns list of simple linear steps from a to b in nsteps
    ssize = float(b-a)/(nsteps-1)
    return [a + i*ssize for i in range(nsteps)]


def isSorted(lyst):             #Checks that the array is sorted in the correct way
    for i in range(1, len(lyst)):
        if lyst[i] < lyst[i-1]:
            return False
    return True

if __name__ == '__main__':
    main()
