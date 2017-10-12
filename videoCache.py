# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 17:33:32 2017

@author: siowmeng
"""



import numpy as np

'''
class endpoint:
    def __init__(self, dataCenterLatency, cache_connections):
        self.dataCenterLatency = np.inf
        self.cache_connections = 0
'''

class Video:
    def __init__(self, iD, size):
        self.size = size
        self.iD = iD
        self.requestsDict = {}  
        
    def update_requests(self, endpoint, numRequests):
        self.requestsDict[endpoint] = numRequests

class Cache:
    def __init__(self, iD, capacity):
        self.iD = iD
        self.capacity = capacity
        self.latencyDict = {}
        
    def update_latency(self, endpoint, latency):
        self.latencyDict[endpoint] = latency
                        
#class EndPt:
#    def __init__(self, dcLatency, cacheLatency):
#        self.dcLatency = np.inf
#        self.cacheLatency = {}
#        
#    def updateDCLatency(self, dcLatency):
#        self.dcLatency = dcLatency
#        
#    def updateEndPtLatency(self, cache, latency):
#        self.cacheLatency[cache] = latency
    

# Useful variables
cacheList = []
videoList = []
dataCenterLatency = {}

filelist = ['me_at_the_zoo.in', 'kittens.in', 'trending_today.in', 'videos_worth_spreading.in']

for filename in filelist:
    
    print('Process file: ' + filename)

    # Reading data in
    with open(filename, 'r') as data:
        first_line = data.readline()  
        params = first_line.split()
        num_videos = int(params[0])
        num_endpts = int(params[1])
        num_requests = int(params[2])
        num_caches = int(params[3])
        cache_size = int(params[4])
        cacheList = [Cache(x, cache_size) for x in range(num_caches)]
    
        second_line = data.readline()
        video_sizes = [int(x) for x in second_line.split()]
        video_sizes = np.array(video_sizes)
        videoList = [Video(x, video_sizes[x]) for x in range(num_videos)]
        
        dataCenterLatency = {}
    
        for i in range(num_endpts):
            endpoint_params = data.readline().split()
            dataCenterLatency[i] = int(endpoint_params[0])
            num_connections = int(endpoint_params[1])
            for c in range(num_connections):
                connection_params = data.readline().split()
                cacheID = int(connection_params[0])
                latency = int(connection_params[1])
                cacheList[cacheID].update_latency(i, latency)
            
        for i in range(num_requests):
            request_params = data.readline().split()
            videoID = int(request_params[0])
            requesting_endpoint = int(request_params[1])
            num_requests = int(request_params[2])
            videoList[videoID].update_requests(requesting_endpoint, num_requests)
        
    
    totalCache = 0
    for cache in cacheList:
        totalCache += cache.capacity
    videoSavings = [0] * num_videos
                   
    videoSavingsPerCache = np.zeros((num_videos, num_caches))
                   
    # Calculate video Savings
    for vid in videoList:
        vidID = vid.iD
        #print(vidID)
        for endpoint in vid.requestsDict.keys():
            cacheLatency = []
            for cache in cacheList:
                cacheID = cache.iD
                if endpoint in cache.latencyDict.keys():
                    #cacheLatency.append([cache.iD, cache.latencyDict[endpoint]])
                    cacheLatency.append(cache.latencyDict[endpoint])
                    #print(vidID, endpoint, cache.iD, cache.latencyDict[endpoint])
                    videoSavingsPerCache[vidID, cacheID] += (dataCenterLatency[endpoint] - cache.latencyDict[endpoint])
            if len(cacheLatency) > 0:
                minLatPerCache = np.mean(cacheLatency)
            
            latencySaved = dataCenterLatency[endpoint] - minLatPerCache # latency from endpoint to cache
            videoSavings[vidID] += vid.requestsDict[endpoint] * latencySaved
            
    videoSavings = np.array(videoSavings)
    vidSavingsRatio = videoSavings / totalCache
    normVidSavingsRatio = vidSavingsRatio / np.sum(vidSavingsRatio)
    
    # Target size of video[i] to be stored at multiple number of cache servers
    targetVidSize = normVidSavingsRatio * totalCache
    
    cacheSizeLeft = totalCache
    
    # Length = number of cache server
    finalResults = np.empty((num_caches, num_videos))
    finalResults[:] = False
    
    
    
    
    
    while (any(video_sizes <= cacheSizeLeft)) and (any(targetVidSize > 0)):
        
        # NEW - 2.0
        cacheSizeUsed = [0] * num_videos
        # Greedy pick, the video ID to cache
        vidToCache = np.argmax(targetVidSize)
        #print(vidToCache)
        
        #cacheLeft = True
        # NEW - 2.0
        if (cacheSizeUsed[vidToCache] < targetVidSize[vidToCache]) and any(np.isfinite(videoSavingsPerCache[vidToCache])):
            # Greedy pick, for this video ID, the Cache ID to use
            useCacheID = np.argmax(videoSavingsPerCache[vidToCache])
            if cacheList[useCacheID].capacity >= videoList[vidToCache].size:
                cacheList[useCacheID].capacity -= videoList[vidToCache].size
                #print(finalResults)
                finalResults[useCacheID, vidToCache] = True
                #print(useCacheID)
                #print(finalResults)
                print(vidToCache, useCacheID)
                
                cacheSizeUsed[vidToCache] += videoList[vidToCache].size
                #cacheSizeLeft -= cacheSizeUsed
                cacheSizeLeft -= videoList[vidToCache].size
                targetVidSize[vidToCache] -= videoList[vidToCache].size
            videoSavingsPerCache[vidToCache, useCacheID] -= np.inf
            #cacheLeft = any(np.isfinite(videoSavingsPerCache[vidToCache]))
                                
        video_sizes[vidToCache] = 0
        targetVidSize[vidToCache] -= np.Inf
    
    noCacheServers = sum(np.sum(finalResults, axis = 1) > 0)
    with open(filename.split('.')[0] + '.out', 'w') as fd:
        fd.write(str(noCacheServers) + '\n')
        for i in range(num_caches):
            if sum(finalResults[i]) > 0:            
                writeLine = str(i)
                for vidCache in np.where(finalResults[i])[0]:
                    writeLine += (' ' + str(vidCache))
                fd.write(writeLine + '\n')