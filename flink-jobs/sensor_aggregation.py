read machine-sensors

sliding for minimum

lowest = sys.maxsize
for I in range (n-k+1):
	lowest = min(lowest, arr[i+j])
return lowest

sliding for max
highest = -sys.maxsize
for I in range (n-k+1):
	highest = max(highest, arr[i+j])
return highest

tumbling average


tumbling count