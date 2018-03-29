import numpy
import matplotlib.pyplot as plt
import json
import os

duration = int(input("Duration of simulation? "))
candidate_files = os.listdir('./outputs')
print("""
Available output files:
{}
""".format(candidate_files)
)

choice = None
while choice == None:
    try:
        index = int(input("Select index of item: (0-{})".format(len(candidate_files)-1)))
        choice = candidate_files[index]
    except IndexError:
        print("Attempted to choose index outsinde bounds, try again! Error:")

try:
    with open('./outputs/{}'.format(choice), 'r') as output_file:
        data = json.loads(output_file.read())
except FileNotFoundError as e:
    print("The file was not found!\n{}".format(e.filename))


y_axis = range(1,duration + 10)
data_points = sorted(list(map(lambda x: int(x), data.keys()))) # get ints of where we have data

messages_in_queue = []
active_containers = []

for point in data_points:
    
    # get message queue length
    msgs = 0
    for item in data.get(str(point)).get('MSG'):
        msgs += data.get(str(point)).get('MSG').get(item)
    messages_in_queue.append(msgs)

    # get number of containers running
    containers = 0
    for ip in data.get(str(point)).get('WORKERS'):
        containers += len(data.get(str(point)).get('WORKERS').get(ip).get('docker'))
    active_containers.append(containers)


datasets = [messages_in_queue, active_containers]

fig = plt.figure()

for sets in datasets:
    plt.plot(data_points, sets)
    #print(str(sets)+str(data_points))


plt.ylabel('Some numbers')
fig.savefig('./test.pdf')
plt.close(fig)