from unqlite import UnQLite
db = UnQLite('sample.db')
data = db.collection('data')

import math


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
   
    # create list to store value 
    output_data=[]
    
    # go over the data and append data with name "cityToSearch"
    for row in range(len(collection)):
        row_data=collection[row]
        if row_data['city'].decode('utf-8')==cityToSearch: 
            output_data.append([row_data['name'].decode('utf-8'),row_data['full_address'].decode('utf-8'),row_data['city'].decode('utf-8'),row_data['state'].decode('utf-8')])
    #print(output_data)
   
    # save to file
    f = open(saveLocation1, 'w')
    for line in output_data:
       f.write('$'.join(str(s) for s in line))
       f.write('\n')
    f.close()
    

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    
    
    # create list to store value
    output_data=[]
    

    #find la1 and lon1 
    lat1=myLocation[0]
    lon1=myLocation[1]
    
    # go over the data, calculate the distance between myLocation and collection location. 
    #When the distance is less/equal than maxDistance and categoriesToSearch is in data, 
    #append the name of bussiness
    
    for row in range(len(collection)):
        row_data=collection[row]
        lat2=row_data['latitude']
        lon2=row_data['longitude']
        distance_two_locations=distance(lat2, lon2, lat1, lon1)
        
        
        if distance_two_locations<=maxDistance:
            for i in categoriesToSearch:
                for j in row_data['categories']:
                    if i == j.decode('utf-8') and row_data['name'].decode('utf-8') not in output_data :
                        output_data.append(row_data['name'].decode('utf-8'))
        
            
    f = open(saveLocation2, 'w')
    for line in output_data:
        f.write(line)
        f.write('\n')
    f.close()

#distance function        
def distance(lat2, lon2, lat1, lon1):
    r=3959
    phi1,phi2=math.radians(lat1),math.radians(lat2)
    delta_phi,delta_lam=math.radians(lat2-lat1),math.radians(lon2-lon1)
    a=math.sin(delta_phi/2)*math.sin(delta_phi/2)+math.cos(phi1)*math.cos(phi2)*math.sin(delta_lam/2)*math.sin(delta_lam/2)
    c=2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    d=r*c
    return d


#FindBusinessBasedOnCity('Tempe', 'output_city.txt', data) 
#FindBusinessBasedOnCity('Scottsdale', 'output_city.txt', data) 
#FindBusinessBasedOnCity('Mesa', 'output_city.txt', data) 
#FindBusinessBasedOnLocation(['Buffets'], [33.3482589, -111.9088346], 10, 'output_loc.txt', data)
#FindBusinessBasedOnLocation(['Bakeries'], [33.3482589, -111.9088346], 15, 'output_loc.txt', data)
#FindBusinessBasedOnLocation(['Food', 'Specialty Food'], [33.3482589, -111.9088346], 30, 'output_loc.txt', data) 
