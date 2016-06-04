# import csv
# with open("./training_data/cluster_map/cluster_map") as f:
# 	reader = csv.reader(f)
# 	for row in reader:
# 		print row

# f.close()

import time

#process the cluster_map data.
cluster_dict = {}
cluster_dict_reverse = {}
for line in open("./training_data/cluster_map/cluster_map"):
	district_hash, district_id = line.split("\t")
	district_id.strip('\n')
	cluster_dict[district_hash] = district_id
	cluster_dict_reverse[district_id] = district_hash
	#print district_hash, district_id

#process the poi data.
poi_dict = {}
poi_class = []
for line in open("./training_data/poi_data/poi_data"):
	pos = line.find("\t", 0, 35)
	district_hash = line[0:pos]
	poi_class_str = line[pos+1 : len(line)]
	poi_class_str = poi_class_str[:-1] #delete last char '\n'
	poi_class = poi_class_str.split("\t")
	poi_dict[district_hash] = poi_class

#process the traffic data.
traffic_dict = {}
tj_level =[] #tj_level[4] = tj_time
for line in open("./training_data/traffic_data/traffic_data"):
	first_sep_pos = line.find("\t", 0, 35)
	last_sep_pos = line.rfind("\t")
	district_hash = line[0 : first_sep_pos]
	tj_level_str = line[first_sep_pos : len(line)]
	tj_level_str = tj_level_str[:-1] #delete last char '\n'
	tj_level = tj_level_str.split("\t")
	tj_time = tj_level[4]
	tj_level.pop()
	traffic_dict[district_hash + "_" + tj_time] = tj_level

#process the weather data.
weather_dict = {}
weather_info = []
for line in open("./training_data/weather_data/weather_data"):
	first_sep_pos = line.find("\t")
	date = line[0:first_sep_pos]
	weather_info_str = line[first_sep_pos : len(line)]
	weather_info_str = weather_info_str[:-1] #delete last char '\n'
	weather_info = weather_info_str.split("\t")
	weather_dict[date] = weather_info

#get the spice number for time
#2016-01-15 00:35:11
#2016-01-15 4
def getTimeSpice(time):
	struct_time = time.strptime(time, '%Y-%m-%d %X')
	hour   = struct_time[3]
	minute = struct_time[4]
	second = struct_time[5]
	time_spice = hour*6 + minute/10 + 1
	return time_spice

#process the order data and generate the complete data.
#schema structure:
#order_id, driver_id, passenger_id, start_district_id, start_tj_level, start_poi_class,
#dest_district_id, dest_tj_level, dest_poi_class, price, time, weather, temperature, pm2.5
output = open("complete_data.csv", "wb")
for line in open("./training_data/order_data/order_data"):
	order_id, driver_id, passenger_id, start_district_hash, dest_district_hash, price, time \
		= line.split("\t")
	time = time.strip('\n')
	time_spice = getTimeSpice(time)

	if "NULL" == driver_id:
		flag = 0
	else:
		flag = 1

	if start_district_hash in cluster_dict.keys():
		start_district_id = cluster_dict[start_district_hash]
	else:
		start_district_id = ""
	if dest_district_hash in cluster_dict.keys():
		dest_district_id = cluster_dict[dest_district_hash]
	else:
		dest_district_id = ""
	
	start_district_time = start_district_hash + "_" + time
	dest_district_time = dest_district_hash + "_" + time
	if start_district_time in traffic_dict.keys():
		start_tj_level = traffic_dict[start_district_time]
		start_tj_level_str = "_".join(start_tj_level)
	else:
		start_tj_level_str = ""
	
	if dest_district_time in traffic_dict.keys():
		dest_tj_level = traffic_dict[dest_district_time]
		dest_tj_level_str = "_".join(dest_tj_level)
	else:
		dest_tj_level_str = ""
	
	if start_district_hash in poi_dict.keys():
		start_poi_class = poi_dict[start_district_hash]
		start_poi_class_str = "_".join(start_poi_class)
	else:
		start_poi_class_str = ""
	if dest_district_hash in poi_dict.keys():
		dest_poi_class = poi_dict[dest_district_hash]
		dest_poi_class_str = "_".join(dest_poi_class)
	else:
		dest_poi_class_str = ""
		
	if time in weather_dict.keys():
		weather_info = weather_dict[time]
		weather = weather_info[0]
		temperature = weather_info[1]
		pm25 = weather_info[2]
	else:
		weather = ""
		temperature = ""
		pm25 = ""

	outputline = []
	outputline.append(start_district_id)
	outputline.append(start_tj_level_str)
	outputline.append(start_poi_class_str)
	outputline.append(dest_district_id)
	outputline.append(dest_tj_level_str)
	outputline.append(dest_poi_class_str)
	outputline.append(weather)
	outputline.append(temperature)
	outputline.append(pm25)
	outputline.append(time_spice)
	outputline.append(flag)

	output_str = "\t".join(outputline)
	output.write(output_str + "\n")
	print("Write following string to file: %s" %(output_str))
output.close()

