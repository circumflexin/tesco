#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# i think i'veimpemented somehting very CPU intensive here, not sure what

#Review notes:
# keep web inplementation in mind
# json instead of python module
# are the duplicates for sales ok? products are sold in more than one category
# generate 




import pandas as pd, glob, os, ast, sqlite3, pdb, json, numpy as np
from collections import OrderedDict
from shutil import copyfile
from openpyxl import load_workbook


def load_and_strip(key, data, writer):
	print('\tCleanup')
	print('\t\tParsing')
	if key in ['CFC Donations', 'B63KI']: # these two datasets have a slightly different format and may contain data from oputside the time period of interest.
		df = pd.ExcelFile(data['filename']).parse(sheet_name = data['sheet'], skip_blank_lines = True, skiprows = data['skiprows']).dropna(thresh = 2) # parse the data from the correct sheet.
		df = df[(df.Year_week_number >= weeks[0]) & (df.Year_week_number <= weeks[1])] # filter out rows where the week is too early or late
	else:	
		df=pd.ExcelFile(data['filename']).parse(sheet_name = data['sheet'], skip_blank_lines = True, skiprows = data['skiprows']) # in all other cases the first sheet is the correct one.
	if 'BASE_PRODUCT_NUMBER' in df:
		df.rename(columns = {'BASE_PRODUCT_NUMBER' : 'Base_Product_Number'}, inplace = True) #standardise BASE column header
	df_names=list(df)
	print(df_names)
	print('\t\tChecking Scope')
	scope_merge = df.merge(scope_map, how = 'left', on = 'Product_Sub_Group_Code') # lookup whether the product is in scope
	check_manually = scope_merge[pd.isna(scope_merge.Edible) == True] # log if not found
	if check_manually.empty == False:
		with open(os.path.join(directory,'Check','check_%s.csv' % key), 'w', encoding = 'utf-8') as f:
			check_manually.to_csv(f)
			check_list.append(key)
	return scope_merge

def pivot(key, data, scope_merge):
	scope_merge = scope_merge[scope_merge.Food == 'Yes'] # drop NaNs and 'No's
	if key in ['Sales', 'Waste']:
		processed = sheet = scope_merge[data['keep']]
	else:
		print('\t\tCreating Pivot')
		processed = scope_merge.pivot_table(values = data['values'], index = data['index'], columns = data['columns'], fill_value = 0, aggfunc = np.sum) # create a 'pivot table' using the values, rows and columns specified in the structure. 
		sheet = processed.reset_index(data['index'])
		sheet[data['index']] = sheet[data['index']].ffill() # repeat labels so lookups will work
	sheet.to_excel(writer, sheet_name = key, startrow = 0)
	return processed
	

def database_upsert(processed, data, connection, cursor, variables):
	print('\tDatabase upsert')
	if 'keep' in data:
		intersection = list(set(variables).intersection(data['keep'])) # columns of interest which are also in this dataset
	else:
		intersection = list(set(variables).intersection(data['index'])) # columns of interest which are also in this dataset
	processed = processed.filter(items = intersection) # strip down to columns of interest
	processed.to_sql('tmp', conn, if_exists = 'replace')
	SG_vars = ['Category_Area', 'Product_Sub_Group_Description']
	if set(SG_vars).issubset(intersection):
		for column_name in SG_vars:
			print('\t\t{}'.format(column_name))
			curs.execute("""
				UPDATE master_subgroups
				SET {0} = IfNull({0},(SELECT tmp.{0} 
											FROM tmp 
											WHERE tmp.Product_Sub_Group_Code = master_subgroups.Product_Sub_Group_Code))
				""".format(column_name))
			conn.commit()
			curs.execute ("""
			INSERT OR IGNORE INTO master_subgroups (Product_Sub_Group_Code, Category_Area, Product_Sub_Group_Description)
			SELECT Product_Sub_Group_Code, Category_Area, Product_Sub_Group_Description
			FROM tmp
			""")
			conn.commit()
	
	query = ("""
		INSERT OR IGNORE INTO master_products (Product_Sub_Group_Code, Base_Product_Number, Long_Description)
		SELECT Product_Sub_Group_Code, Base_Product_Number, Long_Description
		FROM tmp
		""")
	curs.execute(query)		
	conn.commit()


if __name__ == "__main__":

	directory = 'Test' # where the files will be
	new_dirs = ['Processed', 'Check']	
	files = [] # Files to look in; populated by glob.
	weeks = [201751, 201752] # Any data from outside this date range will be excluded.
	variables = ['Category_Area','Product_Sub_Group_Code','Product_Sub_Group_Description','Base_Product_Number','Long_Description'] # columns of interest
	scope_map = pd.read_csv('scope_map.csv') # array of product codes and whether they are in scope	
	with open('structure.json', 'r') as f:
		structure = json.load(f)
	structure = OrderedDict(structure)

	
	for f in glob.glob(os.path.join(directory,'*.xlsx')): # get excel files, won't pick up csv's from previous runs of the script. 
		print(f)
		files.append(f) # add all excel filesnames to list	

	for d in new_dirs:
		try:
			os.makedirs(os.path.join(directory,d))
		except OSError:
			if not os.path.isdir(os.path.join(directory,d)):
				raise
	
	seen = []
	print('\nFiles matched: ')
	for n in structure.keys():
		matching = [s for s in files if n.lower() in s.lower() if s not in seen] # Looks within the full filename strings for the key strings.
		if len(matching) == 0:
			raise ValueError('No %s file seems to be present, is it labelled correctly?' % n)
		elif len(matching) == 0:
			raise ValueError('Two files matching the same key')
		else:
			structure[n]['filename'] = matching[0] # map full filename to settings
			print(n, ':', matching[0])
			seen.append(matching[0])


	conn = sqlite3.connect('master.db')
	curs = conn.cursor()
	curs.execute("""DROP TABLE IF EXISTS master_products""")
	curs.execute("""DROP TABLE IF EXISTS master_subgroups""")
	curs.execute("""CREATE TABLE master_products (Product_Sub_Group_Code,Base_Product_Number PRIMARY KEY,Long_Description)""")
	curs.execute("""CREATE TABLE master_subgroups (Product_Sub_Group_Code VARCHAR PRIMARY KEY, Product_Sub_Group_Description, Category_Area)""")
	conn.commit()

	check_list = []
	print('\nNow processing:')

	copyfile('template.xlsx', os.path.join(directory, 'Processed','output.xlsx'))

	book = load_workbook(os.path.join(directory, 'Processed','output.xlsx'))
	writer = pd.ExcelWriter(os.path.join(directory,'Processed','output.xlsx'), engine = 'openpyxl')
	writer.book = book
	writer.sheets = {ws.title: ws for ws in book.worksheets}
	print(writer.sheets)

	for key, data in structure.items():
		print(key)
		scope_merge = load_and_strip(key = key, data = data, writer = writer)
		if len(check_list) < 8:  # check both cases, change to >0 later
			processed = pivot(key = key, data = data, scope_merge = scope_merge)
			database_upsert(processed, data, connection = conn, cursor= curs, variables = variables) # skip the database operation if the scope mapping is incomplete
			conn.commit()

	results = pd.read_sql_query(""" SELECT  master_subgroups.Category_Area, master_products.Product_Sub_Group_Code, master_subgroups.Product_Sub_Group_Description, master_products.Base_Product_Number, master_products.Long_Description
				FROM master_products
				LEFT OUTER JOIN master_subgroups 
				ON  master_products.Product_Sub_Group_Code = master_subgroups.Product_Sub_Group_Code;
				""", conn)
	results.to_excel(writer, sheet_name = 'Master', startrow = 3, index = False, header = True)
	writer.save()
	conn.close()


	print('\n Processing Complete')

	if len(check_list) > 0:
		print('Not all subgroups were found in the scope_map:')
		print(check_list, 'failed to find at least one subgroup code, the subset which were not matched are saved as CSVs\n. Add them to scope.csv and rerun this script.')
		exit()