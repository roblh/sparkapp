#!/usr/bin/env python
# coding: utf-8

# In[35]:


from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType

from datetime import datetime
import pytz
import builtins



from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# In[36]:


#Load in both files
df_treasury = spark.read.json("/Users/robertharmon/Downloads/gbr.jsonl")
df_ofac = spark.read.json("/Users/robertharmon/Downloads/ofac.jsonl.gz")
df_countries_data = spark.read.csv("/Users/robertharmon/Downloads/countries_of_the_world.csv", header=True)


# In[37]:


#Get idea of Treasury file content
df_treasury.show(vertical=True)


# In[38]:


#Get idea of Ofac file content
df_ofac.show(vertical=True)


# In[39]:


#Select country
df_country = df_countries_data.select('Country')


# In[40]:





print("Initial Treasury count is {} and intitial OFAC count is {}".format(df_treasury.count(),df_ofac.count()))

df_treasury = df_treasury.dropDuplicates()
df_ofac = df_ofac.dropDuplicates()

print("Deduped Treasury count is {} and intitial OFAC count is {}".format(df_treasury.count(),df_ofac.count()))




# In[41]:


#BEGIN - CLEAN TREASURY

#PRINT SCHEMA
df_treasury.printSchema()


# In[42]:


#Explode Treasury array for 1) nationality 2) reported dates of birth


df_treasury = df_treasury.select('*', explode('nationality').alias('uk_exploded_nationality')).select('*', explode('reported_dates_of_birth').alias('uk_exploded_reported_date_of_birth')).drop('nationality', 'reported_dates_of_birth')


# In[43]:


df_treasury.printSchema()


# In[44]:


#Explode Treasury arrays of structs for 1) addresses 2) aliases 3) id numbers 

df_treasury.createOrReplaceTempView('temp_clean_treasury')

df_treasury = spark.sql("""select id as uk_id,
name as uk_name,
place_of_birth as uk_place_of_birth,
position as uk_position,
uk_exploded_reported_date_of_birth,
type as uk_type,
uk_exploded_nationality,
a.country as uk_address_country,
a.postal_code as uk_address_postal_code,
a.value as uk_aliases_value,
b.type as uk_aliases_type,
c.comment as uk_id_numbers_comment,
c.value as uk_id_numbers_value
from temp_clean_treasury
lateral view outer explode(addresses)tmp1 as a
lateral view outer explode(aliases)tmp2 as b
lateral view outer explode(id_numbers)tmp2 as c
""")
df_treasury.show(vertical=True)

# print("DedupeTreasury count is {} and intitial OFAC count is {}".format(df_treasury.count(),df_ofac.count()))


# In[45]:


df_treasury.printSchema()


# In[46]:


df_treasury.show(vertical=True)


# In[47]:


#BEGIN - CLEAN OFAC

#PRINT SCHEMA
df_ofac.printSchema()


# In[48]:


#Explode OFAC array for 1) nationality 2) reported dates of birth

df_ofac = df_ofac.select('*', explode('nationality').alias('ofac_exploded_nationality')).select('*', explode('reported_dates_of_birth').alias('ofac_exploded_reported_date_of_birth')).drop('ofac_nationality', 'ofac_reported_dates_of_birth')

print("Exploded Treasury count is {} and Exploded OFAC count is {}".format(df_treasury.count(),df_ofac.count()))


# In[49]:


df_ofac.printSchema()


# In[50]:


#Explode OFAC arrays of structs for 1) addresses 2) aliases 3) id numbers 

df_ofac.createOrReplaceTempView('temp_ofac_clean')

df_ofac = spark.sql("""select id as ofac_id,
name as ofac_name,
place_of_birth as ofac_place_of_birth,
position as ofac_position,
ofac_exploded_reported_date_of_birth,
type as ofac_type,
ofac_exploded_nationality,
a.country as ofac_address_country,
a.postal_code as ofac_address_postal_code,
a.value as ofac_aliases_value,
b.type as ofac_aliases_type,
c.comment as ofac_id_numbers_comment,
c.value as ofac_id_numbers_value
from temp_ofac_clean
lateral view outer explode(addresses)tmp1 as a
lateral view outer explode(aliases)tmp2 as b
lateral view outer explode(id_numbers)tmp2 as c
""")
df_ofac.show(vertical=True)




# In[51]:


print("Exploded-2 Treasury count is {} and Exploded-2 OFAC count is {}".format(df_treasury.count(),df_ofac.count()))


# In[52]:


df_ofac.printSchema()


# In[53]:


#UTILS

def convert_to_list_with_to_delim(date_string):
    try:
        if "to" not in date_string:
            date_string = date_string.replace(" ", '-')
            return list(date_string.split("/n"))
        else:
            try:
                new_date_array = []
                date_string = date_string.split("to")
                for date in date_string:
                    date = date.strip()
                    date = date.replace(" ", '-')                
                    new_date_array.append(date)
                return new_date_array
            except Exception as e: 
                print(e)
    except Exception as e:
        print(e)

    
def parse_YYYY(date_list):
    try:
        for i, date in enumerate(date_list):
            if len(date) == 4:
                continue 
            else:
                    date = date.replace(" ", '-')
                    time_data_value = str(datetime.strptime(date,'%d-%b-%Y').replace(tzinfo=pytz.UTC).strftime("%Y"))
                    date_list[i] = time_data_value
        return date_list
    except Exception as e:
        print(e)

            

def bridge_date_YYYY(date_list):
    if date_list == None:
        return(date_list)
    elif len(date_list) > 1:
        try:
            minuend = builtins.max(date_list)
            floor = builtins.min(date_list)
            new_list = []
    
            for i in range(minuend-floor + 1):
                new_list.append(minuend - i)
            return new_list
        except Exception as e:
            print(e)  
    else:
        return(date_list)


        
def bridge_date_datetime(date_list):
    try:
        for i, date in enumerate(date_list):
            if len(date) == 4:
                minuend = date_list.max()
                subtrahend = date_list.min()
                continue 
            else:
                    date = date.replace(" ", '-')
                    time_data_value = str(datetime.strptime(date,'%d-%b-%Y').replace(tzinfo=pytz.UTC).strftime("%Y"))
                    date_list[i] = time_data_value
        return date_list
    except Exception as e:
        print(e)   


def reformat_date(date_list):
    try:
        for index, date in enumerate(date_list):
            if len(date) == 4:
                break 
            else:
                date = date.replace(" ", '/')
                time_data_value = str(datetime.strptime(date,'%d/%b/%Y').replace(tzinfo=pytz.UTC).strftime("%d/%m/%Y"))
                date_list[index] = time_data_value
        return date_list
    except Exception as e:
        print(e)  


# In[54]:


# # test YYYY
# #test split_date_on_to
# new_dates = convert_to_list_with_to_delim("1951")
# print(new_dates)


# # test dd Mon YYYY
# new_dates = convert_to_list_with_to_delim("12 Mar 1971")
# print(new_dates)


# # test YYYY to YYYY
# new_dates = convert_to_list_with_to_delim("12 Mar 1973")
# print(new_dates)

# # test dd Mon YYYY to dd Mon YYYY
# new_dates = convert_to_list_with_to_delim("12 Mar 1971 to 12 Mar 1973")
# print(new_dates)

# #test parse_YYYY
# parsed_yyyy = parse_YYYY(['12-Mar-1973'])
# print(parsed_yyyy)

# #test parse_YYYY
parsed_yyyy = parse_YYYY(['05 Jan 1974'])
print(parsed_yyyy)

# #test reformat_date
# parsed_yyyy = reformat_date(['12 Mar 1973'])
# print(parsed_yyyy)

# #test reformat_date
# parsed_yyyy = reformat_date(['12-Mar-1971', '12-Mar-1973'])
# print(parsed_yyyy)

# #test reformat_date
# parsed_yyyy = reformat_date(['12-Mar-1971', '12-Mar-1973'])
# print(parsed_yyyy)


# In[55]:


#Return exploded_reported_date_of_birth as list

df_ofac.createOrReplaceTempView('temp_ofac_clean')

df_ofac = spark.sql("""select ofac_id,
ofac_name,
ofac_place_of_birth,
ofac_position,
split(`ofac_exploded_reported_date_of_birth`, 'to') as `ofac_split_date_of_birth`,
ofac_type,
ofac_exploded_nationality,
ofac_address_country,
ofac_address_postal_code,
ofac_aliases_value,
ofac_aliases_type,
ofac_id_numbers_comment,
ofac_id_numbers_value
from temp_ofac_clean
""")
df_ofac.show(vertical=True)


# In[56]:


# #Create separate column just for year




# df_ofac.createOrReplaceTempView('temp_ofac_clean')

# df_ofac = spark.sql("""select id,
# name,
# place_of_birth,
# position,
# split_date_of_birth,
# split(`split_date_of_birth`, 'to') as `reported_year_of_birth`,
# type,
# exploded_nationality,
# address_country,
# address_postal_code,
# aliases_value,
# aliases_type,
# id_numbers_comment,
# id_numbers_value
# from temp_ofac_clean
# """)
# df_ofac.show()


# In[57]:


#Create separate column just for year


spark.udf.register("reformat_date", reformat_date,ArrayType(StringType()))


df_ofac.createOrReplaceTempView('temp_ofac_clean')

df_ofac = spark.sql("""select ofac_id,
ofac_name,
ofac_place_of_birth,
ofac_position,
ofac_split_date_of_birth,
reformat_date(ofac_split_date_of_birth) as ofac_reformatted_date_of_birth,
ofac_type,
ofac_exploded_nationality,
ofac_address_country,
ofac_address_postal_code,
ofac_aliases_value,
ofac_aliases_type,
ofac_id_numbers_comment,
ofac_id_numbers_value
from temp_ofac_clean
""")
df_ofac.show(vertical=True)


# In[58]:


#Create separate column just for year


spark.udf.register("parse_YYYY", parse_YYYY,ArrayType(StringType()))


df_ofac.createOrReplaceTempView('temp_ofac_clean')

df_ofac = spark.sql("""select ofac_id,
ofac_name,
ofac_place_of_birth,
ofac_position,
ofac_split_date_of_birth,
ofac_reformatted_date_of_birth,
parse_YYYY(ofac_split_date_of_birth) as ofac_reported_year_of_birth,
ofac_type,
ofac_exploded_nationality,
ofac_address_country,
ofac_address_postal_code,
ofac_aliases_value,
ofac_aliases_type,
ofac_id_numbers_comment,
ofac_id_numbers_value
from temp_ofac_clean
""")
df_ofac.show(vertical=True)


# In[59]:


#Explode OFAC array for 1) nationality 2) reported dates of birth

#Explode Treasury array for 1) nationality 2) reported dates of birth



df_ofac = df_ofac.select('*', explode('ofac_reformatted_date_of_birth').alias('ofac_exploded_reformatted_date_of_birth'))


# df_ofac = df_ofac.select('*', explode('reformatted_date_of_birth').alias('exploded_reformatted_date_of_birth'))\
# .select('*', explode('reported_dates_of_birth').alias('exploded_reported_date_of_birth')).drop('nationality', 'reported_dates_of_birth')

print("Exploded-2 Treasury count is {} and Exploded-3 OFAC count is {}".format(df_treasury.count(),df_ofac.count()))



# In[60]:


df_ofac.show(vertical=True)


# In[61]:


df_treasury.show(vertical=True)


# In[62]:


#Dedup on name, dob
df_treasury = df_treasury.dropDuplicates(['uk_name', 'uk_exploded_reported_date_of_birth'])
df_ofac = df_ofac.dropDuplicates(['ofac_name', 'ofac_exploded_reformatted_date_of_birth'])


# In[63]:


#Join data on name, dob
df_joined_data_2 = df_treasury.join(df_ofac, ((df_treasury.uk_name ==  df_ofac.ofac_name) & (df_treasury.uk_exploded_reported_date_of_birth ==  df_ofac.ofac_exploded_reformatted_date_of_birth)) |
                                    #on name and position
((df_treasury.uk_name ==  df_ofac.ofac_name) & (df_treasury.uk_position ==  df_ofac.ofac_position)) |
                                    #on name and postal code
((df_treasury.uk_name ==  df_ofac.ofac_name) & (df_treasury.uk_address_postal_code ==  df_ofac.ofac_address_postal_code))|
                                    #on alias value and dob
((df_treasury.uk_aliases_value ==  df_ofac.ofac_aliases_value) & (df_treasury.uk_exploded_reported_date_of_birth ==  df_ofac.ofac_exploded_reformatted_date_of_birth)),"inner") 






df_joined_data_2.count()


# In[64]:


df_joined_data_2.printSchema()


# In[65]:


df_joined_data_2.drop('ofac_split_date_of_birth', 'ofac_reformatted_date_of_birth', 'ofac_reported_year_of_birth') .repartition(1) .write.format('com.databricks.spark.csv')   .mode('overwrite').option("header", "true").save("./ofac_and_treasury_joined")


# In[ ]:




