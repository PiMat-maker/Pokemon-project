# Pokemon-project

### Project structure
Package airflow contains files with dags and utils for them<br>
- Check_generations_dag(Kudlakov).py - dag that checks generations updating every day and writes data in airflow logs
- Load_dag(Kudlakov).py - dag that copies full data from Pokemon API or loads missing(new) data
- Info_collector - file with necessary functions and classes for load_dag(Kudlakov).py file

Package airflow contains file with sql script
- Snowflake.sql - contains creating of two stages(staging and storage) and data_marts. Storage data is showed as galaxy dimensional model

On s3 data is saved into next files:<br>
- pokemon.json - contains info about pokemons such as pokemon_id, pokemon_name, pokemon_types, pokemon_stats, pokemon_moves, pokemon_species and past_types
- generation.json - contains info about a generation and species were added in this generation
- type.json - contains info about type_id and type_name


### More about dags
#### Check_generations_dag
As it said above check_generations checks appearance of new generations on API. There are three ways how program behaves on that info:<br>
1. If there is no generation.json file on s3, so load_dag will be triggered for uploading it
2. If there is generation.json on s3 but some data is missing or new, so load_dag will be triggered for uploading it
3. If there is generation.json on s3 and no missing or new data, then check_generation dag will finish
<br>

#### Load_dag
As it said above load_dag can work in two different ways:
1. Copy info from API
2. Load new info from API

Second one works by default. That means when check_generations_dag triggers load_dag load new info will execute too. <br>
For executing copy info tasks load_dag should be triggered with config file with key = "copy" and value = "True" in it.


### About snowflake part
As it said before there are staging, storage and data_marts with answers on tasks.<br>
<b>Staging schema</b> contains json data from three files which are presented on s3.<br><br>
<b>Storage schema</b> contains aggregated data from staging schema.<br> 
It is showed a model of the storage schema on the image.
![image](https://user-images.githubusercontent.com/54264954/174674721-9018c784-a078-4324-94e5-1213cb69246d.png)

### Tasks answers
   
### 1.a
![image](https://user-images.githubusercontent.com/54264954/174492464-10e6c48d-5aee-498e-9df5-836ffc306205.png)


### 1.b
![image](https://user-images.githubusercontent.com/54264954/174492449-cd46a7b4-1df0-4eb9-a1d1-bdb4dbecdd0e.png)


### 1.c
![image](https://user-images.githubusercontent.com/54264954/174492420-18e55885-0181-43e9-bce7-14f92943c72c.png)


### 1.d
![image](https://user-images.githubusercontent.com/54264954/174492394-ae22435b-d120-4be8-a647-aac39615ed31.png)
