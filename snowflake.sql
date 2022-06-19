create or replace database Roman_Kudlakov;

create or replace schema Roman_Kudlakov.staging;

create or replace file format JSON_DATA
type = 'JSON';


create or replace table Roman_Kudlakov.staging.stg_pokemons(json_data variant, filename varchar, amnd_user varchar, amnd_date timestamp default current_timestamp());

create or replace table Roman_Kudlakov.staging.stg_generations(json_data variant, filename varchar, amnd_user varchar, amnd_date timestamp default current_timestamp());

create or replace table Roman_Kudlakov.staging.stg_types(json_data variant, filename varchar, amnd_user varchar, amnd_date timestamp default current_timestamp());

create or replace stage Roman_Kudlakov.staging.de_school_stage
  url=env.url
  credentials=(aws_key_id=env.key_id aws_secret_key=env.secret_key);
  
create or replace pipe pokemons_pipe auto_ingest = true as
copy into Roman_Kudlakov.staging.stg_pokemons(json_data, filename, amnd_user)
from (select $1, metadata$filename, current_user() from @Roman_Kudlakov.staging.de_school_stage)
pattern='.*snowpipe/Kudlakov/pokemon.json'
file_format = (type = 'JSON');

create or replace pipe generations_pipe auto_ingest = true as
copy into Roman_Kudlakov.staging.stg_generations(json_data, filename, amnd_user)
from (select $1, metadata$filename, current_user() from @Roman_Kudlakov.staging.de_school_stage)
pattern='.*snowpipe/Kudlakov/generation.json'
file_format = (type = 'JSON');

create or replace pipe generations_pipe auto_ingest = true as
copy into Roman_Kudlakov.staging.stg_types(json_data, filename, amnd_user)
from (select $1, metadata$filename, current_user() from @Roman_Kudlakov.staging.de_school_stage)
pattern='.*snowpipe/Kudlakov/type.json'
file_format = (type = 'JSON');

create or replace stream Roman_Kudlakov.staging.stg_pokemons_stream on table Roman_Kudlakov.staging.stg_pokemons;
create or replace stream Roman_Kudlakov.staging.stg_generations_stream on table Roman_Kudlakov.staging.stg_generations;
create or replace stream Roman_Kudlakov.staging.stg_types_stream on table Roman_Kudlakov.staging.stg_types;
create or replace stream Roman_Kudlakov.staging.stg_stats_stream on table Roman_Kudlakov.staging.stg_pokemons;
create or replace stream Roman_Kudlakov.staging.stg_moves_stream on table Roman_Kudlakov.staging.stg_pokemons;
create or replace stream Roman_Kudlakov.staging.stg_pokemon_moves_stream on table Roman_Kudlakov.staging.stg_pokemons;
create or replace stream Roman_Kudlakov.staging.stg_pokemon_types_stream on table Roman_Kudlakov.staging.stg_pokemons;


create or replace schema Roman_Kudlakov.storage;

create or replace table Roman_Kudlakov.storage.species(
    species_id int identity(1, 1) primary key,
    species_name varchar,
    first_generation int
);

create or replace table Roman_Kudlakov.storage.moves(
    move_id int identity(1, 1) primary key,
    move_name varchar
);

create or replace table Roman_Kudlakov.storage.types(
    type_id int primary key,
    type_name varchar
);

create or replace table Roman_Kudlakov.storage.pokemons(
    pokemon_id int primary key,
    pokemon_name varchar,
    species_id int,
    foreign key (species_id) references species(species_id) not enforced
);

create or replace table Roman_Kudlakov.storage.stats(
    pokemon_id int primary key,
    hp int,
    attack int,
    defense int,
    special_attack int,
    special_defense int,
    speed int,
    foreign key (pokemon_id) references pokemons(pokemon_id) not enforced
);

create or replace table Roman_Kudlakov.storage.pokemon_types_by_first_generation(
    type_id int,
    pokemon_id int,
    first_generation int,
    foreign key (type_id) references types(type_id) not enforced,
    foreign key (pokemon_id) references pokemons(pokemon_id) not enforced,
    primary key(type_id, pokemon_id)
);

create or replace table Roman_Kudlakov.storage.pokemon_moves(
    move_id int,
    pokemon_id int,
    foreign key (move_id) references moves(move_id) not enforced,
    foreign key (pokemon_id) references pokemons(pokemon_id) not enforced,
    primary key(move_id, pokemon_id)
);

create or replace task Roman_Kudlakov.staging.moving_stg_species
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_generations_stream')
as
insert into Roman_Kudlakov.storage.species(species_name, first_generation)
select src_3.value::varchar as species_name,
        first_generation
from (select parse_json(src_1.value):id::int as first_generation,
       parse_json(src_1.value):pokemon_species as species_names
   from Roman_Kudlakov.staging.stg_generations src,
   lateral flatten(input => src.json_data) src_1) src_2,
   lateral flatten(input => src_2.species_names) src_3
   where METADATA$ACTION = 'INSERT';
   
create or replace task Roman_Kudlakov.staging.moving_stg_moves
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_moves_stream')
as
insert into Roman_Kudlakov.storage.moves(move_name)
select distinct move_name from
(select src_3.value::varchar as move_name
from (select parse_json(src_1.value):moves as moves
   from Roman_Kudlakov.staging.stg_pokemons src,
   lateral flatten(input => src.json_data) src_1) src_2,
   lateral flatten(input => src_2.moves) src_3)
   where r = 1 and METADATA$ACTION = 'INSERT';

create or replace task Roman_Kudlakov.staging.moving_stg_stats
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_stats_stream')
as
insert into Roman_Kudlakov.storage.stats(pokemon_id, hp, attack, defense, special_attack, special_defense, speed)
select pokemon_id, stats[0]::int, stats[1]::int,  stats[2]::int,  stats[3]::int,  stats[4]::int,  stats[5]::int 
from (select parse_json(src_1.value):id::int as pokemon_id,
       parse_json(src_1.value):stats as stats
   from Roman_Kudlakov.staging.stg_pokemons src,
   lateral flatten(input => src.json_data) src_1) src_2
   where METADATA$ACTION = 'INSERT';
   
create or replace task Roman_Kudlakov.staging.moving_stg_types
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_types_stream')
as
insert into Roman_Kudlakov.storage.types(type_id, type_name)
select
       parse_json(src_1.value):id::int as type_id,
       parse_json(src_1.value):name::varchar as type_name
   from Roman_Kudlakov.staging.stg_types src,
   lateral flatten(input => src.json_data) src_1
   where METADATA$ACTION = 'INSERT';
   
create or replace task Roman_Kudlakov.staging.moving_stg_pokemons
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_pokemons_stream')
as
insert into Roman_Kudlakov.storage.pokemons(pokemon_id, pokemon_name, species_id)
select pokemon_id, pokemon_name, species_id
from (select parse_json(src_1.value):id::int as pokemon_id,
       parse_json(src_1.value):name::varchar as pokemon_name,
       parse_json(src_1.value):species::varchar as species_name
   from Roman_Kudlakov.staging.stg_pokemons src,
   lateral flatten(input => src.json_data) src_1) src_2
   Join Roman_Kudlakov.storage.species USING(species_name)
   where METADATA$ACTION = 'INSERT';   
   
create or replace task Roman_Kudlakov.staging.moving_stg_pokemon_moves
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_pokemon_moves_stream')
as
insert into Roman_Kudlakov.storage.pokemon_moves(move_id, pokemon_id)
select move_id, pokemon_id
from (select src_3.value::varchar as move_name, pokemon_id
from (select parse_json(src_1.value):id::int as pokemon_id,
      parse_json(src_1.value):moves as moves
   from Roman_Kudlakov.staging.stg_pokemons src,
   lateral flatten(input => src.json_data) src_1) src_2,
   lateral flatten(input => src_2.moves) src_3) src_4
   Join Roman_Kudlakov.storage.moves USING(move_name)
   where METADATA$ACTION = 'INSERT';   
   
create or replace task Roman_Kudlakov.staging.moving_pokemon_types_by_first_generation
warehouse = tasks_wh
schedule = '5 minute'
when system$stream_has_data('Roman_Kudlakov.staging.stg_pokemon_types_stream')
as
insert into Roman_Kudlakov.storage.pokemon_types_by_first_generation(type_id, pokemon_id, first_generation)
select type_id, pokemon_id, 
        IFF(src_6.first_generation = 0, species.first_generation, src_6.first_generation) as first_generation
from (select first_generation, src_5.value as type_name, pokemon_id
from (select src_3.key::int as first_generation, src_3.value as type_names, pokemon_id
from (select parse_json(src_1.value):id::int as pokemon_id,
      parse_json(src_1.value):types_by_generation as types_by_generation
   from Roman_Kudlakov.staging.stg_pokemons src,
   lateral flatten(input => src.json_data) src_1) src_2,
   lateral flatten(input => src_2.types_by_generation) src_3) src_4,
   lateral flatten(input => src_4.type_names) src_5) src_6
   Join Roman_Kudlakov.storage.types USING(type_name)
   Join Roman_Kudlakov.storage.pokemons USING(pokemon_id)
   Join Roman_Kudlakov.storage.species species USING(species_id)
   where METADATA$ACTION = 'INSERT';   
   
alter task Roman_Kudlakov.staging.moving_stg_species suspend;
alter task Roman_Kudlakov.staging.moving_stg_moves suspend;
alter task Roman_Kudlakov.staging.moving_stg_stats suspend;
alter task Roman_Kudlakov.staging.moving_stg_types suspend;
alter task Roman_Kudlakov.staging.moving_stg_pokemons suspend;
alter task Roman_Kudlakov.staging.moving_stg_pokemon_moves suspend;
alter task Roman_Kudlakov.staging.moving_pokemon_types_by_first_generation suspend;

create or replace schema Roman_Kudlakov.data_marts;

create or replace view Roman_Kudlakov.data_marts.pokemon_amount_for_each_type as
select type_name, pokemon_amount,
        pokemon_amount - lag(pokemon_amount) over (order by pokemon_amount) as diff_previous,
        pokemon_amount - lead(pokemon_amount) over (order by pokemon_amount)  as diff_next
from (select type_name, IFNULL(pokemon_amount, 0) as pokemon_amount
from (select type_id, count(pokemon_id) as pokemon_amount
from (select first_generation, pokemon_id, type_id
    from Roman_Kudlakov.storage.pokemon_types_by_first_generation
    Qualify max(first_generation) over (partition by pokemon_id) = first_generation) src_1
    Group by type_id)
    Right join Roman_Kudlakov.storage.types USING(type_id)) src_2
Order by pokemon_amount desc, diff_previous, diff_next;

create or replace view Roman_Kudlakov.data_marts.pokemon_amount_for_each_move as
select move_name, pokemon_amount,
        pokemon_amount - lag(pokemon_amount) over (order by pokemon_amount) as diff_previous,
        pokemon_amount - lead(pokemon_amount) over (order by pokemon_amount)  as diff_next
from (select count(pokemon_id) as pokemon_amount, move_id
    from Roman_Kudlakov.storage.pokemon_moves
    Group by move_id) src_1
    Right Join Roman_Kudlakov.storage.moves USING(move_id)
Order by pokemon_amount desc, diff_previous, diff_next;

create or replace view Roman_Kudlakov.data_marts.pokemon_total_stats_rating as
select pokemon_name, hp + attack + defense + special_attack + special_defense + speed as total_stats
    from Roman_Kudlakov.storage.stats
    Right join Roman_Kudlakov.storage.pokemons USING(pokemon_id)
Order by total_stats desc;

create or replace view Roman_Kudlakov.data_marts.pokemon_amount_by_type_for_each_generation as
select type_name, 
        IFNULL(gen_1, 0) as gen_1, 
        IFNULL(gen_2, 0) as gen_2, 
        IFNULL(gen_3, 0) as gen_3, 
        IFNULL(gen_4, 0) as gen_4, 
        IFNULL(gen_5, 0) as gen_5, 
        IFNULL(gen_6, 0) as gen_6,
        IFNULL(gen_7, 0) as gen_7,
        IFNULL(gen_8, 0) as gen_8
from (select *
from Roman_Kudlakov.storage.pokemon_types_by_first_generation
pivot(count(pokemon_id)
     for first_generation in  (1, 2, 3, 4, 5, 6, 7, 8)) 
     as q(type_id, gen_1, gen_2, gen_3, gen_4, gen_5, gen_6, gen_7, gen_8)) src_1
     Right join Roman_Kudlakov.storage.types USING(type_id);