from concurrent.futures import ThreadPoolExecutor
import requests
import json
import time
from itertools import repeat
from abc import ABC


MAX_QUERIES_PER_SECOND = 5


class Info(ABC):
    def __init__(self) -> None:
        super().__init__() 


class PokemonInfoRaw(Info):
    def __init__(self, id, name, types, stats, moves, species, past_types) -> None:
        self.id = id
        self.name = name
        self.types = types
        self.stats = stats
        self.moves = moves
        self.species = species
        self.past_types = past_types


    def __new__(cls, *args, **kwargs):
        instance = super(PokemonInfoRaw, cls).__new__(cls)
        return instance


    def __aggregate_species(species) -> str:
        return species["name"]


    def __aggregate_stats(stats) -> "list[int]":
        return [stat["base_stat"] for stat in stats if "base_stat" in stat]


    def __aggregate_moves(moves) -> "list[str]":
        return [move["move"]["name"] for move in moves if "move" in move and "name" in move["move"] ]
        

    def __aggregate_types(types) -> "list[str]":
        return [type_el["type"]["name"] for type_el in types if "type" in type_el and "name" in type_el["type"]]


    def __get_id(url: str, what_id: str) -> int:
        what_id_pos = url.rfind(f"{what_id}/") + len(f"{what_id}/")
        print(url)
        return int(url[what_id_pos:-1])


    def __aggregate_past_types(past_types, actual_types) -> "dict(str, list[str])":
        types_by_generation = dict()
        first_generation_id = 0
        for past_type in past_types:
            last_generation_id = PokemonInfoRaw.__get_id(past_type["generation"]["url"], "generation")
            types = PokemonInfoRaw.__aggregate_types(past_type["types"])
            types_by_generation[first_generation_id] = types
            first_generation_id = last_generation_id + 1

        types_by_generation[first_generation_id] = actual_types

        return types_by_generation


    def create_pokemon_info(self):
        species_name = PokemonInfoRaw.__aggregate_species(self.species)
        stats = PokemonInfoRaw.__aggregate_stats(self.stats) 
        moves = PokemonInfoRaw.__aggregate_moves(self.moves)
        types = PokemonInfoRaw.__aggregate_types(self.types)
        types_by_generation = PokemonInfoRaw.__aggregate_past_types(self.past_types, types)
        return PokemonInfo(self.id, self.name, species_name, stats, moves, types_by_generation)


    def __str__(self):
        return f"ID: {self.id}\nName: {self.name}\nTypes: {self.types}\nStats: {self.stats}\nMoves: {self.moves}\nSpecies : {self.species}\nPast types: {self.past_types}"


class PokemonInfo(Info):
    def __init__(self, id: int, pokemon_name: str, species_name: str, stats: "list[int]", moves: "list[str]", types_by_generation: "dict(str, list[str])") -> None:
        self.id = id
        self.name = pokemon_name
        self.species = species_name
        self.stats = stats
        self.moves = moves
        self.types_by_generation  = types_by_generation


    def __new__(cls, *args, **kwargs):
        instance = super(PokemonInfo, cls).__new__(cls)
        return instance


    def __str__(self):
        return f"ID: {self.id}\nName: {self.name}\nSpecies: {self.species}\nStats: {self.stats}\nMoves: {self.moves}\nTypes by generation: {self.types_by_generation}\n"


class GenerationInfo(Info):
    def __init__(self, id: int, pokemon_species: "list[str]") -> None:
        self.id = id
        self.pokemon_species = pokemon_species


    def __new__(cls, *args, **kwargs):
        instance = super(GenerationInfo, cls).__new__(cls)
        return instance


    def __str__(self):
        return f"ID: {self.id}\nPokemon species: {self.pokemon_species}\n"


class TypeInfo(Info):
    def __init__(self, url: str, name: str) -> None:
        self.id = TypeInfo.__get_id(url, "type")
        self.name = name


    def __new__(cls, *args, **kwargs):
        instance = super(TypeInfo, cls).__new__(cls)
        return instance


    def __get_id(url: str, what_id: str) -> int:
        what_id_pos = url.rfind(f"{what_id}/") + len(f"{what_id}/")
        return int(url[what_id_pos:-1])


    def __str__(self):
        return f"Type ID: {self.id}\nType name: {self.name}\n"


def __get_json_file(url: str, offset: int = 0, limit: int = 1000):
    response = requests.get(f'{url}?offset={offset}&limit={limit}')
    return response.json()


def __get_list(url: str, list_name: str, offset: int) -> "list[str]":
    list_url = f"{url}{list_name}/"
    len_list = __get_json_file(list_url, limit=1)["count"]
    return __get_json_file(list_url, offset=offset, limit=len_list - offset)["results"] #change to len_list !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


def __get_urls_list(raw_list: "list[dict(str, str)]"):
    return [el["url"] for el in raw_list]


def __load_json_file(url: str):
    response = requests.get(url)
    return response.json()


def __get_pokemon_info_from_url(url: str, max_queries_number: int):
    time.sleep(1 / max_queries_number)
    json_file = __load_json_file(url)
    pokemon_id = json_file["id"]
    pokemon_name = json_file["name"]
    pokemon_types = json_file["types"]
    pokemon_stats = json_file["stats"]
    pokemon_moves = json_file["moves"]
    pokemon_species = json_file["species"]
    pokemon_past_types = json_file["past_types"]
    return PokemonInfoRaw(pokemon_id, pokemon_name, pokemon_types, pokemon_stats, pokemon_moves, pokemon_species, pokemon_past_types)


def __get_generation_info_from_url(url: str, max_queries_number: int):
    time.sleep(1 / max_queries_number)
    json_file = __load_json_file(url)
    generation_id = json_file["id"]
    pokemon_species = [pokemon["name"] for pokemon in json_file["pokemon_species"]]
    return GenerationInfo(generation_id, pokemon_species)


def __get_type_info_from_type_dict(type: "dict(str, str)") -> TypeInfo:
    return TypeInfo(type["url"], type["name"])


def convert_list_object_to_dict(objects_list):
    return [el.__dict__ for el in objects_list]


def convert_to_json_str(dicts_list):
    return json.dumps(dicts_list)


def copy_info_from_api(url: str, what_info: str, what_offset = 0) -> Info:
    what_list = __get_list(url, what_info, what_offset)

    if what_info != "type":
        urls = __get_urls_list(what_list)

    with ThreadPoolExecutor(max_workers=2) as executor:
        if what_info == "pokemon":
                info_raw = executor.map(__get_pokemon_info_from_url, urls, repeat(MAX_QUERIES_PER_SECOND))
                return [pokemon_info_raw.create_pokemon_info() for pokemon_info_raw in info_raw]

        if what_info == "generation":
                info_generator = executor.map(__get_generation_info_from_url, urls, repeat(MAX_QUERIES_PER_SECOND))
                return [info_el for info_el in info_generator]

        if what_info == "type":
                return [__get_type_info_from_type_dict(type_dict) for type_dict in what_list]
    
    return Info()


if __name__ == '__main__':
    url = "https://pokeapi.co/api/v2/"
    copy_info_from_api(url, "type")
